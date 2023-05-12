/*
 * Copyright 2022-2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.worker.common;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.aggregation.Aggregator;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.concurrent.CountDownLatch;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;
import java.util.concurrent.Executors;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Scan a table split.
 *
 * @author tiannan
 * @author hank
 * @create 2022-03
 * @update 2023-04-23 (moved from pixels-worker-lambda to here as the base worker implementation)
 */
public class BaseScanWorker extends Worker<ScanInput, ScanOutput>
{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseScanWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
    }

    @Override
    public ScanOutput process(ScanInput event)
    {
        ScanOutput scanOutput = new ScanOutput();
        long startTime = System.currentTimeMillis();
        scanOutput.setStartTimeMs(startTime);
        scanOutput.setRequestId(context.getRequestId());
        scanOutput.setSuccessful(true);
        scanOutput.setErrorMessage("");
        workerMetrics.clear();

        try
        {
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            String requestId = context.getRequestId();

            long queryId = event.getQueryId();
            requireNonNull(event.getTableInfo(), "even.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            boolean[] scanProjection = requireNonNull(event.getScanProjection(),
                    "event.scanProjection is null");
            boolean partialAggregationPresent = event.isPartialAggregationPresent();
            checkArgument(partialAggregationPresent != event.getOutput().isRandomFileName(),
                    "partial aggregation and random output file name should not equal");
            String outputFolder = event.getOutput().getPath();
            StorageInfo outputStorageInfo = event.getOutput().getStorageInfo();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean encoding = event.getOutput().isEncoding();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            String[] includeCols = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);

            Aggregator aggregator;
            if (partialAggregationPresent)
            {
                logger.info("start get output schema");
                TypeDescription inputSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
                inputSchema = WorkerCommon.getResultSchema(inputSchema, includeCols);
                PartialAggregationInfo partialAggregationInfo = event.getPartialAggregationInfo();
                requireNonNull(partialAggregationInfo, "event.partialAggregationInfo is null");
                boolean[] groupKeyProjection = new boolean[partialAggregationInfo.getGroupKeyColumnAlias().length];
                Arrays.fill(groupKeyProjection, true);
                aggregator = new Aggregator(WorkerCommon.rowBatchSize, inputSchema,
                        partialAggregationInfo.getGroupKeyColumnAlias(),
                        partialAggregationInfo.getGroupKeyColumnIds(), groupKeyProjection,
                        partialAggregationInfo.getAggregateColumnIds(),
                        partialAggregationInfo.getResultColumnAlias(),
                        partialAggregationInfo.getResultColumnTypes(),
                        partialAggregationInfo.getFunctionTypes(),
                        partialAggregationInfo.isPartition(),
                        partialAggregationInfo.getNumPartition());
            }
            else
            {
                aggregator = null;
            }

            int outputId = 0;
            logger.info("start scan and aggregate");
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();
                String outputPath = outputFolder + requestId + "_scan_" + outputId++;

                threadPool.execute(() -> {
                    try
                    {
                        int rowGroupNum = scanFile(queryId, scanInputs, includeCols, inputStorageInfo.getScheme(),
                                scanProjection, filter, outputPath, encoding, outputStorageInfo.getScheme(),
                                partialAggregationPresent, aggregator);
                        if (rowGroupNum > 0)
                        {
                            scanOutput.addOutput(outputPath, rowGroupNum);
                        }
                    }
                    catch (Exception e)
                    {
                        throw new WorkerException("error during scan", e);
                    }
                });
            }


            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new WorkerException("interrupted while waiting for the termination of scan", e);
            }

            logger.info("start write aggregation result");
            if (partialAggregationPresent)
            {
                String outputPath = event.getOutput().getPath();
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                PixelsWriter pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                        WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                        aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
                aggregator.writeAggrOutput(pixelsWriter);
                pixelsWriter.close();
                if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
                workerMetrics.addOutputCostNs(writeCostTimer.stop());
                workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
                scanOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
            }

            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(scanOutput, workerMetrics);
            return scanOutput;
        } catch (Exception e)
        {
            logger.error("error during scan", e);
            scanOutput.setSuccessful(false);
            scanOutput.setErrorMessage(e.getMessage());
            scanOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return scanOutput;
        }
    }

    /**
     * Scan the files in a query split, apply projection and filters, and output the
     * results to the given path.
     * @param queryId the query id used by I/O scheduler
     * @param scanInputs the information of the files to scan
     * @param columnsToRead the included columns
     * @param inputScheme the storage scheme of the input files
     * @param scanProjection whether the column in columnsToRead is included in the scan output
     * @param filter the filter for the scan
     * @param outputPath fileName for the scan results
     * @param encoding whether encode the scan results or not
     * @param outputScheme the storage scheme for the scan result
     * @param partialAggregate whether perform partial aggregation on the scan result
     * @param aggregator the aggregator for the partial aggregation
     * @return the number of row groups that have been written into the output.
     */
    private int scanFile(long queryId, List<InputInfo> scanInputs, String[] columnsToRead, Storage.Scheme inputScheme,
                         boolean[] scanProjection, TableScanFilter filter, String outputPath, boolean encoding,
                         Storage.Scheme outputScheme, boolean partialAggregate, Aggregator aggregator)
    {
        PixelsWriter pixelsWriter = null;
        Scanner scanner = null;
        if (partialAggregate)
        {
            requireNonNull(aggregator, "aggregator is null whereas partialAggregate is true");
        }
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        for (InputInfo inputInfo : scanInputs)
        {
            readCostTimer.start();
            try (PixelsReader pixelsReader = WorkerCommon.getReader(inputInfo.getPath(), WorkerCommon.getStorage(inputScheme)))
            {
                readCostTimer.stop();
                if (inputInfo.getRgStart() >= pixelsReader.getRowGroupNum())
                {
                    continue;
                }
                if (inputInfo.getRgStart() + inputInfo.getRgLength() >= pixelsReader.getRowGroupNum())
                {
                    inputInfo.setRgLength(pixelsReader.getRowGroupNum() - inputInfo.getRgStart());
                }
                PixelsReaderOption option = WorkerCommon.getReaderOption(queryId, columnsToRead, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;
                CountDownLatch writerLatch=new CountDownLatch(1);
                CountDownLatch endfilterLatch=new CountDownLatch(1);

                // if (scanner == null)
                // {
                //     scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, scanProjection, filter);
                // }
                // if (pixelsWriter == null && !partialAggregate)
                // {
                //     writeCostTimer.start();
                //     pixelsWriter = WorkerCommon.getWriter(scanner.getOutputSchema(), WorkerCommon.getStorage(outputScheme),
                //             outputPath, encoding, false, null);
                //     writeCostTimer.stop();
                // }
                // rowBatch.e

                AsyncEventBus eventBus = new AsyncEventBus(Executors.newCachedThreadPool());
                BaseScanEventSubscriber scansubscriber=null;

                scansubscriber=new BaseScanEventSubscriber(filter,outputPath,rowBatchSchema,columnsToRead,scanProjection,outputScheme,encoding,writerLatch,endfilterLatch);
                eventBus.register(scansubscriber);
                
                do{
                    rowBatch=recordReader.readBatch(WorkerCommon.rowBatchSize);
                    eventBus.post(rowBatch);
                }while(!rowBatch.isEmpty());
                
                endfilterLatch.await();
                System.out.println("finish all the batches now we need to close writer");
            
                Thread.sleep(10000);
                eventBus.post(true);
                writerLatch.await();
                // Thread.sleep(10000);
                
                
                return 1;   
            } catch (Exception e)
            {
                throw new WorkerException("error during main process scanfile", e);
            }    
    }
    return 1;
}

}

class BaseScanEventSubscriber {
    private TableScanFilter filter;
    private String outPutPath;
    private PixelsWriter pixelsWriter;
    private Scanner scanner;
    private CountDownLatch writerLatch;
    private CountDownLatch endfilterLatch;
    // private TypeDescription rowBatchSchema;
    // private String[] columnsToRead;
    // private boolean[] scanProjection;
    // private Storage.Scheme outputScheme;
    // private boolean encoding;


    public BaseScanEventSubscriber(TableScanFilter filter,
    String outputpath,TypeDescription rowbatchschema,String[] columnstoread,
    boolean[] scanprojection,Storage.Scheme outputscheme,boolean encoding,CountDownLatch writerLatch, CountDownLatch endfilterLatch) { 
        this.filter =  filter;
        this.outPutPath=outputpath;
        this.writerLatch=writerLatch;
        this.endfilterLatch=endfilterLatch;
        // this.rowBatchSchema=rowbatchschema;
        // this.columnsToRead=columnstoread;
        // this.scanProjection=scanprojection;
        // this.outputScheme=outputscheme;
        // this.encoding =encoding;

        PixelsWriter pixelsWriter=null;
        Scanner scanner=new Scanner(WorkerCommon.rowBatchSize, rowbatchschema, columnstoread, scanprojection, this.filter);
        pixelsWriter=WorkerCommon.getWriter(scanner.getOutputSchema(), WorkerCommon.getStorage(outputscheme),
        this.outPutPath, encoding, false, null);
        this.pixelsWriter=pixelsWriter;
        this.scanner=scanner;
    } 

    @Subscribe
    public void onScan(VectorizedRowBatch event) {
        VectorizedRowBatch rowBatch=null;
        rowBatch=scanner.filterAndProject(event);
        if (rowBatch.size > 0 )
        {
            try{
                pixelsWriter.addRowBatch(rowBatch);
                if(rowBatch.endOfFile){
                    System.out.println("it's the end of file");
                    endfilterLatch.countDown();
                }
            }
            catch (Exception e)
            {
                throw new WorkerException("error during thread subscriber", e);
            }
        }
    }

    @Subscribe
    public void onScanCloseWriter(Boolean onScanCloseWriter) {

        
        if(onScanCloseWriter){
            try
            {
                // int numRowGroup = 0;
                if (pixelsWriter != null)
                {
                    // This is a pure scan without aggregation, compute time is the file writing time.
                    // writeCostTimer.add(computeCostTimer.getElapsedNs());
                    // writeCostTimer.start();
                    System.out.println("we are closing the writer");
                    pixelsWriter.close();
                    writerLatch.countDown();
                    // if (outputScheme == Storage.Scheme.minio)
                    // {
                    //     while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                    //     {
                    //         // Wait for 10ms and see if the output file is visible.
                    //         TimeUnit.MILLISECONDS.sleep(10);
                    //     }
                    // }
                    // writeCostTimer.stop();
                    // workerMetrics.addWriteBytes(pixelsWriter1.getCompletedBytes());
                    // workerMetrics.addNumWriteRequests(pixelsWriter1.getNumWriteRequests());
                    // workerMetrics.addOutputCostNs(writeCostTimer.getElapsedNs());
                    // numRowGroup = pixelsWriter1.getNumRowGroup();
                }
                // }
                // else
                // {
                //     workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
                // }
                // workerMetrics.addReadBytes(readBytes);
                // workerMetrics.addNumReadRequests(numReadRequests);
                // workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
                // return numRowGroup;
            } catch (Exception e)
            {
                throw new WorkerException("error during subscriber onScanclost operation", e);
            }     
        }
       
    }
}


