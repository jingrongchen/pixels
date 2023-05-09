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
import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Scan a table split and apply multi filter method
 *
 * @author Jingrong
 * @create 2023-05-08
 */
public class BaseThreadScanWorker extends Worker<ThreadScanInput, ScanOutput>{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseThreadScanWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
    }

    @Override
    public ScanOutput process(ThreadScanInput event)
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
            List<String> outputFolders = event.getOutput().getPath();
            StorageInfo outputStorageInfo = event.getOutput().getStorageInfo();
            // for (String outputFolder:outputFolders ){
            //     int i=0;
            //     if (!outputFolder.endsWith("/"))
            //     {
            //         outputFolders.set(i, outputFolder+"/");
            //         i++;
            //     }
            // }
            boolean encoding = event.getOutput().isEncoding();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            String[] includeCols = event.getTableInfo().getColumnsToRead();
            List<String> filterlist=event.getTableInfo().getFilter();
            List<TableScanFilter> scanfilterlist=new ArrayList<TableScanFilter>();

            for (String filter:filterlist){
                scanfilterlist.add(JSON.parseObject(filter, TableScanFilter.class));
            }
            

            Aggregator aggregator=null;
            // if (partialAggregationPresent)
            // {
            //     logger.info("start get output schema");
            //     TypeDescription inputSchema = WorkerCommon.getFileSchemaFromSplits(
            //             WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
            //     inputSchema = WorkerCommon.getResultSchema(inputSchema, includeCols);
            //     PartialAggregationInfo partialAggregationInfo = event.getPartialAggregationInfo();
            //     requireNonNull(partialAggregationInfo, "event.partialAggregationInfo is null");
            //     boolean[] groupKeyProjection = new boolean[partialAggregationInfo.getGroupKeyColumnAlias().length];
            //     Arrays.fill(groupKeyProjection, true);
            //     aggregator = new Aggregator(WorkerCommon.rowBatchSize, inputSchema,
            //             partialAggregationInfo.getGroupKeyColumnAlias(),
            //             partialAggregationInfo.getGroupKeyColumnIds(), groupKeyProjection,
            //             partialAggregationInfo.getAggregateColumnIds(),
            //             partialAggregationInfo.getResultColumnAlias(),
            //             partialAggregationInfo.getResultColumnTypes(),
            //             partialAggregationInfo.getFunctionTypes(),
            //             partialAggregationInfo.isPartition(),
            //             partialAggregationInfo.getNumPartition());
            // }
            // else
            // {
            //     aggregator = null;
            // }
            
            String outfolerprefix1 = outputFolders.get(0);
            String outfolerprefix2 = outputFolders.get(1);
            int outputId = 0;
            logger.info("start scan and aggregate");
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();
            
                threadPool.execute(() -> {
                    try
                    {   
                        ThreadLocalRandom random = ThreadLocalRandom.current();
                        int number = random.nextInt(1024);
                        String folder1= outfolerprefix1 + requestId + "_scan1_" + number;
                        String folder2= outfolerprefix2 + requestId + "_scan2_" + number;
                        List<String> folders= new ArrayList<String>();
                        folders.add(folder1);
                        folders.add(folder2);
                        
                        System.out.println("folder1: "+ folders.get(0));
                        System.out.println("folder2: "+ folders.get(1));
                        int rowGroupNum = scanFile(queryId, scanInputs, includeCols, inputStorageInfo.getScheme(),
                                scanProjection, scanfilterlist, folders, encoding, outputStorageInfo.getScheme(),
                                partialAggregationPresent, aggregator);
                        if (rowGroupNum > 0)
                        {
                            scanOutput.addOutput(outputFolders.get(0), rowGroupNum);
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

            // logger.info("start write aggregation result");
            // if (partialAggregationPresent)
            // {
            //     String outputPath = event.getOutput().getPath();
            //     WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
            //     PixelsWriter pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
            //             WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
            //             aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
            //     aggregator.writeAggrOutput(pixelsWriter);
            //     pixelsWriter.close();
            //     if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
            //     {
            //         while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
            //         {
            //             // Wait for 10ms and see if the output file is visible.
            //             TimeUnit.MILLISECONDS.sleep(10);
            //         }
            //     }
            //     workerMetrics.addOutputCostNs(writeCostTimer.stop());
            //     workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
            //     workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
            //     scanOutput.addOutput(outputPath, pixelsWriter.getNumRowGroup());
            // }

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
                         boolean[] scanProjection, List<TableScanFilter> filter, List<String> outputPath, boolean encoding,
                         Storage.Scheme outputScheme, boolean partialAggregate, Aggregator aggregator)
    {
        PixelsWriter pixelsWriter1 = null;
        PixelsWriter pixelsWriter2 = null;
        Scanner scanner1 = null;
        Scanner scanner2 = null;
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
                VectorizedRowBatch rowBatch1;
                VectorizedRowBatch rowBatch2;
                if (scanner1 == null && scanner2==null)
                {
                    scanner1 = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, scanProjection, filter.get(0));
                    scanner2 = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, scanProjection, filter.get(1));
                }
                if (pixelsWriter1 == null && pixelsWriter2 == null && !partialAggregate)
                {
                    writeCostTimer.start();
                    pixelsWriter1 = WorkerCommon.getWriter(scanner1.getOutputSchema(), WorkerCommon.getStorage(outputScheme),
                            outputPath.get(0), encoding, false, null);
                    
                    
                    pixelsWriter2 = WorkerCommon.getWriter(scanner2.getOutputSchema(), WorkerCommon.getStorage(outputScheme),
                    outputPath.get(1), encoding, false, null);
                    writeCostTimer.stop();
                }

                computeCostTimer.start();
                do
                {   
                    VectorizedRowBatch commonprocess = recordReader.readBatch(WorkerCommon.rowBatchSize);
                    rowBatch1 = scanner1.filterAndProject(commonprocess);
                    rowBatch2 = scanner2.filterAndProject(commonprocess);
                    if (rowBatch1.size > 0 && rowBatch2.size>0)
                    {
                        // if (partialAggregate)
                        // {
                        //     aggregator.aggregate(rowBatch1);
                        // } else
                        // {
                        pixelsWriter1.addRowBatch(rowBatch1);
                        pixelsWriter2.addRowBatch(rowBatch2);
                        // }
                    }
                } while (!rowBatch1.endOfFile && !rowBatch2.endOfFile);
                computeCostTimer.stop();
                computeCostTimer.minus(recordReader.getReadTimeNanos());
                readCostTimer.add(recordReader.getReadTimeNanos());
                readBytes += recordReader.getCompletedBytes();
                numReadRequests += recordReader.getNumReadRequests();
            } catch (Exception e)
            {
                throw new WorkerException("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the result", e);
            }
        }
        // Finished scanning all the files in the split.
        try
        {
            int numRowGroup = 0;
            if (pixelsWriter1 != null && pixelsWriter1 != null)
            {
                // This is a pure scan without aggregation, compute time is the file writing time.
                writeCostTimer.add(computeCostTimer.getElapsedNs());
                writeCostTimer.start();
                pixelsWriter1.close();
                pixelsWriter2.close();
                // if (outputScheme == Storage.Scheme.minio)
                // {
                //     while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                //     {
                //         // Wait for 10ms and see if the output file is visible.
                //         TimeUnit.MILLISECONDS.sleep(10);
                //     }
                // }
                writeCostTimer.stop();
                workerMetrics.addWriteBytes(pixelsWriter1.getCompletedBytes());
                workerMetrics.addNumWriteRequests(pixelsWriter1.getNumWriteRequests());
                workerMetrics.addOutputCostNs(writeCostTimer.getElapsedNs());
                numRowGroup = pixelsWriter1.getNumRowGroup();
            }
            else
            {
                workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
            }
            workerMetrics.addReadBytes(readBytes);
            workerMetrics.addNumReadRequests(numReadRequests);
            workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
            return numRowGroup;
        } catch (Exception e)
        {
            throw new WorkerException(
                    "failed finish writing and close the output file '" + outputPath + "'", e);
        }
    }
}






