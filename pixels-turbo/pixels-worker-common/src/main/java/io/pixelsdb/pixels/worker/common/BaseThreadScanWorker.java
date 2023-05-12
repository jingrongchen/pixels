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

import java.io.IOException;
import java.util.concurrent.*; 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;
import java.util.concurrent.Executors;

/**
 * Scan a table split and apply multi filter method
 *
 * @author Jingrong
 * @create 2023-05-08
 */
public class BaseThreadScanWorker extends Worker<ThreadScanInput, ScanOutput>{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;
    private int fileId;

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
            
            this.fileId=0;
            logger.info("start scan and aggregate");
            logger.info("start threadversion.start threadversion.start threadversion");

            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();
            
                threadPool.execute(() -> {
                    try
                    {   
                        // ThreadLocalRandom random = ThreadLocalRandom.current();
                        // int number = random.nextInt(1024);
                        List<String> folders= new ArrayList<String>();
                        for(String outputfolder :outputFolders){
                            folders.add(outputfolder + requestId + "_Threadscan_" + this.fileId);
                        }
                        this.fileId++;


                        int rowGroupNum = scanFile(queryId, scanInputs, includeCols, inputStorageInfo.getScheme(),
                                scanProjection, scanfilterlist, folders, encoding, outputStorageInfo.getScheme(),
                                partialAggregationPresent, aggregator);
                        
                                // if (rowGroupNum > 0)
                        // {
                        //     scanOutput.addOutput(outputFolders.get(0), rowGroupNum);
                        // }
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
                
                AsyncEventBus eventBus = new AsyncEventBus(Executors.newCachedThreadPool());

                for(int k=0; k<filter.size();k++){
                    ScanEventSubscriber scanevent=null;
                    scanevent=new ScanEventSubscriber(filter.get(k),outputPath.get(k),rowBatchSchema,columnsToRead,scanProjection,outputScheme,encoding);
                    eventBus.register(scanevent);
                }

                VectorizedRowBatch rowBatch=null;
                do{
                    rowBatch=recordReader.readBatch(WorkerCommon.rowBatchSize);
                    eventBus.post(rowBatch);
                }while(!rowBatch.isEmpty());

                
                

                // ConcurrentLinkedQueue<VectorizedRowBatch> queue = new ConcurrentLinkedQueue<VectorizedRowBatch>();
                
                // ExecutorService executorService = Executors.newFixedThreadPool(filter.size()+1);

                // executorService.submit(new ScanProducer(queue,recordReader));
                // // threadgoeshere
                // for(int k=0; k<filter.size();k++){
                //     executorService.submit(new filterThread(queue,filter.get(k),outputPath.get(k),rowBatchSchema,columnsToRead,scanProjection,outputScheme,encoding)); 
                // }
                // executorService.shutdown();
                // while(!executorService.isTerminated());
                
                // wait for the thread finish

                // try {//wait for all the  thread finish
                //     executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
                // } catch (InterruptedException e) {
                //     e.printStackTrace();
                // }

                // threadgoeshere

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
        return 0;
    }
    
}


class ScanEventSubscriber {
    private TableScanFilter filter;
    private String outPutPath;
    private PixelsWriter pixelsWriter;
    private Scanner scanner;
    // private TypeDescription rowBatchSchema;
    // private String[] columnsToRead;
    // private boolean[] scanProjection;
    // private Storage.Scheme outputScheme;
    // private boolean encoding;


    public ScanEventSubscriber(TableScanFilter filter,
    String outputpath,TypeDescription rowbatchschema,String[] columnstoread,
    boolean[] scanprojection,Storage.Scheme outputscheme,boolean encoding) { 
        this.filter =  filter;
        this.outPutPath=outputpath;
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
                
                    pixelsWriter.close();
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
                throw new WorkerException("error during close writer", e);
            }
        }
    }
}


class ScanProducer implements Runnable{
    private ConcurrentLinkedQueue<VectorizedRowBatch> queue;
    private PixelsRecordReader recordReader;
    
    public ScanProducer(ConcurrentLinkedQueue<VectorizedRowBatch> queue, PixelsRecordReader recordReader) {
        this.queue = queue;
        this.recordReader=recordReader;
    }

    public void run() {
        VectorizedRowBatch rowBatch=null;
        
        try{
            rowBatch=recordReader.readBatch(WorkerCommon.rowBatchSize);
            queue.offer(rowBatch);
        } catch (IOException e) {
            System.out.println("error in read first batch");
        }
        
        do{
            try{
                rowBatch=recordReader.readBatch(WorkerCommon.rowBatchSize);
                queue.offer(rowBatch);
                } catch (IOException e) {
                    System.out.println("error in read subsequent batch");
                }
        }while(!rowBatch.isEmpty());
    }
}



// class filterThread implements Callable<VectorizedRowBatch> { 
//     private ConcurrentLinkedQueue<VectorizedRowBatch> queue;
//     private TableScanFilter filter;
//     private String outPutPath;
//     private TypeDescription rowBatchSchema;
//     private String[] columnsToRead;
//     private boolean[] scanProjection;
//     private Storage.Scheme outputScheme;
//     private boolean encoding;
    
//     public filterThread(ConcurrentLinkedQueue<VectorizedRowBatch> queue,TableScanFilter filter,
//     String outputpath,TypeDescription rowbatchschema,String[] columnstoread,
//     boolean[] scanprojection,Storage.Scheme outputscheme,boolean encoding) { 
//         this.queue = queue;
//         this.filter =  filter;
//         this.outPutPath=outputpath;
//         this.rowBatchSchema=rowbatchschema;
//         this.columnsToRead=columnstoread;
//         this.scanProjection=scanprojection;
//         this.outputScheme=outputscheme;
//         this.encoding =encoding;
//     } 

//     /** 
//      * Reads a the whole VectorizedRowBatch and do filter and writes to file
//      * 
//      * @return 
//      * @throws Exception 
//      */
//     public VectorizedRowBatch call() throws IOException { 
//         PixelsWriter pixelsWriter=null;
//         Scanner scanner=new Scanner(WorkerCommon.rowBatchSize, this.rowBatchSchema, this.columnsToRead, this.scanProjection, this.filter);
//         pixelsWriter=WorkerCommon.getWriter(scanner.getOutputSchema(), WorkerCommon.getStorage(this.outputScheme),
//         this.outPutPath, encoding, false, null);
//         VectorizedRowBatch a=null;


        

//         for (VectorizedRowBatch rawBatch:allRawBacth){
//             VectorizedRowBatch rowBatch=null;
//             rowBatch=scanner.filterAndProject(rawBatch);
//             if (rowBatch.size > 0 )
//             {
//                 pixelsWriter.addRowBatch(rowBatch);
//             }
//         }




//         try
//         {
//             // int numRowGroup = 0;
//             if (pixelsWriter != null)
//             {
//                 // This is a pure scan without aggregation, compute time is the file writing time.
//                 // writeCostTimer.add(computeCostTimer.getElapsedNs());
//                 // writeCostTimer.start();
            
//                 pixelsWriter.close();
//                 // if (outputScheme == Storage.Scheme.minio)
//                 // {
//                 //     while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
//                 //     {
//                 //         // Wait for 10ms and see if the output file is visible.
//                 //         TimeUnit.MILLISECONDS.sleep(10);
//                 //     }
//                 // }
//                 // writeCostTimer.stop();
//                 // workerMetrics.addWriteBytes(pixelsWriter1.getCompletedBytes());
//                 // workerMetrics.addNumWriteRequests(pixelsWriter1.getNumWriteRequests());
//                 // workerMetrics.addOutputCostNs(writeCostTimer.getElapsedNs());
//                 // numRowGroup = pixelsWriter1.getNumRowGroup();
//             }
//             // }
//             // else
//             // {
//             //     workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
//             // }
//             // workerMetrics.addReadBytes(readBytes);
//             // workerMetrics.addNumReadRequests(numReadRequests);
//             // workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
//             // return numRowGroup;
//         } catch (Exception e)
//         {
//             throw new IOException();
//         }
//         return a;         
//     } 
// }