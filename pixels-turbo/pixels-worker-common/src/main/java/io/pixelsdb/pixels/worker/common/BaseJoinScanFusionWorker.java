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

import io.grpc.internal.LogExceptionRunnable;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.worker.common.BaseBroadcastJoinWorker;
import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker.ThreadScanProducer2;
import io.pixelsdb.pixels.planner.plan.logical.operation.ListNode;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalAggregate;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalFilter;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalProject;
import io.pixelsdb.pixels.planner.plan.logical.operation.ScanpipeInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.BroadcastTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanPipeInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.apache.logging.log4j.Logger;
import io.pixelsdb.pixels.executor.aggregation.Aggregator;
import com.alibaba.fastjson.JSON;

// import io.pixelsdb.pixels.worker.common.WorkerCommon;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
/** 
 * BaseJoinScanFusion is the combination of a set of partition joins and scan pipline(including scan project aggregate filter).
 * It contains a set of chain partitions joins combined with scan pipline.
 * The scan pipline happen in parallel with join pipline.
 * The 
 * 
 * 
 * @author Jingrong
 * @create 2023-07-17
 */
public class BaseJoinScanFusionWorker extends Worker<JoinScanFusionInput, FusionOutput>{

    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseJoinScanFusionWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
        this.workerMetrics.clear();
    }

    @Override
    public FusionOutput process(JoinScanFusionInput event){

        FusionOutput fusionOutput = new FusionOutput();
        long startTime = System.currentTimeMillis();
        fusionOutput.setStartTimeMs(startTime);
        fusionOutput.setRequestId(context.getRequestId());
        fusionOutput.setSuccessful(true);
        fusionOutput.setErrorMessage("");

        try{
            // broacastJoinAndPartition(event,fusionOutput);
            ExecutorService threadPool = Executors.newFixedThreadPool(2);
            CompletableFuture<FusionOutput> future = PartitionAndScan(event,fusionOutput,threadPool);

            // ExecutorService brocastJointhreadPool = Executors.newFixedThreadPool(2);
            // CompletableFuture<FusionOutput> future2 = broacastJoinAndPartition(event,fusionOutput,brocastJointhreadPool);

            future.get();
            System.out.println("success get future 1");
            // future2.get(); 
            // System.out.println("success get future 2");
            threadPool.shutdown();
            // System.out.println("success shutdown threadpool");
            // brocastJointhreadPool.shutdown();

            System.out.println("success execute all");
            return fusionOutput;
        } catch (Exception e)
        {
            logger.error("error during join", e);
            fusionOutput.setSuccessful(false);
            fusionOutput.setErrorMessage(e.getMessage());
            fusionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return fusionOutput;
        }

    }

    public void batchFactory(LinkedBlockingQueue<InputInfo> inputInfoQueue,List<InputSplit> inputSplits, long queryId, String[] includeCols,LinkedBlockingQueue<VectorizedRowBatch> blockingQueue,List<BatchToQueue> batchToQueueList,ExecutorService producerPool){
        if(inputInfoQueue==null){
            inputInfoQueue=new LinkedBlockingQueue<>();
        }
        for (InputSplit inputSplit : inputSplits)
        {
            List<InputInfo> scanInputs = inputSplit.getInputInfos();
            for (InputInfo inputInfo : scanInputs){
                try{
                    inputInfoQueue.put(inputInfo);
                } catch (InterruptedException e){
                    throw new WorkerException("error during putting inputInfo", e);
                }
            }
        }

        System.out.println("batchToQueueList size: "+batchToQueueList.size());
        // ExecutorService producerPool = Executors.newFixedThreadPool(batchToQueueList.size());
        for(int i=0;i<batchToQueueList.size();i++){
            producerPool.submit(batchToQueueList.get(i));
        }
        

    }
    
    public CompletableFuture<FusionOutput> PartitionAndScan(JoinScanFusionInput event, FusionOutput fusionOutput,ExecutorService threadPool){
        PartitionInput rightpartitionInput = event.getPartitionlargeTable();
        StorageInfo rightInputStorageInfo = requireNonNull(rightpartitionInput.getTableInfo().getStorageInfo(), "rightStorageInfo is null");
        int numPartition = event.getPartitionlargeTable().getPartitionInfo().getNumPartition();
        
        // System.out.println("number of partitions: "+numPartition);

        List<InputSplit> inputSplits = rightpartitionInput.getTableInfo().getInputSplits();
        // List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
        ScanpipeInfo scanPipeInfo =requireNonNull(event.getScanPipelineInfo(), "scanPipelineInfo is null");

        //do i need this?
        WorkerCommon.initStorage(rightInputStorageInfo);
        // for (int i = 0; i < numPartition; ++i)
        // {
        //     partitioned.add(new ConcurrentLinkedQueue<>());
        // }
        
        Set<String> finalToread = new HashSet<>();
        String[] partitionToread = event.getPartitionlargeTable().getTableInfo().getColumnsToRead();
        String[] ScanToread = scanPipeInfo.getIncludeCols();
        for(String s:partitionToread){
            finalToread.add(s);
        }
        for(String s:ScanToread){
            finalToread.add(s);
        }
        String[] includeColumns = finalToread.toArray(new String[finalToread.size()]);
        LinkedBlockingQueue<VectorizedRowBatch> blockingQueue = new LinkedBlockingQueue<>();
        
        //preparing batches
        CountDownLatch schemalatch = new CountDownLatch(1);
        // BatchToQueue batchToQueue = new BatchToQueue(event.getTransId(), includeColumns, blockingQueue,schemalatch);
        LinkedBlockingQueue<InputInfo> inputInfoQueue = new LinkedBlockingQueue<>();
        List<BatchToQueue> batchToQueueList = new ArrayList<>();

        CountDownLatch producerlatch = new CountDownLatch(2);
        batchToQueueList.add(new BatchToQueue(event.getTransId(), includeColumns, blockingQueue,inputInfoQueue,producerlatch,schemalatch,true));
        batchToQueueList.add(new BatchToQueue(event.getTransId(), includeColumns, blockingQueue,inputInfoQueue,producerlatch));
        
        ExecutorService producerPool = Executors.newFixedThreadPool(2);

        batchFactory(inputInfoQueue,inputSplits,event.getTransId(), includeColumns,blockingQueue,batchToQueueList,producerPool);

        System.out.println("i dont' need to compile again");

        System.out.println("thread keep running");
        try
        {   
            System.out.println("waiting for the latch");
            schemalatch.await();
        } catch (Exception e)
        {
            logger.error(String.format("error during waiting for the latch: %s", e));
            throw new WorkerException("error during waiting for the latch", e);
        }        

        TypeDescription rowBatchSchema = batchToQueueList.get(0).getRowBatchSchema();
        System.out.println("rowBatchSchema: "+rowBatchSchema.toString());

        String[] columnsToRead = event.getPartitionlargeTable().getTableInfo().getColumnsToRead();
        boolean[] projection = event.getPartitionlargeTable().getProjection();
        TableScanFilter filter = JSON.parseObject(event.getPartitionlargeTable().getTableInfo().getFilter(), TableScanFilter.class);
        int[] keyColumnIds = event.getPartitionlargeTable().getPartitionInfo().getKeyColumnIds();
        AtomicReference<TypeDescription> writerSchema = new AtomicReference<>();
        List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult = new ArrayList<>(numPartition);
        for (int i = 0; i < numPartition; ++i)
        {
            partitionResult.add(new ConcurrentLinkedQueue<>());
        }
        Set<Integer> hashValues = new HashSet<>(numPartition);
        
        StorageInfo outputStorageInfo = event.getFusionOutput().getStorageInfo();
        String outputPath = event.getFusionOutput().getPath();
        boolean encoding = event.getFusionOutput().isEncoding();


        CompletableFuture<FusionOutput> future = CompletableFuture.supplyAsync(() ->
        {
            try
            {   
                //initiating 
                boolean[] tablescanProjection = new boolean[columnsToRead.length];
                        // For tablescan Scanner 
                TableScanFilter tableScanFilter = TableScanFilter.empty("tpch", "lineitem");
                
                Aggregator aggregator = null;
                // PixelsWriter TableScanWriter = null;
                String [] tablescanColtoRead = scanPipeInfo.getIncludeCols();
                System.out.println("tablescanColtoRead: "+Arrays.toString(tablescanColtoRead));
                tablescanProjection = scanPipeInfo.getProjectFieldIds(tablescanColtoRead);
                boolean ispartialAggregate = scanPipeInfo.isPartialAggregation();
                

                if(ispartialAggregate){
                        PartialAggregationInfo partialAggregationInfo = scanPipeInfo.getPartialAggregationInfo();
                        boolean[] groupKeyProjection = new boolean[partialAggregationInfo.getGroupKeyColumnAlias().length];
                        Arrays.fill(groupKeyProjection, true);
                        // numpartitions
                        aggregator = new Aggregator(WorkerCommon.rowBatchSize, rowBatchSchema,
                        partialAggregationInfo.getGroupKeyColumnAlias(),
                        partialAggregationInfo.getGroupKeyColumnIds(), groupKeyProjection,
                        partialAggregationInfo.getAggregateColumnIds(),
                        partialAggregationInfo.getResultColumnAlias(),
                        partialAggregationInfo.getResultColumnTypes(),
                        partialAggregationInfo.getFunctionTypes(),
                        partialAggregationInfo.isPartition(),
                        partialAggregationInfo.getNumPartition());
                    }
        
                String tablescanOutput = outputPath + event.getFusionOutput().getFileNames().get(2);
                Scanner TableScanScanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, tablescanColtoRead, tablescanProjection, tableScanFilter);
                Scanner scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, projection, filter);
                PixelsWriter TableScanWriter = null;
                if(ispartialAggregate){
                    TableScanWriter = WorkerCommon.getWriter(TableScanScanner.getOutputSchema(), WorkerCommon.getStorage(outputStorageInfo.getScheme()),
                                tablescanOutput, encoding, false, null);
                }

                Partitioner partitioner = new Partitioner(numPartition, WorkerCommon.rowBatchSize,
                            scanner.getOutputSchema(), keyColumnIds);
                
                VectorizedRowBatch rowBatch;
                VectorizedRowBatch scanRowBatch;
                if (writerSchema.get() == null)
                {
                    writerSchema.weakCompareAndSet(null, scanner.getOutputSchema());
                }

                while ((rowBatch = blockingQueue.poll(3000, TimeUnit.MILLISECONDS)) != null )
                {   
                    scanRowBatch = rowBatch.clone();
                    scanRowBatch = TableScanScanner.filterAndProject(scanRowBatch);
                    if (rowBatch.size > 0)
                    {
                        if (ispartialAggregate)
                        {
                            aggregator.aggregate(scanRowBatch);
                        } else
                        {
                            TableScanWriter.addRowBatch(scanRowBatch);
                        }
                    }

                    //partition start partition the table!!!!!!!!!!!!!!!!!
                    if (writerSchema.get() == null)
                    {
                        writerSchema.weakCompareAndSet(null, scanner.getOutputSchema());
                    }

                    rowBatch = scanner.filterAndProject(rowBatch);
                    if (rowBatch.size > 0)
                    {     
                        // System.out.println("rowBatch size > 0");
                        Map<Integer, VectorizedRowBatch> result = partitioner.partition(rowBatch);
                        // Thread.sleep(1000);
                        // System.out.print("result size: "+ result.keySet());
                        if (!result.isEmpty())
                        {   
                            // System.out.println("result not empty");
                            for (Map.Entry<Integer, VectorizedRowBatch> entry : result.entrySet())
                            {
                                
                                partitionResult.get(entry.getKey()).add(entry.getValue());
                                
                            }
                        }
                        // 
                    }
                }
                

                // initiate the writer schema
                if (writerSchema.get() == null)
                {
                    TypeDescription fileSchema = WorkerCommon.getFileSchemaFromSplits(
                            WorkerCommon.getStorage(event.getOutput().getStorageInfo().getScheme()), inputSplits);
                    TypeDescription resultSchema = WorkerCommon.getResultSchema(fileSchema, columnsToRead);
                    writerSchema.set(resultSchema);
                }

                // write the partition output
                String partitionOutputPath = outputPath + event.getFusionOutput().getFileNames().get(1);
                PixelsWriter PartitionsWriter = WorkerCommon.getWriter(writerSchema.get(),
                WorkerCommon.getStorage(outputStorageInfo.getScheme()), partitionOutputPath, encoding,
                true, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));

                for (int hash = 0; hash < numPartition; ++hash)
                {
                    ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitionResult.get(hash);
                    if (!batches.isEmpty())
                    {
                        for (VectorizedRowBatch batch : batches)
                        {   
                            // System.out.print("batch size: "+batch.size);
                            PartitionsWriter.addRowBatch(batch, hash);
                        }
                        hashValues.add(hash);
                    }
                }
                
                
                
                // initial the table scan writer
                if(ispartialAggregate){
                    TableScanWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                                WorkerCommon.getStorage(outputStorageInfo.getScheme()), tablescanOutput, encoding,
                                aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
                } else {
                    TableScanWriter = WorkerCommon.getWriter(TableScanScanner.getOutputSchema(), WorkerCommon.getStorage(outputStorageInfo.getScheme()),
                                tablescanOutput, encoding, false, null);
                }
                //write the table scan output
                if (ispartialAggregate){
                    aggregator.writeAggrOutput(TableScanWriter);
                }

                System.out.println("success partition");
                
                
                producerlatch.await();
                producerPool.shutdown();
                // Thread.sleep(5000);
                PartitionsWriter.close();
                TableScanWriter.close();
                
                
                System.out.println("success shutdown");
                System.out.println("success before close");
                fusionOutput.addSecondPartitionOutput(new PartitionOutput(partitionOutputPath, hashValues));
                fusionOutput.setOutputs(new ArrayList<String> (Arrays.asList(tablescanOutput)));
                System.out.println("success close");
                return fusionOutput;
            } catch (Exception e)
            {
                logger.error(String.format("error during scan and partition: %s", e));
                throw new WorkerException("error during scan and partition", e);
            }
        }, threadPool);

        return future;
    }

    public CompletableFuture<FusionOutput>  broacastJoinAndPartition(JoinScanFusionInput event, FusionOutput fusionOutput,ExecutorService brocastJointhreadPool){
        CompletableFuture<FusionOutput> JoinAndPartitionfuture = CompletableFuture.supplyAsync(() ->{
        try{
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores);

            long transId = event.getTransId();
            BroadcastTableInfo leftTable = requireNonNull(event.getSmallTable(), "leftTable is null");
            StorageInfo leftInputStorageInfo = requireNonNull(leftTable.getStorageInfo(), "leftStorageInfo is null");
            List<InputSplit> leftInputs = requireNonNull(leftTable.getInputSplits(), "leftInputs is null");
            checkArgument(leftInputs.size() > 0, "left table is empty");
            String[] leftCols = leftTable.getColumnsToRead();
            int[] leftKeyColumnIds = leftTable.getKeyColumnIds();
            TableScanFilter leftFilter = JSON.parseObject(leftTable.getFilter(), TableScanFilter.class);

            BroadcastTableInfo rightTable = requireNonNull(event.getLargeTable(), "rightTable is null");
            StorageInfo rightInputStorageInfo = requireNonNull(rightTable.getStorageInfo(), "rightStorageInfo is null");
            List<InputSplit> rightInputs = requireNonNull(rightTable.getInputSplits(), "rightInputs is null");
            checkArgument(rightInputs.size() > 0, "right table is empty");
            String[] rightCols = rightTable.getColumnsToRead();
            int[] rightKeyColumnIds = rightTable.getKeyColumnIds();
            TableScanFilter rightFilter = JSON.parseObject(rightTable.getFilter(), TableScanFilter.class);

            String[] leftColAlias = event.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = event.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = event.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = event.getJoinInfo().getLargeProjection();
            JoinType joinType = event.getJoinInfo().getJoinType();
            checkArgument(joinType != JoinType.EQUI_LEFT && joinType != JoinType.EQUI_FULL,
                    "broadcast join can not be used for LEFT_OUTER or FULL_OUTER join");
            
            MultiOutputInfo outputInfo = event.getFusionOutput();
            String outputFolder = outputInfo.getPath();
            StorageInfo outputStorageInfo = outputInfo.getStorageInfo();
            checkArgument(outputInfo.getFileNames().size() == 3,
                    "it is incorrect to not have 3 output files");
            String postPartitionOutput = outputInfo.getFileNames().get(0);
            boolean partitionOutput = event.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }
            boolean encoding = outputInfo.isEncoding();
            
            logger.info("small table: " + event.getSmallTable().getTableName() +
            "', large table: " + event.getLargeTable().getTableName());

            // left input is the post partition output of the previous join
            // WorkerCommon.initStorage(leftInputStorageInfo);
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);
            
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            WorkerCommon.getFileSchemaFromSplits(threadPool,
                    WorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                    WorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                    leftSchema, rightSchema, leftInputs, rightInputs);
            
            Joiner joiner = new Joiner(joinType,
            WorkerCommon.getResultSchema(leftSchema.get(), leftCols), leftColAlias, leftProjection, leftKeyColumnIds,
            WorkerCommon.getResultSchema(rightSchema.get(), rightCols), rightColAlias, rightProjection, rightKeyColumnIds);
            
            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>();
            for (InputSplit inputSplit : leftInputs)
            {
                List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                leftFutures.add(threadPool.submit(() -> {
                    try
                    {
                        BaseBroadcastJoinWorker.buildHashTable(transId, joiner, inputs, leftInputStorageInfo.getScheme(),
                                !leftTable.isBase(), leftCols, leftFilter, workerMetrics);
                    }
                    catch (Exception e)
                    {
                        logger.error(String.format("error during hash table construction: %s", e));
                        throw new WorkerException("error during hash table construction", e);
                    }
                }));
            }
            for (Future future : leftFutures)
            {
                future.get();
            }
            logger.info("hash table size: " + joiner.getSmallTableSize() + ", duration (ns): " +
            (workerMetrics.getInputCostNs() + workerMetrics.getComputeCostNs()));

            // partition output
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> result = new ArrayList<>();
            if (partitionOutput)
            {
                for (int i = 0; i < outputPartitionInfo.getNumPartition(); ++i)
                {
                    result.add(new ConcurrentLinkedQueue<>());
                }
            }
            else
            {
                result.add(new ConcurrentLinkedQueue<>());
            }

            // scan the right table and do the join.
            if (joiner.getSmallTableSize() > 0)
            {
                for (InputSplit inputSplit : rightInputs)
                {
                    List<InputInfo> inputs = new LinkedList<>(inputSplit.getInputInfos());
                    threadPool.execute(() -> {
                        try
                        {
                            int numJoinedRows = partitionOutput ?
                            BaseBroadcastJoinWorker.joinWithRightTableAndPartition(
                                            transId, joiner, inputs, rightInputStorageInfo.getScheme(),
                                            !rightTable.isBase(), rightCols, rightFilter,
                                            outputPartitionInfo, result, workerMetrics) :
                            BaseBroadcastJoinWorker.joinWithRightTable(transId, joiner, inputs, rightInputStorageInfo.getScheme(),
                                            !rightTable.isBase(), rightCols, rightFilter, result.get(0), workerMetrics);
                        } catch (Exception e)
                        {
                            logger.error(String.format("error during broadcast join: %s", e));
                            throw new WorkerException("error during broadcast join", e);
                        }
                    });
                }
                threadPool.shutdown();
                try
                {
                    while (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) ;
                } catch (InterruptedException e)
                {
                    logger.error(String.format("interrupted while waiting for the termination of join: %s", e));
                    throw new WorkerException("interrupted while waiting for the termination of join", e);
                }
            }
            
            String outputPath = outputFolder + outputInfo.getFileNames().get(0);
            try
            {
                PixelsWriter pixelsWriter;
                WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
                if (partitionOutput)
                {
                    pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                            WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                            encoding, true, Arrays.stream(
                                    outputPartitionInfo.getKeyColumnIds()).boxed().
                                    collect(Collectors.toList()));
                    for (int hash = 0; hash < outputPartitionInfo.getNumPartition(); ++hash)
                    {
                        ConcurrentLinkedQueue<VectorizedRowBatch> batches = result.get(hash);
                        if (!batches.isEmpty())
                        {
                            for (VectorizedRowBatch batch : batches)
                            {
                                pixelsWriter.addRowBatch(batch, hash);
                            }
                        }
                    }
                }
                else
                {
                    pixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                            WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath,
                            encoding, false, null);
                    ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                    for (VectorizedRowBatch rowBatch : rowBatches)
                    {
                        pixelsWriter.addRowBatch(rowBatch);
                    }
                }
                pixelsWriter.close();

                PartitionOutput parOutput = new PartitionOutput();
                parOutput.setPath(outputPath);
                System.out.println("we are here");
                // System.out.println("grounum:"+pixelsWriter.getNumRowGroup());

                parOutput.setHashValuesWithNumber(pixelsWriter.getNumRowGroup());
                fusionOutput.addFirstPartitionOutput(parOutput);

                if (outputStorageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(outputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }
                // workerMetrics.addOutputCostNs(writeCostTimer.stop());
                // workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
                // workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());
            } catch (Exception e)
            {
                logger.error(String.format("failed to finish writing and close the join result file '%s': %s", outputPath, e));
                throw new WorkerException(
                        "failed to finish writing and close the join result file '" + outputPath + "'", e);
            }
            
            return fusionOutput;
        } catch (Exception e)
        {
            logger.error("error during broadcast join", e);
            return fusionOutput;
        }
        },brocastJointhreadPool);

        return JoinAndPartitionfuture;
    }


}
