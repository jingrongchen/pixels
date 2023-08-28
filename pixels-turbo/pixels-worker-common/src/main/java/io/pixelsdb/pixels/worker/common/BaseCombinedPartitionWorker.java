package io.pixelsdb.pixels.worker.common;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.worker.common.Worker;
import redis.clients.jedis.search.Schema;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Logger;
import java.util.concurrent.Future;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import java.io.File;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.physical.StorageProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.PixelsReaderImpl;
import io.pixelsdb.pixels.core.PixelsWriter;
import io.pixelsdb.pixels.core.PixelsWriterImpl;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.join.Joiner;
import io.pixelsdb.pixels.executor.join.Partitioner;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.executor.scan.Scanner;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.CombinedPartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.core.PixelsFooterCache;
import static java.util.Objects.requireNonNull;
import java.util.Iterator;
import java.io.IOException;

public class BaseCombinedPartitionWorker extends Worker<CombinedPartitionInput, JoinOutput>{
    private final Logger logger;
    private final WorkerMetrics workerMetrics;

    public BaseCombinedPartitionWorker(WorkerContext context)
    {
        super(context);
        this.logger = context.getLogger();
        this.workerMetrics = context.getWorkerMetrics();
    }
    
    @Override
    public JoinOutput process(CombinedPartitionInput event){
        JoinOutput joinOutput = new JoinOutput();
        long startTime = System.currentTimeMillis();
        joinOutput.setStartTimeMs(startTime);
        joinOutput.setRequestId(context.getRequestId());
        joinOutput.setSuccessful(true);
        joinOutput.setErrorMessage("");

        try{   
            // the combined partition process configuration
            List<Integer> JoinhashValues = event.getJoinInfo().getHashValues();
            String localPartitionPath = event.getOutput().getPath();
            String intermediatePath = event.getIntermideatePath();
            String bigTableName = event.getBigTableName();
            String smallTableName = event.getSmallTableName();
            int bigTablePartitionCount = event.getBigTablePartitionCount();
            int smallTablePartitionCount = event.getSmallTablePartitionCount();
            int lassversion= event.getLassversion();
            //the normal partition process
            int cores = Runtime.getRuntime().availableProcessors();
            logger.info("Number of cores available: " + cores);
            WorkerThreadExceptionHandler exceptionHandler = new WorkerThreadExceptionHandler(logger);
            ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2,
                    new WorkerThreadFactory(exceptionHandler));

            long transId = event.getTransId();
            requireNonNull(event.getTableInfo(), "event.tableInfo is null");
            StorageInfo inputStorageInfo = event.getTableInfo().getStorageInfo();
            List<InputSplit> inputSplits = event.getTableInfo().getInputSplits();
            requireNonNull(event.getPartitionInfo(), "event.partitionInfo is null");
            int numPartition = event.getPartitionInfo().getNumPartition();
            logger.info("table '" + event.getTableInfo().getTableName() +
                    "', number of partitions (" + numPartition + ")");
            int[] keyColumnIds = event.getPartitionInfo().getKeyColumnIds();
            boolean[] projection = event.getProjection();
            requireNonNull(event.getOutput(), "event.output is null");
            StorageInfo outputStorageInfo = requireNonNull(event.getOutput().getStorageInfo(),
                    "output.storageInfo is null");
            String outputPath = event.getOutput().getPath();
            boolean encoding = event.getOutput().isEncoding();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            String[] columnsToRead = event.getTableInfo().getColumnsToRead();
            TableScanFilter filter = JSON.parseObject(event.getTableInfo().getFilter(), TableScanFilter.class);
            AtomicReference<TypeDescription> writerSchema = new AtomicReference<>();
            // The partitioned data would be kept in memory.
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitioned = new ArrayList<>(numPartition);
            for (int i = 0; i < numPartition; ++i)
            {
                partitioned.add(new ConcurrentLinkedQueue<>());
            }
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();

                threadPool.execute(() -> {
                    try
                    {
                        partitionFile(transId, scanInputs, columnsToRead, inputStorageInfo.getScheme(),
                                filter, keyColumnIds, projection, partitioned, writerSchema);
                    }
                    catch (Throwable e)
                    {
                        throw new WorkerException("error during partitioning", e);
                    }
                });
            }
            threadPool.shutdown();
            try
            {
                while (!threadPool.awaitTermination(60, TimeUnit.SECONDS));
            } catch (InterruptedException e)
            {
                throw new WorkerException("interrupted while waiting for the termination of partitioning", e);
            }

            if (exceptionHandler.hasException())
            {
                throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
            }

            WorkerMetrics.Timer writeCostTimer = new WorkerMetrics.Timer().start();
            if (writerSchema.get() == null)
            {
                TypeDescription fileSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
                TypeDescription resultSchema = WorkerCommon.getResultSchema(fileSchema, columnsToRead);
                writerSchema.set(resultSchema);
            }
            PixelsWriter pixelsWriter = WorkerCommon.getWriter(writerSchema.get(),
                    WorkerCommon.getStorage(outputStorageInfo.getScheme()), outputPath, encoding,
                    true, Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));
            Set<Integer> hashValues = new HashSet<>(numPartition);
            
            List<String> localFiles = new ArrayList<String>();

            //the writer writes to the tmp file
            String[] stringparts = localPartitionPath.split("/");
            // lastElement
            String lastElement = stringparts[stringparts.length - 1];
            String tmpFilePath = "/tmp"+"/"+lastElement;
            String toRemove = tmpFilePath.split("/")[2];
            System.out.println("local cached file:" + toRemove);
            localFiles.add(toRemove);
            

            ConfigFactory configFactory = ConfigFactory.Instance();
            Storage storage = StorageFactory.Instance().getStorage(Storage.Scheme.file);
            // Storage storage = 
            PixelsWriterImpl.Builder builder = PixelsWriterImpl.newBuilder()
                .setSchema(writerSchema.get())
                .setPixelStride(Integer.parseInt(configFactory.getProperty("pixel.stride")))
                .setRowGroupSize(Integer.parseInt(configFactory.getProperty("row.group.size")))
                .setStorage(storage)
                .setPath(tmpFilePath)
                .setOverwrite(true) // set overwrite to true to avoid existence checking.
                .setEncoding(encoding)
                .setPartitioned(true);
            builder.setPartKeyColumnIds(Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));
            PixelsWriter tmpWriter = builder.build();

            for (int hash = 0; hash < numPartition; ++hash)
            {
                ConcurrentLinkedQueue<VectorizedRowBatch> batches = partitioned.get(hash);
                if (!batches.isEmpty())
                {   
                    
                    for (VectorizedRowBatch batch : batches)
                    {                           
                        pixelsWriter.addRowBatch(batch, hash);
                    }
                    hashValues.add(hash);
                }

                if (JoinhashValues.contains(hash))
                {   
                    if (!batches.isEmpty())
                    {    
                        for (VectorizedRowBatch batch : batches)
                        {                           
                            tmpWriter.addRowBatch(batch, hash);
                        }
                        hashValues.add(hash);
                    }
                }

            }
            // partitionOutput.setPath(outputPath);
            // partitionOutput.setHashValues(hashValues);
            pixelsWriter.close();
            tmpWriter.close();
            workerMetrics.addOutputCostNs(writeCostTimer.stop());
            workerMetrics.addWriteBytes(pixelsWriter.getCompletedBytes());
            workerMetrics.addNumWriteRequests(pixelsWriter.getNumWriteRequests());

            // partitionOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            // WorkerCommon.setPerfMetrics(partitionOutput, workerMetrics);

            // second stage: join!!! need to wait until it's ready 
            // createria:loop check both small and big table should be ready

            String smallTablePath = intermediatePath + smallTableName + "_partition/";
            String bigTablePath = intermediatePath + bigTableName + "_partition/";
            StorageFactory storageFactory = StorageFactory.Instance();
            Storage storage1 = storageFactory.getStorage(smallTablePath);
            Storage storage2 = storageFactory.getStorage(bigTablePath);

            // List<String> notLocal = new ArrayList<String>();
            File folder = new File("/tmp/");
            System.out.println("lass version: "+lassversion);
            while(storage1.listPaths(smallTablePath).size()!= smallTablePartitionCount && storage2.listPaths(bigTablePath).size()!= bigTablePartitionCount)
            {   
                if(lassversion==2){
                    System.out.println("inside waiting loop");
                    List<String> toLoad = storage2.listPaths(bigTablePath);
                    System.out.println("local files :"+localFiles);

                    for (String file:localFiles){
                        toLoad.remove(bigTablePath+file);
                    }
                    System.out.println("toLoad:"+toLoad);

                    if (toLoad.size() != 0 && (folder.getUsableSpace()/(1024*1024)) > 200)
                    {
                        String filetoPull = toLoad.get(0);
                        
                        int index = filetoPull.lastIndexOf('/');
                        String fileName = filetoPull.substring(index +1);
                        // System.out.println("fileName:"+fileName);

                        String filetoWrite = "/tmp/"+fileName;
                        System.out.println("filetoPull:"+filetoPull);
                        
                        //Initiate the writer for the pull partitioned file
                        PixelsWriterImpl.Builder pullbuilder = PixelsWriterImpl.newBuilder()
                            .setSchema(writerSchema.get())
                            .setPixelStride(Integer.parseInt(configFactory.getProperty("pixel.stride")))
                            .setRowGroupSize(Integer.parseInt(configFactory.getProperty("row.group.size")))
                            .setStorage(storage)
                            .setPath(filetoWrite)
                            .setOverwrite(true) // set overwrite to true to avoid existence checking.
                            .setEncoding(encoding)
                            .setPartitioned(true);
                        pullbuilder.setPartKeyColumnIds(Arrays.stream(keyColumnIds).boxed().collect(Collectors.toList()));
                        PixelsWriter pullWriter = pullbuilder.build();
                        VectorizedRowBatch pullRowBatch;
                        
                        PixelsReader pixelsReader = WorkerCommon.getReader(bigTablePath + filetoPull, WorkerCommon.getStorage(Storage.Scheme.s3));
                        for (int hashValue:JoinhashValues){
                            PixelsReaderOption option = WorkerCommon.getReaderOption(transId, event.getLargeTable().getColumnsToRead(), pixelsReader,
                            hashValue, numPartition);
                            PixelsRecordReader recordReader = pixelsReader.read(option);

                            do{
                                pullRowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                                if (pullRowBatch.isEmpty())
                                {   
                                    System.out.println("pullRowBatch is empty, break");
                                    break;
                                }
                                pullWriter.addRowBatch(pullRowBatch, hashValue);
                            }while(!pullRowBatch.isEmpty());

                            workerMetrics.addReadBytes(recordReader.getCompletedBytes()); 
                        }
                        

                        pullWriter.close();
                        System.out.println("pullWriter completed");
                        localFiles.add(fileName);
                    }else{
                        System.out.println("no file to pull or space is full, going to sleep");
                        Thread.sleep(500);
                    }

                } else if(lassversion==1){
                    System.out.println("simply waiting for files to be ready");
                    Thread.sleep(500);
                } else{
                    System.out.println("wrong lassversion");
                }

            }
            
            System.out.println("All partitions are ready, start join!");
            // read cached partitioned and other files
            ExecutorService joinPool = Executors.newFixedThreadPool(cores * 3,
                    new WorkerThreadFactory(exceptionHandler));

            requireNonNull(event.getSmallTable(), "event.smallTable is null");
            StorageInfo leftInputStorageInfo = event.getSmallTable().getStorageInfo();
            List<String> leftPartitioned = event.getSmallTable().getInputFiles();
            requireNonNull(leftPartitioned, "leftPartitioned is null");
            checkArgument(leftPartitioned.size() > 0, "leftPartitioned is empty");
            int leftParallelism = event.getSmallTable().getParallelism();
            checkArgument(leftParallelism > 0, "leftParallelism is not positive");
            String[] leftColumnsToRead = event.getSmallTable().getColumnsToRead();
            int[] leftKeyColumnIds = event.getSmallTable().getKeyColumnIds();

            requireNonNull(event.getLargeTable(), "event.largeTable is null");
            StorageInfo rightInputStorageInfo = event.getLargeTable().getStorageInfo();
            List<String> rightPartitioned = event.getLargeTable().getInputFiles();
            requireNonNull(rightPartitioned, "rightPartitioned is null");
            checkArgument(rightPartitioned.size() > 0, "rightPartitioned is empty");
            int rightParallelism = event.getLargeTable().getParallelism();
            checkArgument(rightParallelism > 0, "rightParallelism is not positive");
            String[] rightColumnsToRead = event.getLargeTable().getColumnsToRead();
            int[] rightKeyColumnIds = event.getLargeTable().getKeyColumnIds();

            String[] leftColAlias = event.getJoinInfo().getSmallColumnAlias();
            String[] rightColAlias = event.getJoinInfo().getLargeColumnAlias();
            boolean[] leftProjection = event.getJoinInfo().getSmallProjection();
            boolean[] rightProjection = event.getJoinInfo().getLargeProjection();
            JoinType joinType = event.getJoinInfo().getJoinType();
            
            // List<Integer> JoinhashValues = event.getJoinInfo().getHashValues();
            int JoinnumPartition = event.getJoinInfo().getNumPartition();
            logger.info("small table: " + event.getSmallTable().getTableName() +
                    ", large table: " + event.getLargeTable().getTableName() +
                    ", number of partitions (" + JoinnumPartition + ")");

            MultiOutputInfo outputInfo = event.getMultiOutput();
            StorageInfo JoinoutputStorageInfo = outputInfo.getStorageInfo();

            if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
            {
                checkArgument(outputInfo.getFileNames().size() == 2,
                        "it is incorrect to have more than two output files");
            }
            else
            {
                checkArgument(outputInfo.getFileNames().size() == 1,
                        "it is incorrect to have more than one output files");
            }
            String outputFolder = outputInfo.getPath();
            if (!outputFolder.endsWith("/"))
            {
                outputFolder += "/";
            }
            boolean joinencoding = outputInfo.isEncoding();

            boolean partitionOutput = event.getJoinInfo().isPostPartition();
            PartitionInfo outputPartitionInfo = event.getJoinInfo().getPostPartitionInfo();
            if (partitionOutput)
            {
                requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
            }

            WorkerCommon.initStorage(leftInputStorageInfo);
            WorkerCommon.initStorage(rightInputStorageInfo);
            WorkerCommon.initStorage(JoinoutputStorageInfo);


            // build the joiner.
            AtomicReference<TypeDescription> leftSchema = new AtomicReference<>();
            AtomicReference<TypeDescription> rightSchema = new AtomicReference<>();
            WorkerCommon.getFileSchemaFromPaths(joinPool,
                    WorkerCommon.getStorage(leftInputStorageInfo.getScheme()),
                    WorkerCommon.getStorage(rightInputStorageInfo.getScheme()),
                    leftSchema, rightSchema, leftPartitioned, rightPartitioned);
            /*
             * Issue #450:
             * For the left and the right partial partitioned files, the file schema is equal to the columns to read in normal cases.
             * However, it is safer to turn file schema into result schema here.
             */
            Joiner joiner = new Joiner(joinType,
                    WorkerCommon.getResultSchema(leftSchema.get(), leftColumnsToRead),
                    leftColAlias, leftProjection, leftKeyColumnIds,
                    WorkerCommon.getResultSchema(rightSchema.get(), rightColumnsToRead),
                    rightColAlias, rightProjection, rightKeyColumnIds);
            // build the hash table for the left table.
            List<Future> leftFutures = new ArrayList<>(leftPartitioned.size());
            int leftSplitSize = leftPartitioned.size() / leftParallelism;
            if (leftPartitioned.size() % leftParallelism > 0)
            {
                leftSplitSize++;
            }
            for (int i = 0; i < leftPartitioned.size(); i += leftSplitSize)
            {
                List<String> parts = new LinkedList<>();
                for (int j = i; j < i + leftSplitSize && j < leftPartitioned.size(); ++j)
                {
                    parts.add(leftPartitioned.get(j));
                }
                leftFutures.add(joinPool.submit(() -> {
                    try
                    {
                        buildHashTable(transId, joiner, parts, leftColumnsToRead, leftInputStorageInfo.getScheme(),
                                JoinhashValues, JoinnumPartition, workerMetrics);
                    }
                    catch (Throwable e)
                    {
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

            System.out.println("pass buid hash table");

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
                int rightSplitSize = rightPartitioned.size() / rightParallelism;
                if (rightPartitioned.size() % rightParallelism > 0)
                {
                    rightSplitSize++;
                }

                for (int i = 0; i < rightPartitioned.size(); i += rightSplitSize)
                {
                    List<String> parts = new LinkedList<>();
                    for (int j = i; j < i + rightSplitSize && j < rightPartitioned.size(); ++j)
                    {
                        parts.add(rightPartitioned.get(j));
                    }
                    joinPool.execute(() -> {
                        try
                        {
                            int numJoinedRows = partitionOutput ?
                            joinWithRightTableAndPartition(
                                            transId, joiner, parts, rightColumnsToRead,
                                            rightInputStorageInfo.getScheme(), JoinhashValues,
                                            JoinnumPartition, outputPartitionInfo, result, workerMetrics, localFiles) :
                            joinWithRightTable(transId, joiner, parts, rightColumnsToRead,
                                            rightInputStorageInfo.getScheme(), JoinhashValues, JoinnumPartition,
                                            result.get(0), workerMetrics, localFiles);
                        } catch (Throwable e)
                        {
                            throw new WorkerException("error during hash join", e);
                        }
                    });
                }
                joinPool.shutdown();
                try
                {
                    while (!joinPool.awaitTermination(60, TimeUnit.SECONDS)) ;
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the termination of join", e);
                }

                if (exceptionHandler.hasException())
                {
                    throw new WorkerException("error occurred threads, please check the stacktrace before this log record");
                }
            }

            String JoinoutputPath = outputFolder + outputInfo.getFileNames().get(0);
            try
            {
                WorkerMetrics.Timer JoinwriteCostTimer = new WorkerMetrics.Timer().start();
                PixelsWriter joinpixelsWriter;
                if (partitionOutput)
                {
                    joinpixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                            WorkerCommon.getStorage(JoinoutputStorageInfo.getScheme()), JoinoutputPath,
                            joinencoding, true, Arrays.stream(
                                            outputPartitionInfo.getKeyColumnIds()).boxed().
                                    collect(Collectors.toList()));
                    for (int hash = 0; hash < outputPartitionInfo.getNumPartition(); ++hash)
                    {
                        ConcurrentLinkedQueue<VectorizedRowBatch> batches = result.get(hash);
                        if (!batches.isEmpty())
                        {
                            for (VectorizedRowBatch batch : batches)
                            {
                                joinpixelsWriter.addRowBatch(batch, hash);
                            }
                        }
                    }
                }
                else
                {
                    joinpixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                            WorkerCommon.getStorage(JoinoutputStorageInfo.getScheme()), JoinoutputPath,
                            joinencoding, false, null);
                    ConcurrentLinkedQueue<VectorizedRowBatch> rowBatches = result.get(0);
                    for (VectorizedRowBatch rowBatch : rowBatches)
                    {
                        joinpixelsWriter.addRowBatch(rowBatch);
                    }
                }
                joinpixelsWriter.close();
                workerMetrics.addWriteBytes(joinpixelsWriter.getCompletedBytes());
                workerMetrics.addNumWriteRequests(joinpixelsWriter.getNumWriteRequests());
                joinOutput.addOutput(JoinoutputPath, joinpixelsWriter.getNumRowGroup());
                if (JoinoutputStorageInfo.getScheme() == Storage.Scheme.minio)
                {
                    while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(JoinoutputPath))
                    {
                        // Wait for 10ms and see if the output file is visible.
                        TimeUnit.MILLISECONDS.sleep(10);
                    }
                }

                if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
                {
                    // output the left-outer tail.
                    outputPath = outputFolder + outputInfo.getFileNames().get(1);
                    if (partitionOutput)
                    {
                        requireNonNull(outputPartitionInfo, "outputPartitionInfo is null");
                        joinpixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(JoinoutputStorageInfo.getScheme()), outputPath,
                                joinencoding, true, Arrays.stream(
                                        outputPartitionInfo.getKeyColumnIds()).boxed().
                                        collect(Collectors.toList()));
                        joiner.writeLeftOuterAndPartition(joinpixelsWriter, WorkerCommon.rowBatchSize,
                                outputPartitionInfo.getNumPartition(), outputPartitionInfo.getKeyColumnIds());
                    }
                    else
                    {
                        joinpixelsWriter = WorkerCommon.getWriter(joiner.getJoinedSchema(),
                                WorkerCommon.getStorage(JoinoutputStorageInfo.getScheme()), JoinoutputPath,
                                joinencoding, false, null);
                        joiner.writeLeftOuter(joinpixelsWriter, WorkerCommon.rowBatchSize);
                    }
                    joinpixelsWriter.close();
                    workerMetrics.addWriteBytes(joinpixelsWriter.getCompletedBytes());
                    workerMetrics.addNumWriteRequests(joinpixelsWriter.getNumWriteRequests());
                    joinOutput.addOutput(JoinoutputPath, joinpixelsWriter.getNumRowGroup());
                    if (JoinoutputStorageInfo.getScheme() == Storage.Scheme.minio)
                    {
                        while (!WorkerCommon.getStorage(Storage.Scheme.minio).exists(JoinoutputPath))
                        {
                            // Wait for 10ms and see if the output file is visible.
                            TimeUnit.MILLISECONDS.sleep(10);
                        }
                    }
                }
                workerMetrics.addOutputCostNs(JoinwriteCostTimer.stop());
            } catch (Throwable e)
            {
                throw new WorkerException(
                        "failed to finish writing and close the join result file '" + outputPath + "'", e);
            }

            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            WorkerCommon.setPerfMetrics(joinOutput, workerMetrics);
            // normal partition process
            return joinOutput;
        } catch(Throwable e)
        {
            logger.error("error during join", e);
            joinOutput.setSuccessful(false);
            joinOutput.setErrorMessage(e.getMessage());
            joinOutput.setDurationMs((int) (System.currentTimeMillis() - startTime));
            return joinOutput;
        }
        
    }

    /**
     * Scan and partition the files in a query split.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param scanInputs the information of the files to scan
     * @param columnsToRead the columns to be read from the input files
     * @param inputScheme the storage scheme of the input files
     * @param filter the filer for the scan
     * @param keyColumnIds the ids of the partition key columns
     * @param projection the projection for the partition
     * @param partitionResult the partition result
     * @param writerSchema the schema to be used for the partition result writer
     */
    private void partitionFile(long transId, List<InputInfo> scanInputs,
                            String[] columnsToRead, Storage.Scheme inputScheme,
                            TableScanFilter filter, int[] keyColumnIds, boolean[] projection,
                            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult,
                            AtomicReference<TypeDescription> writerSchema)
    {
        Scanner scanner = null;
        Partitioner partitioner = null;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        for (InputInfo inputInfo : scanInputs)
        {
            readCostTimer.start();
            try (PixelsReader pixelsReader = WorkerCommon.getReader(
                    inputInfo.getPath(), WorkerCommon.getStorage(inputScheme)))
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
                PixelsReaderOption option = WorkerCommon.getReaderOption(transId, columnsToRead, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                TypeDescription rowBatchSchema = recordReader.getResultSchema();
                VectorizedRowBatch rowBatch;

                if (scanner == null)
                {
                    scanner = new Scanner(WorkerCommon.rowBatchSize, rowBatchSchema, columnsToRead, projection, filter);
                }
                if (partitioner == null)
                {
                    partitioner = new Partitioner(partitionResult.size(), WorkerCommon.rowBatchSize,
                            scanner.getOutputSchema(), keyColumnIds);
                }
                if (writerSchema.get() == null)
                {
                    writerSchema.weakCompareAndSet(null, scanner.getOutputSchema());
                }

                computeCostTimer.start();
                do
                {
                    rowBatch = scanner.filterAndProject(recordReader.readBatch(WorkerCommon.rowBatchSize));
                    if (rowBatch.size > 0)
                    {
                        Map<Integer, VectorizedRowBatch> result = partitioner.partition(rowBatch);
                        if (!result.isEmpty())
                        {
                            for (Map.Entry<Integer, VectorizedRowBatch> entry : result.entrySet())
                            {
                                partitionResult.get(entry.getKey()).add(entry.getValue());
                            }
                        }
                    }
                } while (!rowBatch.endOfFile);
                computeCostTimer.stop();
                computeCostTimer.minus(recordReader.getReadTimeNanos());
                readCostTimer.add(recordReader.getReadTimeNanos());
                readBytes += recordReader.getCompletedBytes();
                numReadRequests += recordReader.getNumReadRequests();
            } catch (Throwable e)
            {
                throw new WorkerException("failed to scan the file '" +
                        inputInfo.getPath() + "' and output the partitioning result", e);
            }
        }
        if (partitioner != null)
        {
            VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
            for (int hash = 0; hash < tailBatches.length; ++hash)
            {
                if (!tailBatches[hash].isEmpty())
                {
                    partitionResult.get(hash).add(tailBatches[hash]);
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
    }


    /**
     * Scan the partitioned file of the right table and do the join.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param joiner the joiner for the partitioned join
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param rightScheme the storage scheme of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param joinResult the container of the join result
     * @param workerMetrics the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    public int joinWithRightTable(
            long transId, Joiner joiner, List<String> rightParts, String[] rightCols, Storage.Scheme rightScheme,
            List<Integer> hashValues, int numPartition, ConcurrentLinkedQueue<VectorizedRowBatch> joinResult,
            WorkerMetrics workerMetrics, List<String> localFiles)
    {
        int joinedRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;

        // read one part of the rightpart from the tmp file
        try{

            for(String tmpFileName:localFiles){
                Storage storage = StorageFactory.Instance().getStorage("file");
                PixelsFooterCache footerCache = new PixelsFooterCache();
                PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                                .setStorage(storage)
                                .setPath("/tmp/"+tmpFileName)
                                .setEnableCache(false)
                                .setCacheOrder(ImmutableList.of())
                                .setPixelsCacheReader(null)
                                .setPixelsFooterCache(footerCache);
                PixelsReader tmppixelsReader = builder.build();
                PixelsReaderOption option = WorkerCommon.getReaderOption(transId, rightCols, tmppixelsReader,
                Integer.parseInt(tmpFileName.split("_")[1]), numPartition);
                PixelsRecordReader tmprecordReader = tmppixelsReader.read(option);
                VectorizedRowBatch tmprowBatch;
                // do the join
                do
                {
                    tmprowBatch = tmprecordReader.readBatch(WorkerCommon.rowBatchSize);
                    if (tmprowBatch.size > 0)
                    {
                        List<VectorizedRowBatch> joinedBatches = joiner.join(tmprowBatch);
                        for (VectorizedRowBatch joined : joinedBatches)
                        {
                            if (!joined.isEmpty())
                            {
                                joinResult.add(joined);
                                joinedRows += joined.size;
                            }
                        }
                    }
                } while (!tmprowBatch.endOfFile);
                tmprecordReader.close();
                tmppixelsReader.close();
                rightParts.removeIf(str -> str.contains(tmpFileName));
            }

        } catch (Throwable e)
        {
            throw new WorkerException("failed to scan the file '" +
                    localFiles + "' and output the partitioning result", e);
        }

        // exclude it from the rightParts        
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        rightPartitioned, WorkerCommon.getStorage(rightScheme)))
                {
                    readCostTimer.stop();
                    checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                    Set<Integer> rightHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                    for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                    {
                        rightHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                    }
                    for (int hashValue : hashValues)
                    {
                        if (!rightHashValues.contains(hashValue))
                        {
                            continue;
                        }
                        PixelsReaderOption option = WorkerCommon.getReaderOption(transId, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                                for (VectorizedRowBatch joined : joinedBatches)
                                {
                                    if (!joined.isEmpty())
                                    {
                                        joinResult.add(joined);
                                        joinedRows += joined.size;
                                    }
                                }
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
            if (!rightParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the partitioned files");
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }

    /**
     * Scan the partitioned file of the right table, do the join, and partition the output.
     *
     * @param transId the transaction id used by I/O scheduler
     * @param joiner the joiner for the partitioned join
     * @param rightParts the information of partitioned files of the right table
     * @param rightCols the column names of the right table
     * @param rightScheme the storage scheme of the right table
     * @param hashValues the hash values that are processed by this join worker
     * @param numPartition the total number of partitions
     * @param postPartitionInfo the partition information of post partitioning
     * @param partitionResult the container of the join and post partitioning result
     * @param workerMetrics the collector of the performance metrics
     * @return the number of joined rows produced in this split
     */
    public int joinWithRightTableAndPartition(
            long transId, Joiner joiner, List<String> rightParts, String[] rightCols, Storage.Scheme rightScheme,
            List<Integer> hashValues, int numPartition, PartitionInfo postPartitionInfo,
            List<ConcurrentLinkedQueue<VectorizedRowBatch>> partitionResult, WorkerMetrics workerMetrics, List<String> localFiles)
    {
        requireNonNull(postPartitionInfo, "outputPartitionInfo is null");
        Partitioner partitioner = new Partitioner(postPartitionInfo.getNumPartition(),
                WorkerCommon.rowBatchSize, joiner.getJoinedSchema(), postPartitionInfo.getKeyColumnIds());
        int joinedRows = 0;
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;

        try{
            for(String tmpFileName:localFiles){
                Storage storage = StorageFactory.Instance().getStorage("file");
                PixelsFooterCache footerCache = new PixelsFooterCache();
                PixelsReaderImpl.Builder builder = PixelsReaderImpl.newBuilder()
                                .setStorage(storage)
                                .setPath("/tmp/"+tmpFileName)
                                .setEnableCache(false)
                                .setCacheOrder(ImmutableList.of())
                                .setPixelsCacheReader(null)
                                .setPixelsFooterCache(footerCache);
                PixelsReader tmppixelsReader = builder.build();
                PixelsReaderOption option = WorkerCommon.getReaderOption(transId, rightCols, tmppixelsReader,
                Integer.parseInt(tmpFileName.split("_")[1]), numPartition);
                PixelsRecordReader tmprecordReader = tmppixelsReader.read(option);
                VectorizedRowBatch tmprowBatch;
                // do the join
                do
                {
                    tmprowBatch = tmprecordReader.readBatch(WorkerCommon.rowBatchSize);
                    if (tmprowBatch.size > 0)
                    {
                        List<VectorizedRowBatch> joinedBatches = joiner.join(tmprowBatch);
                        for (VectorizedRowBatch joined : joinedBatches)
                        {
                            if (!joined.isEmpty())
                            {
                                Map<Integer, VectorizedRowBatch> parts = partitioner.partition(joined);
                                for (Map.Entry<Integer, VectorizedRowBatch> entry : parts.entrySet())
                                {
                                    partitionResult.get(entry.getKey()).add(entry.getValue());
                                }
                                joinedRows += joined.size;
                            }
                        }
                    }
                } while (!tmprowBatch.endOfFile);
                tmprecordReader.close();
                tmppixelsReader.close();
                rightParts.removeIf(str -> str.contains(tmpFileName));
            }

        } catch (Throwable e)
        {
            throw new WorkerException("failed to scan local file '" +
                    localFiles + "' and output the partitioning result: ", e);
        }

        // exclude it from the rightParts
        
        while (!rightParts.isEmpty())
        {
            for (Iterator<String> it = rightParts.iterator(); it.hasNext(); )
            {
                String rightPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        rightPartitioned, WorkerCommon.getStorage(rightScheme)))
                {
                    readCostTimer.stop();
                    checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                    Set<Integer> rightHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                    for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                    {
                        rightHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                    }
                    for (int hashValue : hashValues)
                    {
                        if (!rightHashValues.contains(hashValue))
                        {
                            continue;
                        }
                        PixelsReaderOption option = WorkerCommon.getReaderOption(transId, rightCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();
                        do
                        {
                            rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            if (rowBatch.size > 0)
                            {
                                List<VectorizedRowBatch> joinedBatches = joiner.join(rowBatch);
                                for (VectorizedRowBatch joined : joinedBatches)
                                {
                                    if (!joined.isEmpty())
                                    {
                                        Map<Integer, VectorizedRowBatch> parts = partitioner.partition(joined);
                                        for (Map.Entry<Integer, VectorizedRowBatch> entry : parts.entrySet())
                                        {
                                            partitionResult.get(entry.getKey()).add(entry.getValue());
                                        }
                                        joinedRows += joined.size;
                                    }
                                }
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the partitioned file '" +
                            rightPartitioned + "' and do the join", e);
                }
            }
            if (!rightParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the partitioned files");
                }
            }
        }

        VectorizedRowBatch[] tailBatches = partitioner.getRowBatches();
        for (int hash = 0; hash < tailBatches.length; ++hash)
        {
            if (!tailBatches[hash].isEmpty())
            {
                partitionResult.get(hash).add(tailBatches[hash]);
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        return joinedRows;
    }


    protected static void buildHashTable(long transId, Joiner joiner, List<String> leftParts, String[] leftCols,
                                         Storage.Scheme leftScheme, List<Integer> hashValues, int numPartition,
                                         WorkerMetrics workerMetrics)
    {
        WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer();
        WorkerMetrics.Timer computeCostTimer = new WorkerMetrics.Timer();
        long readBytes = 0L;
        int numReadRequests = 0;
        while (!leftParts.isEmpty())
        {
            for (Iterator<String> it = leftParts.iterator(); it.hasNext(); )
            {
                String leftPartitioned = it.next();
                readCostTimer.start();
                try (PixelsReader pixelsReader = WorkerCommon.getReader(
                        leftPartitioned, WorkerCommon.getStorage(leftScheme)))
                {
                    readCostTimer.stop();
                    checkArgument(pixelsReader.isPartitioned(), "pixels file is not partitioned");
                    Set<Integer> leftHashValues = new HashSet<>(pixelsReader.getRowGroupNum());
                    for (PixelsProto.RowGroupInformation rgInfo : pixelsReader.getRowGroupInfos())
                    {
                        leftHashValues.add(rgInfo.getPartitionInfo().getHashValue());
                    }
                    for (int hashValue : hashValues)
                    {
                        if (!leftHashValues.contains(hashValue))
                        {
                            continue;
                        }
                        PixelsReaderOption option = WorkerCommon.getReaderOption(transId, leftCols, pixelsReader,
                                hashValue, numPartition);
                        VectorizedRowBatch rowBatch;
                        PixelsRecordReader recordReader = pixelsReader.read(option);
                        checkArgument(recordReader.isValid(), "failed to get record reader");

                        computeCostTimer.start();

                        do
                        {   
                            // System.out.printf("reading hash value %d\n", hashValue);
                            rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                            
                            if (rowBatch.size > 0)
                            {
                                joiner.populateLeftTable(rowBatch);
                            }
                        } while (!rowBatch.endOfFile);
                        computeCostTimer.stop();
                        computeCostTimer.minus(recordReader.getReadTimeNanos());
                        readCostTimer.add(recordReader.getReadTimeNanos());
                        readBytes += recordReader.getCompletedBytes();
                        numReadRequests += recordReader.getNumReadRequests();
                    }
                    it.remove();
                } catch (Throwable e)
                {
                    if (e instanceof IOException)
                    {
                        continue;
                    }
                    throw new WorkerException("failed to scan the partitioned file '" +
                            leftPartitioned + "' and build the hash table", e);
                }
            }
            if (!leftParts.isEmpty())
            {
                try
                {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e)
                {
                    throw new WorkerException("interrupted while waiting for the partitioned files");
                }
            }
        }
        workerMetrics.addReadBytes(readBytes);
        workerMetrics.addNumReadRequests(numReadRequests);
        workerMetrics.addComputeCostNs(computeCostTimer.getElapsedNs());
        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
    }

}
