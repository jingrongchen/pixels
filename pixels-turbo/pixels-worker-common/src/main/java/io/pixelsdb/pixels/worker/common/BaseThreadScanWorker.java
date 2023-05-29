package io.pixelsdb.pixels.worker.common;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONValidator.Type;

import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.Input;
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

import org.checkerframework.checker.units.qual.C;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.concurrent.*; 
import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;



// publisher,subscriber
import io.reactivex.rxjava3.processors.PublishProcessor;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;
import io.reactivex.rxjava3.core.FlowableOnSubscribe;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import software.amazon.awssdk.annotations.ToBuilderIgnoreField;
import software.amazon.awssdk.core.endpointdiscovery.providers.SystemPropertiesEndpointDiscoveryProvider;


import java.util.concurrent.LinkedBlockingQueue;

import java.util.UUID;
import io.reactivex.rxjava3.core.FlowableSubscriber;
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
            // ExecutorService threadPool = Executors.newFixedThreadPool(cores * 2);
            String requestId = context.getRequestId();

            long queryId = event.getTransId();
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
            HashMap<String, List<Integer>> filterOnAggreation=event.getFilterOnAggreation();

            WorkerCommon.initStorage(inputStorageInfo);
            WorkerCommon.initStorage(outputStorageInfo);

            String[] includeCols = event.getTableInfo().getColumnsToRead();
            List<String> filterlist=event.getTableInfo().getFilter();
            List<TableScanFilter> scanfilterlist=new ArrayList<TableScanFilter>();

            /*start preparing batches */
            int producerPoolSize=2;
            ExecutorService producerPool = Executors.newFixedThreadPool(producerPoolSize);
            LinkedBlockingQueue<VectorizedRowBatch> blockingQueue = new LinkedBlockingQueue<>();
            LinkedBlockingQueue<InputInfo> inputInfoQueue = new LinkedBlockingQueue<>();
            
            for (InputSplit inputSplit : inputSplits)
            {
                List<InputInfo> scanInputs = inputSplit.getInputInfos();
                for (InputInfo inputInfo : scanInputs){
                    inputInfoQueue.put(inputInfo);
                }
            }
            int EOFsize=inputInfoQueue.size();

            long peektime1 = System.currentTimeMillis();
            InputInfo inputInfoaa=inputInfoQueue.peek();
            long endpeektime1 = System.currentTimeMillis();

            Storage.Scheme outputschema=event.getOutput().getStorageInfo().getScheme();

            for(int i=0;i<producerPoolSize;i++){
                producerPool.submit(new ThreadScanProducer2(queryId,includeCols,blockingQueue,inputInfoQueue,outputschema));
            }
            /*start preparing batches */

            long startTimeonFilter = System.currentTimeMillis();
            for (String filter:filterlist){
                scanfilterlist.add(JSON.parseObject(filter, TableScanFilter.class));
            }
            long endTimeonFilter = System.currentTimeMillis();
            System.out.println("time wasted in loding the filter ：" + (endTimeonFilter-startTimeonFilter) + " ms");

            List<Aggregator> aggregatorList = new ArrayList<>();
            if (partialAggregationPresent)
            {   
                logger.info("start get output schema");
                TypeDescription inputSchema = WorkerCommon.getFileSchemaFromSplits(
                        WorkerCommon.getStorage(inputStorageInfo.getScheme()), inputSplits);
                inputSchema = WorkerCommon.getResultSchema(inputSchema, includeCols);
                List<PartialAggregationInfo> partialAggregationInfoList = event.getPartialAggregationInfo();
                requireNonNull(partialAggregationInfoList, "event.partialAggregationInfo is null");
                for(int i=0;i<partialAggregationInfoList.size();i++){
                    boolean[] groupKeyProjection = new boolean[partialAggregationInfoList.get(i).getGroupKeyColumnAlias().length];
                    Arrays.fill(groupKeyProjection, true);
                    Aggregator aggregator = new Aggregator(WorkerCommon.rowBatchSize, inputSchema,
                    partialAggregationInfoList.get(i).getGroupKeyColumnAlias(),
                    partialAggregationInfoList.get(i).getGroupKeyColumnIds(), groupKeyProjection,
                    partialAggregationInfoList.get(i).getAggregateColumnIds(),
                    partialAggregationInfoList.get(i).getResultColumnAlias(),
                    partialAggregationInfoList.get(i).getResultColumnTypes(),
                    partialAggregationInfoList.get(i).getFunctionTypes(),
                    partialAggregationInfoList.get(i).isPartition(),
                    partialAggregationInfoList.get(i).getNumPartition());

                    aggregatorList.add(aggregator);
                }                
            }
            else
            {
                aggregatorList = null;
            }
            logger.info("start scan and aggregate");
            logger.info("start threadversion.start threadversion.start threadversion");
            
            // All the batches will put into the blocingqueue, and all the consumer, consumer the data from this queue, 
            // producershould be 
            // 1. Put **VectorizedRowBatch** to the queue, nothing else
            // 2. Each thread do, read each inputinofo and put batches
            // 3. Should use take() from a blocking queue?
            // 4. Each thread should read each inputeinfo and produce vbatch
            // 有一个inputeinfo linkedqueue 来存信息，不会很大，for循环就好
            // 每次都从这个queue里面取来放进


            // just for the schema!!!!!!
            long starttime1 = System.currentTimeMillis();
            PixelsReader pixelsReader = WorkerCommon.getReader(inputInfoaa.getPath(), WorkerCommon.getStorage(inputStorageInfo.getScheme()));
            PixelsReaderOption option = WorkerCommon.getReaderOption(queryId, includeCols,inputInfoaa );
            PixelsRecordReader recordReader = pixelsReader.read(option);
            TypeDescription rowBatchSchema = recordReader.getResultSchema();
            long endTime1 = System.currentTimeMillis();
            System.out.println("time wasted in get the schema ：" + (endTime1-starttime1)+(endpeektime1-peektime1) + " ms");
            // just for the schema!!!!!!        


            // GroupProcessor gourpProcessor=new GroupProcessor(blockingQueue,scanfilterlist,outputFolders,rowBatchSchema,includeCols,
            // scanProjection,encoding,outputStorageInfo.getScheme(), requestId,partialAggregationPresent,aggregatorList,filterOnAggreation,latch,EOFsize);
            
            
            long starTime = System.currentTimeMillis();
            int consumerPoolSize=1;

            System.out.println("EOFsize is : "+ EOFsize);
            CountDownLatch latch=new CountDownLatch(EOFsize*2*consumerPoolSize);
            CountDownLatch triggerlatch=new CountDownLatch(consumerPoolSize*2);

            ExecutorService consumerPool = Executors.newFixedThreadPool(consumerPoolSize);
            // for(int i=0;i<consumerPoolSize;i++){
            //     consumerPool.submit(new GroupProcessor(blockingQueue,scanfilterlist,outputFolders,rowBatchSchema,includeCols,
            //     scanProjection,encoding,outputStorageInfo.getScheme(), requestId,partialAggregationPresent,aggregatorList,filterOnAggreation,latch,EOFsize));
            // }

            GroupProcessor groupProcessor=new GroupProcessor(blockingQueue,scanfilterlist,outputFolders,rowBatchSchema,includeCols,
            scanProjection,encoding,outputStorageInfo.getScheme(), requestId,partialAggregationPresent,aggregatorList,filterOnAggreation,latch,EOFsize,triggerlatch);
            


            consumerPool.submit(groupProcessor);
            // gourpProcessor.start();            
            latch.await();
            groupProcessor.trigger();
            triggerlatch.await();


            long endTime = System.currentTimeMillis();
            long totalTime = endTime -starTime;
            System.out.println("the filter and aggregation cost time ：" + totalTime + " ms");

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
}

class GroupProcessor implements Runnable{
    private LinkedBlockingQueue<VectorizedRowBatch> queue;
    private int writeSize;
    private List<TableScanFilter> scanfilterlist;
    private Flowable<VectorizedRowBatch> publisher;
    private List<String> outputFolders;
    private TypeDescription rowbatchschema;
    private String[] columnstoread;
    private boolean[] scanprojection;
    private boolean encoding;
    private Storage.Scheme outputscheme;
    private String requestId;
    private boolean partialAggregationPresent;
    private List<Aggregator> aggregatorList;
    private HashMap<String, List<Integer>> filterOnAggreation;
    private CountDownLatch latch;
    private int EOFsize;
    private PublishProcessor<Boolean> subject=PublishProcessor.create();
    private CountDownLatch triggerLatch;

    public GroupProcessor(LinkedBlockingQueue<VectorizedRowBatch> queue,List<TableScanFilter> scanfilterlist,List<String> outputFolders,TypeDescription rowbatchschema,String[] columnstoread,
    boolean[] scanprojection,boolean encoding, Storage.Scheme outputscheme,String requestId,boolean partialAggregationPresent,List<Aggregator> aggregatorList,HashMap<String, List<Integer>> filterOnAggreation,
    CountDownLatch latch,int endOfFile,CountDownLatch triggerLatch) {
        this.queue = queue;
        this.writeSize = 37;
        this.scanfilterlist=scanfilterlist;
        this.outputFolders=outputFolders;
        this.rowbatchschema=rowbatchschema;
        this.columnstoread=columnstoread;
        this.scanprojection=scanprojection;
        this.encoding=encoding;
        this.outputscheme=outputscheme;
        this.requestId=requestId;
        this.partialAggregationPresent=partialAggregationPresent;
        this.aggregatorList=aggregatorList;
        this.filterOnAggreation=filterOnAggreation;
        this.latch=latch;
        this.EOFsize=endOfFile;
        this.triggerLatch=triggerLatch;
    }

    //TODO: writesize proplem arise? if file is above 7 or 8, the observer doesn't get enough latch count down to continue.
    @Override
    public void run() {
        publisher = Flowable.create(new FlowableOnSubscribe<VectorizedRowBatch>() {
            @Override
            public void subscribe(FlowableEmitter<VectorizedRowBatch> emitter) throws Exception {
                while (!emitter.isCancelled()) {
                    VectorizedRowBatch message = queue.take();
                    emitter.onNext(message);

                    if(message.endOfFile){
                        System.out.println("take an end of file message from queue");
                    }

                }
                System.out.println("emitter start to do onComplete");
                emitter.onComplete();
                
            }
        }, BackpressureStrategy.BUFFER).onBackpressureBuffer(1024).share();
        // }, BackpressureStrategy.BUFFER).onBackpressureBuffer(1024).subscribeOn(Schedulers.newThread()).share();

        // 创建 Observer 1
        FlowableSubscriber<VectorizedRowBatch> observer1 = new FlowableSubscriber<VectorizedRowBatch>() {
            private int messageCount = 0;
            private PixelsWriter pixelsWriter=null;
            private int fileCount=0;
            private String outputPath;
            private Scanner scanner;
            private TypeDescription outputschema;
            private VectorizedRowBatch rowBatch;
            private Subscription subscription;
            private List<Aggregator> aggregatorOnFilterList=new ArrayList<>(); ;

            @Override
            public void onSubscribe(Subscription d) {
                subscription=d;
                // 不需要处理
                // PixelsWriter pixelsWriter=null;
                Scanner scanner=new Scanner(WorkerCommon.rowBatchSize, rowbatchschema, columnstoread, scanprojection, scanfilterlist.get(0));
                if(partialAggregationPresent){
                    System.out.println("partialAggregationPresent start preparing");
                    UUID uuid = UUID.randomUUID();
                    this.outputPath=outputFolders.get(0) + uuid.toString() + requestId +"_scan1_";
                    List<Integer> aggregationList= filterOnAggreation.get("0"); 
                    for (Integer i : aggregationList) {
                        System.out.println("filter 1 get aggregator: "+ i);
                        this.aggregatorOnFilterList.add(aggregatorList.get(i));
                    } 
                }
                this.outputschema =scanner.getOutputSchema();
                this.scanner=scanner;
                subscription.request(1);
            }

            @Override
            public void onNext(VectorizedRowBatch message) {
                messageCount++;
                if(message.endOfFile){
                    
                    System.out.println("filter 1 receive a endOfFile");
                    System.out.println("filter 1 receive a endOfFile"+Thread.currentThread().getName());
                    // EOFsize--;
                    // if(EOFsize==0){
                    //     emitter.onNext(message);
                    //     break;
                    // }
                }
                rowBatch=scanner.filterAndProject(message);

                if(partialAggregationPresent){
                   
                    for (Aggregator aggregator:aggregatorOnFilterList){
                        aggregator.aggregate(rowBatch);
                    }

                    if (messageCount == writeSize) {
                        for (Aggregator aggregator:aggregatorOnFilterList){
                            String tempPath=outputPath +"aggregation_"+ aggregatorOnFilterList.indexOf(aggregator)+fileCount++;
                            pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                                    WorkerCommon.getStorage(outputscheme), tempPath, encoding,
                                    aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
                            try{
                                aggregator.writeAggrOutput(pixelsWriter);
                                
                                pixelsWriter.close();
                            }catch (Exception e){ 
                                System.out.print("filter 1 count an exception in write size");
                                e.printStackTrace();
                            }
                        }
                        messageCount = 0;
                    }  
                } else {
                    if(pixelsWriter==null){
                        String tempPath=outputPath+fileCount++;
                        pixelsWriter=WorkerCommon.getWriter(outputschema, WorkerCommon.getStorage(outputscheme),
                        tempPath, encoding, false, null);
                    }

                    try{
                        pixelsWriter.addRowBatch(rowBatch);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    if (messageCount == writeSize) {
                        try{
                            pixelsWriter.close();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        pixelsWriter=null;
                        messageCount = 0;
                    }  
                }

                if(message.endOfFile){
                    latch.countDown();
                    System.out.println("filter 1 receive a endOfFile count down latch");
                    // EOFsize--;
                    // if(EOFsize==0){
                    //     emitter.onNext(message);
                    //     break;
                    // }
                }
                subscription.request(1);
            }

            @Override
            public void onError(Throwable e) {
                e.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("filter 1 oncomplete begins.");
                if(partialAggregationPresent){
                    System.out.println("filter 1 on partial complete");
                    for (Aggregator aggregator:aggregatorOnFilterList){
                        String tempPath=outputPath +"aggregation_"+ aggregatorOnFilterList.indexOf(aggregator)+fileCount++;
                        pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                                WorkerCommon.getStorage(outputscheme), tempPath, encoding,
                                aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
                        try{
                            
                            aggregator.writeAggrOutput(pixelsWriter);
                            pixelsWriter.close();
                        }catch (Exception e){ 
                            e.printStackTrace();
                        }
                    }
                } else{
                    try{
                        if(pixelsWriter!=null){
                            pixelsWriter.close();
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                triggerLatch.countDown();
                // latch.countDown();
                // 不需要处理
            }
        };

        // 创建 Observer 2
        FlowableSubscriber<VectorizedRowBatch> observer2 = new FlowableSubscriber<VectorizedRowBatch>() {
            private int messageCount = 0;
            private PixelsWriter pixelsWriter=null;
            private int fileCount=0;
            private String outputPath;
            private Scanner scanner;
            private TypeDescription outputschema;
            private VectorizedRowBatch rowBatch;
            private Subscription subscription;
            private List<Aggregator> aggregatorOnFilterList=new ArrayList<>();;

            @Override
            public void onSubscribe(Subscription d) {
                subscription=d;
                // 不需要处理
                // PixelsWriter pixelsWriter=null;
                Scanner scanner=new Scanner(WorkerCommon.rowBatchSize, rowbatchschema, columnstoread, scanprojection, scanfilterlist.get(0));
                if(partialAggregationPresent){
                    UUID uuid = UUID.randomUUID();
                    this.outputPath=outputFolders.get(1) + uuid.toString() + requestId +"_scan2_";
                    List<Integer> aggregationList= filterOnAggreation.get("1"); 
                    for (Integer i : aggregationList) {
                        System.out.println("filter 2 get aggregator: "+ i);
                        this.aggregatorOnFilterList.add(aggregatorList.get(i));
                    } 
                }
                this.outputschema =scanner.getOutputSchema();
                this.scanner=scanner;
                subscription.request(1);
            }

            @Override
            public void onNext(VectorizedRowBatch message) {
                messageCount++;
                rowBatch=scanner.filterAndProject(message);

                if(partialAggregationPresent){
                   
                    for (Aggregator aggregator:aggregatorOnFilterList){
                        aggregator.aggregate(rowBatch);
                    }

                    if (messageCount == writeSize) {
                        for (Aggregator aggregator:aggregatorOnFilterList){
                            String tempPath=outputPath +"aggregation_"+ aggregatorOnFilterList.indexOf(aggregator)+fileCount++;
                            pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                                    WorkerCommon.getStorage(outputscheme), tempPath, encoding,
                                    aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
                            try{
                                aggregator.writeAggrOutput(pixelsWriter);
                                pixelsWriter.close();
                            }catch (Exception e){ 
                                e.printStackTrace();
                            }
                        }
                        messageCount = 0;
                    }  
                } else {
                    if(pixelsWriter==null){
                        String tempPath=outputPath+fileCount++;
                        pixelsWriter=WorkerCommon.getWriter(outputschema, WorkerCommon.getStorage(outputscheme),
                        tempPath, encoding, false, null);
                    }

                    try{
                        pixelsWriter.addRowBatch(rowBatch);
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    if (messageCount == writeSize) {
                        try{
                            pixelsWriter.close();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        pixelsWriter=null;
                        messageCount = 0;
                    }  
                }

                if(message.endOfFile){
                    latch.countDown();
                    // EOFsize--;
                    // if(EOFsize==0){
                    //     emitter.onNext(message);
                    //     break;
                    // }
                }
                subscription.request(1);
            }

            @Override
            public void onError(Throwable e) {
                e.printStackTrace();
            }

            @Override
            public void onComplete() {
                System.out.println("filter 2 oncomplete begins.");
                if(partialAggregationPresent){
                    System.out.println("filter 2 on partial complete");
                    for (Aggregator aggregator:aggregatorOnFilterList){
                        String tempPath=outputPath +"aggregation_"+ aggregatorOnFilterList.indexOf(aggregator)+fileCount++;
                        pixelsWriter = WorkerCommon.getWriter(aggregator.getOutputSchema(),
                                WorkerCommon.getStorage(outputscheme), tempPath, encoding,
                                aggregator.isPartition(), aggregator.getGroupKeyColumnIdsInResult());
                        try{
                            aggregator.writeAggrOutput(pixelsWriter);
                            pixelsWriter.close();
                        }catch (Exception e){ 
                            e.printStackTrace();
                        }
                    }
                } else{
                    try{
                        if(pixelsWriter!=null){
                            pixelsWriter.close();
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                triggerLatch.countDown();
                // 不需要处理
                // latch.countDown();
            }
        };

        
        // 订阅 Observer 1
        publisher.observeOn(Schedulers.newThread()).subscribe(observer1);

        // 订阅 Observer 2
        publisher.observeOn(Schedulers.newThread()).subscribe(observer2);

        subject.subscribe(ignore -> {
            System.out.println("trigger on complete triggering");
            observer1.onComplete();
            observer2.onComplete();
        });


    }

    public void trigger() {
        subject.onNext(true);
    }

}


class ThreadScanProducer2 implements Callable{
    private long queryId;
    private String[] includeCols;
    private LinkedBlockingQueue<VectorizedRowBatch> blockingQueue;
    private LinkedBlockingQueue<InputInfo> inputInfoQueue;
    private Storage.Scheme inputScheme;

    public ThreadScanProducer2(long queryId,String[] includeCols,LinkedBlockingQueue<VectorizedRowBatch> blockingque,LinkedBlockingQueue<InputInfo> inputInfoQueue, Storage.Scheme inputScheme){
        this.queryId=queryId;
        this.includeCols=includeCols;
        this.blockingQueue=blockingque;
        this.inputInfoQueue=inputInfoQueue;
        this.inputScheme=inputScheme;
    }

    @Override
    public Object call() throws IOException{
        while(true){
            try{
                if(inputInfoQueue.isEmpty()){
                    return true;
                }
                InputInfo inputInfo=inputInfoQueue.poll();
                PixelsReader pixelsReader = WorkerCommon.getReader(inputInfo.getPath(), WorkerCommon.getStorage(inputScheme));
                if (inputInfo.getRgStart() >= pixelsReader.getRowGroupNum())
                {
                    return true;
                }
                if (inputInfo.getRgStart() + inputInfo.getRgLength() >= pixelsReader.getRowGroupNum())
                {
                    inputInfo.setRgLength(pixelsReader.getRowGroupNum() - inputInfo.getRgStart());
                }
                PixelsReaderOption option = WorkerCommon.getReaderOption(queryId, includeCols, inputInfo);
                PixelsRecordReader recordReader = pixelsReader.read(option);
                VectorizedRowBatch rowBatch=null;
                // TODO: issue, if "rgStart": is not start from 0;
                while(true){ 
                    rowBatch=recordReader.readBatch(WorkerCommon.rowBatchSize);
                    if(rowBatch.endOfFile){
                        blockingQueue.put(rowBatch);
                        break;
                    }
                    if(rowBatch.isEmpty()){
                        break;
                    }
                    blockingQueue.put(rowBatch);
                }
            }catch (Exception e){
                throw new WorkerException("error in producer", e);
            }
        }
    }
}
