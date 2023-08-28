package io.pixelsdb.pixels.worker.common;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.IOException;
import io.pixelsdb.pixels.core.vector.VectorizedRowBatch;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.reader.PixelsReaderOption;
import io.pixelsdb.pixels.core.reader.PixelsRecordReader;
import io.pixelsdb.pixels.core.PixelsReader;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.logging.log4j.Logger;


class BatchToQueue implements Callable{
        private long queryId;
        private String[] includeCols;
        private LinkedBlockingQueue<VectorizedRowBatch> blockingQueue;
        private LinkedBlockingQueue<InputInfo> inputInfoQueue;
        private Storage.Scheme inputScheme;
        private TypeDescription rowBatchSchema;
        private boolean isLatch=false;
        private CountDownLatch schemalatch;
        private CountDownLatch producerlatch;
        private WorkerMetrics workerMetrics;
        private long readBytes = 0L;
        private int numReadRequests = 0;
        // private List<InputSplit> inputSplits;
    
        public BatchToQueue(long queryId, String[] includeCols, LinkedBlockingQueue<VectorizedRowBatch> blockingque,LinkedBlockingQueue<InputInfo> inputInfoQueue,CountDownLatch producerlatch,WorkerMetrics workerMetrics){
            this.queryId=queryId;
            this.includeCols=includeCols;
            this.blockingQueue=blockingque;
            // this.schemalatch=latch;
            this.producerlatch=producerlatch;
            this.inputInfoQueue=inputInfoQueue;
            this.inputScheme=Storage.Scheme.s3;
            this.workerMetrics=workerMetrics;
        }

        public BatchToQueue(long queryId, String[] includeCols, LinkedBlockingQueue<VectorizedRowBatch> blockingque,LinkedBlockingQueue<InputInfo> inputInfoQueue,CountDownLatch producerlatch,CountDownLatch latch,boolean isLatch,WorkerMetrics workerMetrics){
            this.queryId=queryId;
            this.includeCols=includeCols;
            this.blockingQueue=blockingque;
            this.inputInfoQueue=inputInfoQueue;
            this.schemalatch=latch;
            this.producerlatch=producerlatch;
            this.isLatch=isLatch;
            this.inputScheme=Storage.Scheme.s3;
            this.workerMetrics=workerMetrics;
        }
    
        @Override
        public Object call() throws IOException{
            WorkerMetrics.Timer readCostTimer = new WorkerMetrics.Timer().start();
            while(true){
                try{
                    if(inputInfoQueue.isEmpty()){
                        System.out.println("inputInfoQueue is empty");
                        producerlatch.countDown();
                        workerMetrics.addReadBytes(readBytes);
                        workerMetrics.addInputCostNs(readCostTimer.getElapsedNs());
                        workerMetrics.addNumReadRequests(numReadRequests);
                        workerMetrics.addOutputCostNs(readCostTimer.stop());
                        return true;
                    }
                    // WorkerCommon.initStorage(
                    
                    InputInfo inputInfo=inputInfoQueue.take();
                    System.out.println(Thread.currentThread().getName()+":::"+inputInfo.getPath());
                    PixelsReader pixelsReader = WorkerCommon.getReader(inputInfo.getPath(), WorkerCommon.getStorage(inputScheme));
                    
                    if (inputInfo.getRgStart() >= pixelsReader.getRowGroupNum())
                    {   
                        producerlatch.countDown();
                        workerMetrics.addReadBytes(readBytes);
                        workerMetrics.addNumReadRequests(numReadRequests);
                        workerMetrics.addOutputCostNs(readCostTimer.stop());
                        return true;
                    }
                    if (inputInfo.getRgStart() + inputInfo.getRgLength() >= pixelsReader.getRowGroupNum())
                    {
                        
                        inputInfo.setRgLength(pixelsReader.getRowGroupNum() - inputInfo.getRgStart());
                    }
                 
                    
                    PixelsReaderOption option = WorkerCommon.getReaderOption(queryId, includeCols, inputInfo);
                    PixelsRecordReader recordReader = pixelsReader.read(option);
                    this.rowBatchSchema = recordReader.getResultSchema();
                    System.out.println("before enter the latch");
                    if(isLatch){
                        System.out.println("latch is true");
                        if(schemalatch.getCount()>0){
                            System.out.println("latch coundt down");
                            schemalatch.countDown();
                        }
                    }
                    VectorizedRowBatch rowBatch=null;
                    // TODO: issue, if "rgStart": is not start from 0;
                    while(true){ 
                        rowBatch = recordReader.readBatch(WorkerCommon.rowBatchSize);
                        // System.out.println("read first batch success");
                        if(rowBatch.endOfFile){
                            blockingQueue.put(rowBatch);
                            break;
                        }
                        if(rowBatch.isEmpty()){
                            break;
                        }
                        blockingQueue.put(rowBatch);
                    }
                    readBytes += recordReader.getCompletedBytes();
                    numReadRequests += recordReader.getNumReadRequests();

                    readCostTimer.add(recordReader.getReadTimeNanos());

                }catch (Exception e){
                    throw new WorkerException("error in producer", e);
                }
            }
        
        }


        public TypeDescription getRowBatchSchema(){
            return rowBatchSchema;
        }
}


    

