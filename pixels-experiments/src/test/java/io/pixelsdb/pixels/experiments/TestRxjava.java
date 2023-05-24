package io.pixelsdb.pixels.experiments;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;
import io.reactivex.rxjava3.core.FlowableOnSubscribe;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import io.reactivex.rxjava3.core.BackpressureStrategy;



public class TestRxjava {
    // @Test
    // public void example2() throws ExecutionException, InterruptedException{
    //     Flowable.create(new FlowableOnSubscribe<Integer>() {
    //         @Override
    //         public void subscribe(FlowableEmitter<Integer> emitter) throws Exception {
    //             for (int i = 0; i < 256; i++) {
    //                 emitter.onNext(i);
    //             }
    //         }
    //     }, BackpressureStrategy.MISSING)
    //         .subscribeOn(Schedulers.io())
    //         .observeOn(Schedulers.computation())
    //         .subscribe(new Subscriber<Integer>(){
    //             @Override
    //             public void onSubscribe(Subscription s) {
    //                 Log.d(TAG, "onSubscribe");
    //                 //s.request(64);
    //             }
         
    //             @Override
    //             public void onNext(Integer integer) {
    //                 Log.d(TAG, "onNext: " + integer);
    //             }
         
    //             @Override
    //             public void onError(Throwable t) {
    //                 Log.w(TAG, "onError: ", t);
    //             }
         
    //             @Override
    //             public void onComplete() {
    //                 Log.d(TAG, "onComplete");
    //             }
    //         });

    // } 

    @Test
    public void exampleRxjava() throws ExecutionException, InterruptedException{
        Flowable<Integer> flowable = Flowable.create(new FlowableOnSubscribe<Integer>() {
            @Override
            public void subscribe(FlowableEmitter<Integer> emitter) throws Exception {
                for (int i = 1; i <= 10 && !emitter.isCancelled(); i++) {
                    System.out.println(Thread.currentThread().getName());
                    System.out.println("emmite roabatch");
                    emitter.onNext(i);
                    // rowbatch=recordReader.readBatch(WorkerCommon.rowBatchSize);
                    // if(!rowbatch.isEmpty()){
                    //     System.out.println("emmite roabatch");
                    //     emitter.onNext(rowbatch);
                    // } else{
                    //     emitter.onComplete();
                    // }
                }
                emitter.onComplete();
            }
        }, BackpressureStrategy.BUFFER);
        
        flowable.subscribe(System.out::println);
        Thread.sleep(2000);
    }


}
