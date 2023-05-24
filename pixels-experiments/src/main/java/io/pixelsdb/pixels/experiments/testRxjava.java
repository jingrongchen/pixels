
package io.pixelsdb.pixels.experiments;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableEmitter;
import io.reactivex.rxjava3.core.FlowableOnSubscribe;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.TimeUnit;
import io.reactivex.rxjava3.core.BackpressureStrategy;

public class testRxjava {

    public static void main(String[] args) throws InterruptedException {

        Flowable<Integer> flowable = Flowable.create(new FlowableOnSubscribe<Integer>() {
            @Override
            public void subscribe(FlowableEmitter<Integer> emitter) throws Exception {
                int count = 0;
                while (!emitter.isCancelled()) {
                    emitter.onNext(count++);
                    Thread.sleep(1000);
                }
                emitter.onComplete();
            }
        }, BackpressureStrategy.BUFFER)
            .subscribeOn(Schedulers.computation());

        flowable.subscribe(System.out::println);

        Thread.sleep(5000);
    }
}
