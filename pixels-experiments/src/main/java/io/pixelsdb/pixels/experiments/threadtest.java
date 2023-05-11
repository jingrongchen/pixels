
package io.pixelsdb.pixels.experiments;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

public class threadtest {
    private int id;

    threadtest(){}

    threadtest(int passid){
        this.id=passid;
    }

    public void testmethod(){
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        
        for(int a=0; a<10; a++){
            threadPool.execute(() -> {
                this.id++;
                System.out.println("thread add id : "+ this.id);
            });
        }
        threadPool.shutdown();
    }

    public int getid(){
        return this.id;
    }

}
