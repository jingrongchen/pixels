package io.pixelsdb.pixels.experiments;

import org.junit.Test;

public class TestThreadChangeId {
    
    @Test
    public void testthread(){
        threadtest test=new threadtest(0);
        test.testmethod();
        
        System.out.println(test.getid());


    }





}

