package io.pixelsdb.pixels.experiments;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.junit.Test;
import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;



import java.util.ArrayList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class TestScanfilter {
    @Test
    public void scanExapmle() throws ExecutionException, InterruptedException
    {   


        String filter1 =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                        "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
                        "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                        "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                        "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                        "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                        "\\\"discreteValues\\\":[]}\"}}}";



        List<InputSplit> myList = new ArrayList<InputSplit>();
        try {
                List<String> allLines = Files.readAllLines(Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-urls.txt"));

                for (String line : allLines) {
                        InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                        myList.add(temp);
                }
        } catch (IOException e) {
                e.printStackTrace();
        }

        ScanInput scaninput = new ScanInput();
        scaninput.setTransId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");

        tableInfo.setInputSplits(myList);
        tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filter1);
        tableInfo.setBase(true);
        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
        scaninput.setTableInfo(tableInfo);
        scaninput.setScanProjection(new boolean[]{true, true, true, true});

        scaninput.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/test_scan1", true,
                new StorageInfo(Storage.Scheme.s3, null, null, null), true));
        
        ScanOutput output = (ScanOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.SCAN).invoke(scaninput).get();    
        
        System.out.println(output.getDurationMs());
        

        // filter2 
        // String filter2 =
        // "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
        //         "\"columnFilters\":{1:{\"columnName\":\"o_orderkey\",\"columnType\":\"LONG\"," +
        //         "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
        //         "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
        //         "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
        //         "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
        //         "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
        //         "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
        //         "\\\"discreteValues\\\":[]}\"}}}";
        // ScanInput scaninput2 = new ScanInput();
        // scaninput2.setQueryId(678910);
        // ScanTableInfo tableInfo2 = new ScanTableInfo();
        // tableInfo2.setTableName("orders");

        // tableInfo2.setInputSplits(myList);
        // tableInfo2.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        // tableInfo2.setFilter(filter2);
        // tableInfo2.setBase(true);
        // tableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
        // scaninput2.setTableInfo(tableInfo2);
        // scaninput2.setScanProjection(new boolean[]{true, true, true, true});

        // scaninput2.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/test_scan2", true,
        //         new StorageInfo(Storage.Scheme.s3, null, null, null), true));

        // System.out.println(JSON.toJSONString(scaninput2));
        // ScanOutput output2 = (ScanOutput) InvokerFactory.Instance()
        //         .getInvoker(WorkerType.SCAN).invoke(scaninput2).get();    

        // System.out.println(output2.getDurationMs());
    }

    @Test
    public void scanExapmleOptimized() throws ExecutionException, InterruptedException{
        String filter1 =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                        "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
                        "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                        "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                        "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                        "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                        "\\\"discreteValues\\\":[]}\"}}}";
        String filter2 =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                        "\"columnFilters\":{1:{\"columnName\":\"o_orderkey\",\"columnType\":\"LONG\"," +
                        "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                        "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                        "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                        "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                        "\\\"discreteValues\\\":[]}\"}}}";
        List<String> filterlist=Arrays.asList(filter1,filter2);

        ThreadScanInput scaninput = new ThreadScanInput();
        scaninput.setTransId(123456);
        ThreadScanTableInfo tableInfo = new ThreadScanTableInfo();
        tableInfo.setTableName("orders");

        List<InputSplit> myList = new ArrayList<InputSplit>();
        try {
                List<String> allLines = Files.readAllLines(Paths.get("/home/ubuntu/opt/pixels/pixels-experiments/orders-urls.txt"));

                for (String line : allLines) {
                        InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                        myList.add(temp);
                }
        } catch (IOException e) {
                e.printStackTrace();
        }
        tableInfo.setInputSplits(myList);
        

        // tableInfo.setInputSplits(Arrays.asList(
        //         new InputSplit(Arrays.asList(new InputInfo("jingrong-test/orders/v-0-order/20230425100700_2.pxl", 0, -1)))));

        tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filterlist);
        tableInfo.setBase(true);
        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
        scaninput.setTableInfo(tableInfo);
        // scaninput.setScanProjection(new boolean[]{true, true, true, true});
        
        List<String> list=new ArrayList<String>();
        list.add("s3://jingrong-lambda-test/unit_tests/test_scan1");
        list.add("s3://jingrong-lambda-test/unit_tests/test_scan2");
        ThreadOutputInfo threadoutput = new ThreadOutputInfo(list, true,
         new StorageInfo(Storage.Scheme.s3, null, null, null), true);
        
        scaninput.setOutput(threadoutput);
                
        System.out.println(JSON.toJSONString(scaninput));
        // ScanOutput output = (ScanOutput) InvokerFactory.Instance()
        //         .getInvoker(WorkerType.SCAN).invoke(scaninput).get();    
        
        // System.out.println(output.getDurationMs());
        

        // filter2 
        // String filter2 =
        // "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
        //         "\"columnFilters\":{1:{\"columnName\":\"o_orderkey\",\"columnType\":\"LONG\"," +
        //         "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
        //         "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
        //         "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
        //         "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
        //         "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
        //         "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
        //         "\\\"discreteValues\\\":[]}\"}}}";
        // ScanInput scaninput2 = new ScanInput();
        // scaninput2.setQueryId(678910);
        // ScanTableInfo tableInfo2 = new ScanTableInfo();
        // tableInfo2.setTableName("orders");

        // tableInfo2.setInputSplits(myList);
        // tableInfo2.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        // tableInfo2.setFilter(filter2);
        // tableInfo2.setBase(true);
        // tableInfo2.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
        // scaninput2.setTableInfo(tableInfo2);
        // scaninput2.setScanProjection(new boolean[]{true, true, true, true});

        // scaninput2.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/test_scan2", true,
        //         new StorageInfo(Storage.Scheme.s3, null, null, null), true));

        // System.out.println(JSON.toJSONString(scaninput2));
        // ScanOutput output2 = (ScanOutput) InvokerFactory.Instance()
        //         .getInvoker(WorkerType.SCAN).invoke(scaninput2).get();    

        // System.out.println(output2.getDurationMs());

        
    }


}
