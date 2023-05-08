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
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;


public class TestScanfilter {
    @Test
    public void scanExapmle() throws ExecutionException, InterruptedException
    {   

        String filter =
                "{\"schemaName\":\"tpch\",\"tableName\":\"orders\"," +
                        "\"columnFilters\":{1:{\"columnName\":\"o_custkey\",\"columnType\":\"LONG\"," +
                        "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                        "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"ranges\\\":[{" +
                        "\\\"lowerBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":100}}," +
                        "{\\\"lowerBound\\\":{\\\"type\\\":\\\"EXCLUDED\\\",\\\"value\\\":200}," +
                        "\\\"upperBound\\\":{\\\"type\\\":\\\"UNBOUNDED\\\"}}]," +
                        "\\\"discreteValues\\\":[]}\"}}}";
        ScanInput scaninput = new ScanInput();
        scaninput.setQueryId(123456);
        ScanTableInfo tableInfo = new ScanTableInfo();
        tableInfo.setTableName("orders");
        tableInfo.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/orders/v-0-order/20230425100657_1.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/orders/v-0-order/20230425100657_1.pxl", 4, 4)))));
        tableInfo.setColumnsToRead(new String[]{"o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"});
        tableInfo.setFilter(filter);
        tableInfo.setBase(true);
        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null));
        scaninput.setTableInfo(tableInfo);
        scaninput.setScanProjection(new boolean[]{true, true, true, true});

        scaninput.setOutput(new OutputInfo("s3://jingrong-lambda-test/unit_tests/orders_part_1", true,
                new StorageInfo(Storage.Scheme.s3, null, null, null), true));

        System.out.println(JSON.toJSONString(scaninput));

        ScanOutput output = (ScanOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.SCAN).invoke(scaninput).get();    
        
        System.out.println(output.getDurationMs());
    
    }



}
