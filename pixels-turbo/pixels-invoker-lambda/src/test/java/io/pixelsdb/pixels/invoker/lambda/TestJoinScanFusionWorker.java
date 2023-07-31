/*
 * Copyright 2022 PixelsDB.
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
package io.pixelsdb.pixels.invoker.lambda;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Joiner;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedChainJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;

import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalAggregate;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalFilter;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalProject;
import io.pixelsdb.pixels.planner.plan.logical.operation.LogicalFilter.operation;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;


/**
 * 
 * @author Jingrong
 * @create 2022-05-14
 */

public class TestJoinScanFusionWorker {

    @Test
    public void brocastjoinPartitionScan() throws ExecutionException, InterruptedException{
        String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
        String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

        JoinScanFusionInput joinScanInput = new JoinScanFusionInput();

        BroadcastTableInfo customer = new BroadcastTableInfo();
        customer.setColumnsToRead(new String[]{"c_name", "c_custkey"});
        customer.setKeyColumnIds(new int[]{1});
        customer.setTableName("customer");
        customer.setBase(true);
        customer.setInputSplits(Arrays.asList(
            new InputSplit(Arrays.asList(new InputInfo("jingrong-test/tpch/customer/v-0-order/20230425092143_0.pxl", 0, -1)))));
        customer.setFilter(customerFilter);
        customer.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
        joinScanInput.setSmallTable(customer);

        BroadcastTableInfo orders = new BroadcastTableInfo();
        orders.setColumnsToRead(new String[]{"o_orderkey", "o_orderdate","o_totalprice","o_custkey"});
        orders.setKeyColumnIds(new int[]{3});
        orders.setTableName("orders");
        orders.setBase(true);
        orders.setInputSplits(Arrays.asList(
            new InputSplit(Arrays.asList(new InputInfo("jingrong-test/tpch/orders/v-0-order/20230425100657_1.pxl", 0, -1)))));
        orders.setFilter(ordersFilter);
        orders.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        joinScanInput.setLargeTable(orders);

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setSmallProjection(new boolean[]{true, true});
        joinInfo.setLargeProjection(new boolean[]{true, true, true, true});
        joinInfo.setSmallColumnAlias(new String[]{"c_name", "c_custkey"});
        joinInfo.setLargeColumnAlias(new String[]{"o_orderkey", "o_orderdate","o_totalprice"});
        PartitionInfo postPartitionInfo = new PartitionInfo();
        postPartitionInfo.setKeyColumnIds(new int[]{2});
        postPartitionInfo.setNumPartition(20);
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(postPartitionInfo);
        joinScanInput.setJoinInfo(joinInfo);

        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]
                {"l_orderkey", "l_quantity"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
            "s3://jingrong-lambda-test/unit_tests/lineitem_part_0",
            "s3://jingrong-lambda-test/unit_tests/lineitem_part_1"));
        rightTableInfo.setParallelism(2);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
        // joinScanInput.setPartitionlargeTable(rightTableInfo);

        ScanPipeInfo scanPipeInfo = new ScanPipeInfo();
        scanPipeInfo.setRootTableName("lineitem");
        // 1.project
        LogicalProject logicalProject = new LogicalProject(new String[]{"l_orderkey","l_orderkey"}, new int[]{0,4});
        scanPipeInfo.addOperation(logicalProject);
        // 2.aggregate
        LogicalAggregate logicalAggregate = new LogicalAggregate("SUM", "DECIMAL", new int[]{0}, new int[]{1});
        scanPipeInfo.addOperation(logicalAggregate);
        // joinScanInput.setScanPipelineInfo(scanPipeInfo);

        //set fusionOutput 
        MultiOutputInfo fusionOutput = new MultiOutputInfo();
        fusionOutput.setPath("jingrong-test/scanoutput");
        fusionOutput.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
        fusionOutput.setEncoding(false);
        fusionOutput.setFileNames(new ArrayList<String>(Arrays.asList("jingrong-test/partitionoutput1","jingrong-test/partitionoutput2")));

        joinScanInput.setFusionOutput(fusionOutput);
        

        // System.out.println(JSON.toJSONString(joinScanInput));
        WorkerMetrics workerMetrics = new WorkerMetrics();
        Logger logger = LogManager.getLogger(TestJoinScanFusionWorker.class);
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
        BaseJoinScanFusionWorker baseWorker = new BaseJoinScanFusionWorker(workerContext);
        FusionOutput result =(FusionOutput) baseWorker.process(joinScanInput);
        // System.out.println(JSON.toJSONString(result));


    }


    /*
     * Test the chain join and scan fusion worker.
     * (Customer join Orders join Lineitem join (lineitem filter) ) filter
     * brocast join -> partitioned join -> broca
     */
    @Test
    public void chainJoinAndScan() throws ExecutionException, InterruptedException
    {   
        String customerFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"customer\",\"columnFilters\":{}}";
        String ordersFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"orders\",\"columnFilters\":{}}";
        String lineitemFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{}}";

        PartitionedChainJoinInput joinInput = new PartitionedChainJoinInput();
        joinInput.setTransId(123456);

        List<BroadcastTableInfo> chainTables = new ArrayList<>();
        List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();

        BroadcastTableInfo customer = new BroadcastTableInfo();
        customer.setColumnsToRead(new String[]{"c_name", "c_custkey"});
        customer.setKeyColumnIds(new int[]{1});
        customer.setTableName("customer");
        customer.setBase(true);
        customer.setInputSplits(Arrays.asList(
            new InputSplit(Arrays.asList(new InputInfo("jingrong-test/tpch/customer/v-0-order/20230425092143_0.pxl", 0, -1)))));
        customer.setFilter(customerFilter);
        customer.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
        chainTables.add(customer);


        BroadcastTableInfo orders = new BroadcastTableInfo();
        orders.setColumnsToRead(new String[]{"o_orderkey", "o_orderdate","o_totalprice","o_custkey"});
        orders.setKeyColumnIds(new int[]{3});
        orders.setTableName("orders");
        orders.setBase(true);
        orders.setInputSplits(Arrays.asList(
            new InputSplit(Arrays.asList(new InputInfo("jingrong-test/tpch/orders/v-0-order/20230425100657_1.pxl", 0, -1)))));
        orders.setFilter(ordersFilter);
        orders.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        chainTables.add(orders);

        PartitionInfo postPartitionInfo = new PartitionInfo();
        postPartitionInfo.setKeyColumnIds(new int[]{2});
        postPartitionInfo.setNumPartition(20);

        ChainJoinInfo chainJoinInfo0 = new ChainJoinInfo();
        chainJoinInfo0.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo0.setSmallProjection(new boolean[]{true, true});
        chainJoinInfo0.setLargeProjection(new boolean[]{true, true, true, true});
        chainJoinInfo0.setPostPartition(true);
        chainJoinInfo0.setPostPartitionInfo(postPartitionInfo);
        chainJoinInfo0.setSmallColumnAlias(new String[]{"c_name", "c_custkey"});
        chainJoinInfo0.setLargeColumnAlias(new String[]{"o_orderkey", "o_orderdate","o_totalprice"});
        chainJoinInfo0.setKeyColumnIds(new int[]{1});
        chainJoinInfos.add(chainJoinInfo0);

        Set<Integer> hashValues = new HashSet<>(40);
        for (int i = 0 ; i < 40; ++i)
        {
            hashValues.add(i);
        }
        //left small 
        PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
        //what is the correct table name?
        leftTableInfo.setTableName("customer_orders_join");
        leftTableInfo.setColumnsToRead(new String[]
                {"c_name", "c_custkey", "o_orderkey", "o_orderdate","o_totalprice"});
        leftTableInfo.setKeyColumnIds(new int[]{1});

        //the data should be in memory?
        leftTableInfo.setInputFiles(Arrays.asList(
                "jingrong-test/tpch/customer_orders_join/v-0-order/20230425092344_47.pxl",
                "jingrong-test/tpch/customer_orders_join/v-0-order/20230425092347_48.pxl"));
        leftTableInfo.setParallelism(2);
        leftTableInfo.setBase(false);
        leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
        joinInput.setSmallTable(leftTableInfo);
        
        //right bigger line item
        PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
        rightTableInfo.setTableName("lineitem");
        rightTableInfo.setColumnsToRead(new String[]
                {"l_orderkey", "l_quantity"});
        rightTableInfo.setKeyColumnIds(new int[]{0});
        rightTableInfo.setInputFiles(Arrays.asList(
                "jingrong-test/tpch/lineitem/v-0-order/20230425092344_47.pxl",
                "jingrong-test/tpch/lineitem/v-0-order/20230425092347_48.pxl"));
        rightTableInfo.setParallelism(2);
        rightTableInfo.setBase(false);
        rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
        joinInput.setLargeTable(rightTableInfo);


        ScanPipeInfo scanPipeInfo = new ScanPipeInfo();
        //1.project
        LogicalProject logicalProject = new LogicalProject(new String[]{"l_orderkey","l_orderkey"}, new int[]{0,4});
        scanPipeInfo.addOperation(logicalProject);
        //2.aggregate
        LogicalAggregate logicalAggregate = new LogicalAggregate("SUM", "DECIMAL", new int[]{0}, new int[]{1});
        scanPipeInfo.addOperation(logicalAggregate);
        //3.filter
        LogicalFilter logicalFilter = new LogicalFilter(">", "GREATER_THAN");
        logicalFilter.addOperation(new int[]{1}, ">", "GREATER_THAN", 314, "INTEGER");
        scanPipeInfo.addOperation(logicalFilter);
        //4.project
        LogicalProject logicalProject2 = new LogicalProject(new String[]{"l_orderkey"}, new int[]{0});
        scanPipeInfo.addOperation(logicalProject2);
        //5.aggregate

        //this is a empty aggregate generate by calcite, no sure what does it do.
        LogicalAggregate logicalAggregate2 = new LogicalAggregate("null", "null", new int[]{0}, null);
        scanPipeInfo.addOperation(logicalAggregate2);

        for (Object obj : scanPipeInfo.getObjectList())
        {
            System.out.println(obj.toString());
        }

        
        BroadcastTableInfo lineite_aft_pipleine = new BroadcastTableInfo();
        lineite_aft_pipleine.setColumnsToRead(new String[]{"l_orderkey"});
        lineite_aft_pipleine.setKeyColumnIds(new int[]{0});
        lineite_aft_pipleine.setTableName("lineitem_after_pipeline");
        lineite_aft_pipleine.setBase(true);
        // what is the correct input file?

        // should be in memeory
        lineite_aft_pipleine.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/tpch/lineitem/v-0-order/20230425092344_47.pxl", 0, -1)))));
        //TODO: set the pipeline here?

        lineite_aft_pipleine.setFilter(lineitemFilter);
        lineite_aft_pipleine.setStorageInfo(new StorageInfo(Storage.Scheme.s3,null, null, null, null));
        chainTables.add(lineite_aft_pipleine);



        // change the order of the chain join infos
        ChainJoinInfo chainJoinInfo1 = new ChainJoinInfo();
        chainJoinInfo1.setJoinType(JoinType.EQUI_INNER);
        chainJoinInfo1.setSmallProjection(new boolean[]{true, true});
        chainJoinInfo1.setLargeProjection(new boolean[]{true, true, true, true});
        chainJoinInfo1.setPostPartition(true);
        chainJoinInfo1.setPostPartitionInfo(postPartitionInfo);
        chainJoinInfo1.setSmallColumnAlias(new String[]{"c_name", "c_custkey"});
        chainJoinInfo1.setLargeColumnAlias(new String[]{"o_orderkey", "o_orderdate","o_totalprice"});
        chainJoinInfo1.setKeyColumnIds(new int[]{1});
        chainJoinInfos.add(chainJoinInfo1);
        

        

    }



}
