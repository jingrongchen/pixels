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
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.predicate.Bound;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.BroadcastJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import static io.pixelsdb.pixels.executor.predicate.Bound.Type.INCLUDED;

/**
 * @author hank
 * @create 2022-05-15
 */
public class TestBroadcastJoinLambdaInvoker
{
    @Test
    public void testPartLineitem() throws ExecutionException, InterruptedException
    {
        String leftFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\",\"columnFilters\":{2:{\"columnName\":\"p_size\",\"columnType\":\"INT\",\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false,\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"onlyNull\\\":false,\\\"ranges\\\":[],\\\"discreteValues\\\":[{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":49},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":14},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":23},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":45},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":19},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":3},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":36},{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":9}]}\"}}}";

        String rightFilter = "{\"schemaName\":\"tpch\",\"tableName\":\"part\",\"columnFilters\":{}}";
        

        BroadcastJoinInput joinInput = new BroadcastJoinInput();
        joinInput.setTransId(123456);

        BroadcastTableInfo leftTable = new BroadcastTableInfo();
        leftTable.setColumnsToRead(new String[]{"p_partkey", "p_name", "p_size"});
        leftTable.setKeyColumnIds(new int[]{0});
        leftTable.setTableName("part");
        leftTable.setBase(true);
        leftTable.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/part/v-0-order/20230425102035_236.pxl", 28, 4)))));
        leftTable.setFilter(leftFilter);
        leftTable.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        joinInput.setSmallTable(leftTable);

        BroadcastTableInfo rightTable = new BroadcastTableInfo();
        rightTable.setColumnsToRead(new String[]{"l_orderkey", "l_partkey", "l_extendedprice", "l_discount"});
        rightTable.setKeyColumnIds(new int[]{1});
        rightTable.setTableName("lineitem");
        rightTable.setBase(true);
        rightTable.setInputSplits(Arrays.asList(
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 0, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 4, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 8, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 12, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 16, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 20, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 24, 4))),
                new InputSplit(Arrays.asList(new InputInfo("jingrong-test/lineitem/v-0-order/20230425092344_47.pxl", 28, 4)))));
        rightTable.setFilter(rightFilter);
        rightTable.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null, null, null));
        joinInput.setLargeTable(rightTable);

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setJoinType(JoinType.EQUI_INNER);
        joinInfo.setSmallColumnAlias(new String[]{"p_name", "p_size"});
        joinInfo.setLargeColumnAlias(new String[]{"l_orderkey", "l_extendedprice", "l_discount"});
        joinInfo.setSmallProjection(new boolean[]{false, true, true});
        joinInfo.setLargeProjection(new boolean[]{true, false, true, true});
        joinInfo.setPostPartition(true);
        joinInfo.setPostPartitionInfo(new PartitionInfo(new int[] {2}, 100));
        joinInput.setJoinInfo(joinInfo);
        joinInput.setOutput(new MultiOutputInfo("pixels-lambda-test/unit_tests/",
                new StorageInfo(Storage.Scheme.s3, null, null, null, null), true,
                Arrays.asList("broadcast_join_lineitem_part_0")));

        System.out.println(JSON.toJSONString(joinInput));
        JoinOutput output = (JoinOutput) InvokerFactory.Instance()
                .getInvoker(WorkerType.BROADCAST_JOIN).invoke(joinInput).get();
        System.out.println(output.getOutputs().size());
        System.out.println(Joiner.on(",").join(output.getOutputs()));
        System.out.println(Joiner.on(",").join(output.getRowGroupNums()));
    }

    @Test
    public void testSerFilter()
    {
        ArrayList<Bound<Long>> discreteValues = new ArrayList<>();
        discreteValues.add(new Bound<>(INCLUDED, 49L));
        discreteValues.add(new Bound<>(INCLUDED, 14L));
        discreteValues.add(new Bound<>(INCLUDED, 23L));
        discreteValues.add(new Bound<>(INCLUDED, 45L));
        discreteValues.add(new Bound<>(INCLUDED, 19L));
        discreteValues.add(new Bound<>(INCLUDED, 3L));
        discreteValues.add(new Bound<>(INCLUDED, 36L));
        discreteValues.add(new Bound<>(INCLUDED, 9L));
        ColumnFilter<Long> columnFilter = new ColumnFilter<Long>("p_size", TypeDescription.Category.INT,
                new Filter<>(Long.TYPE, new ArrayList<>(), discreteValues, false, false, false, false));
        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<>();
        columnFilters.put(2, columnFilter);
        TableScanFilter filter = new TableScanFilter("tpch", "lineitem", columnFilters);
        System.out.println(JSON.toJSONString(filter));
    }

    @Test
    public void testDeFilter()
    {
        String filter = "{\"schemaName\":\"tpch\",\"tableName\":\"lineitem\"," +
                "\"columnFilters\":{2:{\"columnName\":\"p_size\",\"columnType\":\"INT\"," +
                "\"filterJson\":\"{\\\"javaType\\\":\\\"long\\\",\\\"isAll\\\":false," +
                "\\\"isNone\\\":false,\\\"allowNull\\\":false,\\\"onlyNull\\\":false," +
                "\\\"ranges\\\":[],\\\"discreteValues\\\":[{" +
                "\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":49}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":14}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":23}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":45}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":19}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":3}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":36}," +
                "{\\\"type\\\":\\\"INCLUDED\\\",\\\"value\\\":9}]}\"}}}";
        TableScanFilter tableScanFilter = JSON.parseObject(filter, TableScanFilter.class);
        assert tableScanFilter.getColumnFilter(2).getFilter().getDiscreteValueCount() == 8;
    }
}
