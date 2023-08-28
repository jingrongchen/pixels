package io.pixelsdb.pixels.planner;

import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.Operator;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.logical.BaseTable;
import io.pixelsdb.pixels.planner.plan.logical.Join;
import io.pixelsdb.pixels.planner.plan.logical.JoinEndian;
import io.pixelsdb.pixels.planner.plan.logical.JoinedTable;
import org.junit.Test;
import java.util.List;
import java.io.IOException;
import java.util.Optional;

public class TestSplit {
    


    @Test
    public void testSplitsizeLineitem() throws IOException, MetadataException{

        
        BaseTable orders = new BaseTable(
            "tpch", "orders", "orders",
            new String[] {"o_orderkey", "o_custkey"},
            TableScanFilter.empty("tpch", "orders"));

        BaseTable lineitem = new BaseTable(
                "tpch", "lineitem", "lineitem",
                new String[] {"l_orderkey", "l_quantity"},
                TableScanFilter.empty("tpch", "lineitem"));

        Join join1 = new Join(orders, lineitem,
                new String[]{"o_orderkey", "o_custkey"},
                new String[]{"l_orderkey", "l_quantity"},
                new int[]{0}, new int[]{0}, new boolean[]{true, true},
                new boolean[]{true, true},
                JoinEndian.LARGE_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.PARTITIONED);

        JoinedTable joinedTable = new JoinedTable("tpch",
                "orders_join_lineitem",
                "orders_join_lineitem_table", join1);
                
        PixelsPlanner testplanner = new PixelsPlanner(
                123456, joinedTable, false, true, Optional.empty());
        
        List<InputSplit> testsplit=testplanner.getInputSplits(lineitem);

        System.out.println("testsplit size: "+testsplit.size());

    }
}
