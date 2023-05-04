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
package io.pixelsdb.pixels.experiments;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import io.pixelsdb.pixels.common.metadata.domain.Splits;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.PlanOptimizer;
import io.pixelsdb.pixels.planner.plan.logical.*;
import io.pixelsdb.pixels.planner.plan.physical.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import io.pixelsdb.pixels.planner.PixelsPlanner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


public class TestPureServerlessJoin {
    @Test
    public void testChainJoin() throws IOException, MetadataException
    {
        BaseTable region = new BaseTable(
                "tpch", "region", "region",
                new String[] {"r_regionkey", "r_name"},
                TableScanFilter.empty("tpch", "region"));

        BaseTable nation = new BaseTable(
                "tpch", "nation", "nation",
                new String[] {"n_nationkey", "n_name", "n_regionkey"},
                TableScanFilter.empty("tpch", "nation"));

        Join join1 = new Join(region, nation,
                new String[] {"r_name"}, new String[]{"n_nationkey", "n_name"},
                new int[]{0}, new int[]{2}, new boolean[]{false, true}, new boolean[]{true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable1 = new JoinedTable(
                "join_1", "region_join_nation", "region_join_nation", join1);

        BaseTable supplier = new BaseTable(
                "tpch", "supplier", "supplier",
                new String[] {"s_suppkey", "s_name", "s_acctbal", "s_nationkey"},
                TableScanFilter.empty("tpch", "supplier"));

        Join join2 = new Join(joinedTable1, supplier,
                new String[] {"r_name", "n_name"},
                new String[] {"s_suppkey", "s_name", "s_acctbal"},
                new int[]{1}, new int[]{3}, new boolean[]{true, false, true},
                new boolean[]{true, true, true, false},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable joinedTable2 = new JoinedTable(
                "join_2", "region_join_nation_join_supplier",
                "region_join_nation_join_supplier", join2);

        BaseTable lineitem = new BaseTable(
                "tpch", "lineitem", "lineitem",
                new String[] {"l_orderkey", "l_suppkey", "l_extendedprice", "l_shipdate"},
                TableScanFilter.empty("tpch", "lineitem"));

        Join join3 = new Join(joinedTable2, lineitem,
                new String[] {"r_name", "n_name", "s_name", "s_acctbal"},
                new String[] {"l_orderkey", "l_extendedprice", "l_shipdate"},
                new int[]{2}, new int[]{1}, new boolean[]{true, true, false, true, true},
                new boolean[]{true, false, true, true},
                JoinEndian.SMALL_LEFT, JoinType.EQUI_INNER, JoinAlgorithm.BROADCAST);

        JoinedTable root = new JoinedTable("tpch",
                "region_join_nation_join_supplier_join_lineitem",
                "region_join_nation_join_supplier_join_lineitem", join3);

        PixelsPlanner joinExecutor = new PixelsPlanner(
                123456, root, false, true, Optional.empty());

        Operator joinOperator = joinExecutor.getRootOperator();
    }
}
