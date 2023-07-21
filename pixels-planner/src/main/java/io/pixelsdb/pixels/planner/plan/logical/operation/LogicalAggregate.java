package io.pixelsdb.pixels.planner.plan.logical.operation;

import java.util.Arrays;

/**
 * 
 * logical aggreate operation
 * @author Jingrong
 * @date 2023-07-19
 */
public class LogicalAggregate {
    
    private String aggregationName;

    private String aggregationType;

    private int[] groupKeyColumnIds;

    private boolean distinct;

    private int[] aggregateColumnIds;

    private String[] aggregateColumnNames;

    public LogicalAggregate(String aggregationName, String aggregationType, int[] groupKeyColumnIds,  int[] aggregateColumnIds, boolean distinct, String[] aggregateColumnNames) {
        this.aggregationName = aggregationName;
        this.aggregationType = aggregationType;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.distinct = distinct;
        this.aggregateColumnIds = aggregateColumnIds;
    }

    public LogicalAggregate(String aggregationName, String aggregationType, int[] groupKeyColumnIds, int[] aggregateColumnIds, String[] aggregateColumnNames) {
        this.aggregationName = aggregationName;
        this.aggregationType = aggregationType;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.distinct = false;
    }

    public LogicalAggregate(String aggregationName, String aggregationType, int[] groupKeyColumnIds, int[] aggregateColumnIds) {
        this.aggregationName = aggregationName;
        this.aggregationType = aggregationType;
        this.groupKeyColumnIds = groupKeyColumnIds;
        this.aggregateColumnIds = aggregateColumnIds;
        this.aggregateColumnNames = null;
        this.distinct = false;
    }


    public String getAggregationName() {
        return aggregationName;
    }

    public void setAggregationName(String aggregationName) {
        this.aggregationName = aggregationName;
    }

    public String getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(String aggregationType) {
        this.aggregationType = aggregationType;
    }

    public int[] getGroupKeyColumnIds() {
        return groupKeyColumnIds;
    }

    public void setGroupKeyColumnIds(int[] groupKeyColumnIds) {
        this.groupKeyColumnIds = groupKeyColumnIds;
    }

    public int[] getAggregateColumnIds() {
        return aggregateColumnIds;
    }

    public void setAggregateColumnIds(int[] aggregateColumnIds) {
        this.aggregateColumnIds = aggregateColumnIds;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public String[] getAggregateColumnNames() {
        return aggregateColumnNames;
    }

    public void setAggregateColumnNames(String[] aggregateColumnNames) {
        this.aggregateColumnNames = aggregateColumnNames;
    }

    @Override
    public String toString() {
        return "LogicalAggregate {" +
                "aggregationName='" + aggregationName + '\'' +
                ", aggregationType='" + aggregationType + '\'' +
                ", groupKeyColumnIds=" + Arrays.toString(groupKeyColumnIds) +
                ", distinct=" + distinct +
                ", aggregateColumnIds=" + Arrays.toString(aggregateColumnIds) +
                ", aggregateColumnNames=" + aggregateColumnNames +
                '}';
    }
}
