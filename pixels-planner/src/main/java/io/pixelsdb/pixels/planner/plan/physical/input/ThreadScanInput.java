package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.planner.plan.physical.domain.ThreadOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ThreadScanTableInfo;
/**
 * The input format for multithread table scan.
 * @author jingrong
 * @create 2023-05-08
 */
public class ThreadScanInput extends Input {
    /**
     * The information of the table to scan.
     */
    private ThreadScanTableInfo tableInfo;
    /**
     * Whether the columns in tableInfo.columnsToRead should be included in the scan output.
     */
    private boolean[] scanProjection;
    /**
     * Whether the partial aggregation exists.
     */
    private boolean partialAggregationPresent = false;
    /**
     * The information of the partial aggregation.
     */
    private PartialAggregationInfo partialAggregationInfo;

    /**
     * The output of the scan.
     */
    private ThreadOutputInfo output;

    /**
     * Default constructor for Jackson.
     */
    public ThreadScanInput()
    {
        super(-1);
    }

    public ThreadScanInput(long queryId, ThreadScanTableInfo tableInfo, boolean[] scanProjection,
                     boolean partialAggregationPresent, PartialAggregationInfo partialAggregationInfo, ThreadOutputInfo output)
    {
        super(queryId);
        this.tableInfo = tableInfo;
        this.scanProjection = scanProjection;
        this.partialAggregationPresent = partialAggregationPresent;
        this.partialAggregationInfo = partialAggregationInfo;
        this.output = output;
    }

    public ThreadScanTableInfo getTableInfo()
    {
        return tableInfo;
    }

    public void setTableInfo(ThreadScanTableInfo tableInfo)
    {
        this.tableInfo = tableInfo;
    }

    public boolean[] getScanProjection()
    {
        return scanProjection;
    }

    public void setScanProjection(boolean[] scanProjection)
    {
        this.scanProjection = scanProjection;
    }

    public boolean isPartialAggregationPresent()
    {
        return partialAggregationPresent;
    }

    public void setPartialAggregationPresent(boolean partialAggregationPresent)
    {
        this.partialAggregationPresent = partialAggregationPresent;
    }

    public PartialAggregationInfo getPartialAggregationInfo()
    {
        return partialAggregationInfo;
    }

    public void setPartialAggregationInfo(PartialAggregationInfo partialAggregationInfo)
    {
        this.partialAggregationInfo = partialAggregationInfo;
    }

    public ThreadOutputInfo getOutput()
    {
        return output;
    }

    public void setOutput(ThreadOutputInfo output)
    {
        this.output = output;
    }
}
