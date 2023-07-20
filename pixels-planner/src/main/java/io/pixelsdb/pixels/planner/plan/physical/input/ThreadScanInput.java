package io.pixelsdb.pixels.planner.plan.physical.input;
import java.util.List;
import java.util.HashMap;
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
    private HashMap<String, List<Boolean>> scanProjection;
    /**
     * Whether the partial aggregation exists.
     */
    private boolean partialAggregationPresent = false;
    /**
     * The information of the partial aggregation.
     */
    private List<PartialAggregationInfo> partialAggregationInfo;

    /**
     * The output of the scan.
     */
    private ThreadOutputInfo output;

    /**
     * The output of the scan.
     */
    private HashMap<String, List<Integer>> filterOnAggreation;


    /**
     * Default constructor for Jackson.
     */
    public ThreadScanInput()
    {
        super(-1);
    }

    public ThreadScanInput(long queryId, ThreadScanTableInfo tableInfo, HashMap<String, List<Boolean>> scanProjection,
                     boolean partialAggregationPresent, List<PartialAggregationInfo> partialAggregationInfo, ThreadOutputInfo output,HashMap<String, List<Integer>> filterOnAggreation)
    {
        super(queryId);
        this.tableInfo = tableInfo;
        this.scanProjection = scanProjection;
        this.partialAggregationPresent = partialAggregationPresent;
        this.partialAggregationInfo = partialAggregationInfo;
        this.output = output;
        this.filterOnAggreation=filterOnAggreation;
    }

    public ThreadScanTableInfo getTableInfo()
    {
        return tableInfo;
    }

    public void setTableInfo(ThreadScanTableInfo tableInfo)
    {
        this.tableInfo = tableInfo;
    }

    public HashMap<String, List<Boolean>> getScanProjection()
    {
        return scanProjection;
    }

    public void setScanProjection(HashMap<String, List<Boolean>> scanProjection)
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

    public List<PartialAggregationInfo> getPartialAggregationInfo()
    {
        return partialAggregationInfo;
    }

    public void setPartialAggregationInfo(List<PartialAggregationInfo> partialAggregationInfo)
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

    public void setFilterOnAggreation(HashMap<String, List<Integer>> filterOnAggreation)
    {
        this.filterOnAggreation=filterOnAggreation;

    }

    public HashMap<String, List<Integer>> getFilterOnAggreation()
    {
        return filterOnAggreation;

    }

}
