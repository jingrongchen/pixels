package io.pixelsdb.pixels.planner.plan.physical.input;

import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import java.util.List;

public class CombinedPartitionInput extends PartitionInput{
    
    private List<Integer> localHashedPartitionIds;

    private String intermideatePath;

    private String bigTableName;

    private String smallTableName;

    private int bigTablePartitionCount;

    private int smallTablePartitionCount;

    private String localTableName;

    private int lassversion;

    /**
     * The information of the small partitioned table.
     */
    private PartitionedTableInfo smallTable;
    /**
     * The information of the large partitioned table.
     */
    private PartitionedTableInfo largeTable;
    /**
     * The information of the partitioned join.
     */
    private PartitionedJoinInfo joinInfo;


    //  JoinInput;
    /**
     * Whether the partial aggregation exists.
     */
    private boolean partialAggregationPresent = false;

    /**
     * The information of the partial aggregation.
     */
    private PartialAggregationInfo partialAggregationInfo;

    /**
     * The information of the join output files.<br/>
     * <b>Note: </b>for inner, right-outer, and natural joins, there is only one output file.
     * For left-outer and full-outer joins, there might be an additional output file for the left-outer records.
     */
    private MultiOutputInfo multioutput;

    // public CombinedPartitionInput(PartitionInput partitionInput) {
    //     super(partitionInput);
    // }
    

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

    /**
     * Get the information about the join output.
     * @return the join output information
     */
    public MultiOutputInfo getMultiOutput()
    {
        return multioutput;
    }

    /**
     * Set the information about the join output.
     * @param output the join output
     */
    public void setMultiOutput(MultiOutputInfo output)
    {
        this.multioutput = output;
    }


    public PartitionedTableInfo getSmallTable()
    {
        return smallTable;
    }

    public void setSmallTable(PartitionedTableInfo smallTable)
    {
        this.smallTable = smallTable;
    }

    public PartitionedTableInfo getLargeTable()
    {
        return largeTable;
    }

    public void setLargeTable(PartitionedTableInfo largeTable)
    {
        this.largeTable = largeTable;
    }

    public PartitionedJoinInfo getJoinInfo()
    {
        return joinInfo;
    }

    public void setJoinInfo(PartitionedJoinInfo joinInfo)
    {
        this.joinInfo = joinInfo;
    }

    public CombinedPartitionInput() {
    }

    public String getLocalTableName() {
        return localTableName;
    }

    public void setLocalTableName(String localTableName) {
        this.localTableName = localTableName;
    }

    public int getBigTablePartitionCount() {
        return bigTablePartitionCount;
    }

    public void setBigTablePartitionCount(int bigTablePartitionCount) {
        this.bigTablePartitionCount = bigTablePartitionCount;
    }

    public int getSmallTablePartitionCount() {
        return smallTablePartitionCount;
    }

    public void setSmallTablePartitionCount(int smallTablePartitionCount) {
        this.smallTablePartitionCount = smallTablePartitionCount;
    }

    public String getBigTableName() {
        return bigTableName;
    }

    public void setBigTableName(String bigTableName) {
        this.bigTableName = bigTableName;
    }

    public String getSmallTableName() {
        return smallTableName;
    }

    public void setSmallTableName(String smallTableName) {
        this.smallTableName = smallTableName;
    }

    public String getIntermideatePath() {
        return intermideatePath;
    }

    public void setIntermideatePath(String intermideatePath) {
        this.intermideatePath = intermideatePath;
    }

    public List<Integer> getLocalHashedPartitionIds() {
        return localHashedPartitionIds;
    }

    public void setLocalHashedPartitionIds(List<Integer> localHashedPartitionIds) {
        this.localHashedPartitionIds = localHashedPartitionIds;
    }


    public int getLassversion() {
        return lassversion;
    }

    public void setLassversion(int lassversion) {
        this.lassversion = lassversion;
    }

    

}
