package io.pixelsdb.pixels.planner.plan.physical.input;



import io.pixelsdb.pixels.planner.plan.physical.domain.BroadcastTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.JoinInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanPipeInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;

public class JoinScanFusionInput extends BroadcastJoinInput{
    
    /**
     * The information of the large partitioned table.
     */
    private PartitionedTableInfo PartitionlargeTable;

    /**
     * The information of the scan pipline.
    */
    
    private ScanPipeInfo scanPipelineInfo;
    
    /**
     * The output of the fusion.
     */
    private MultiOutputInfo fusionOutput;

    /**
     * Default constructor for Jackson.
     */
    public JoinScanFusionInput() { }

    
    public JoinScanFusionInput(long transId, BroadcastTableInfo smallTable, BroadcastTableInfo largeTable,
        JoinInfo joinInfo, boolean partialAggregationPresent,
        PartialAggregationInfo partialAggregationInfo, MultiOutputInfo output){
            super(transId, smallTable, largeTable, joinInfo, partialAggregationPresent, partialAggregationInfo, output);
        }
    
    public JoinScanFusionInput(long transId, BroadcastTableInfo smallTable, BroadcastTableInfo largeTable,
        JoinInfo joinInfo, boolean partialAggregationPresent,
        PartialAggregationInfo partialAggregationInfo, MultiOutputInfo output, PartitionedTableInfo PartitionlargeTable, ScanPipeInfo scanPipelineInfo){
            super(transId, smallTable, largeTable, joinInfo, partialAggregationPresent, partialAggregationInfo, output);
            this.PartitionlargeTable = PartitionlargeTable;
            this.scanPipelineInfo = scanPipelineInfo;
    }

    public PartitionedTableInfo getPartitionlargeTable() {
        return PartitionlargeTable;
    }

    public void setPartitionlargeTable(PartitionedTableInfo partitionlargeTable) {
        PartitionlargeTable = partitionlargeTable;
    }

    public ScanPipeInfo getScanPipelineInfo() {
        return scanPipelineInfo;
    }

    public void setScanPipelineInfo(ScanPipeInfo scanPipelineInfo) {
        this.scanPipelineInfo = scanPipelineInfo;
    }

    public MultiOutputInfo getFusionOutput() {
        return fusionOutput;
    }

    public void setFusionOutput(MultiOutputInfo fusionOutput) {
        this.fusionOutput = fusionOutput;
    }


}
