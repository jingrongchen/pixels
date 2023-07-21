package io.pixelsdb.pixels.planner.plan.physical.output;

import io.pixelsdb.pixels.common.turbo.Output;
import java.util.ArrayList;
import java.util.Set;

public class FusionOutput extends NonPartitionOutput {
    
    private ArrayList<PartitionOutput> partitionOutputs = new ArrayList<>();

    public FusionOutput() { super(); }
    
    public ArrayList<PartitionOutput> getPartitionOutputs() {
        return partitionOutputs;
    }

    public void setPartitionOutputs(ArrayList<PartitionOutput> partitionOutputs) {
        this.partitionOutputs = partitionOutputs;
    }


}
