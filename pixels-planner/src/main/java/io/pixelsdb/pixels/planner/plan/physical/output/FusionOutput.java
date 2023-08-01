package io.pixelsdb.pixels.planner.plan.physical.output;

import io.pixelsdb.pixels.common.turbo.Output;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class FusionOutput extends NonPartitionOutput {
    
    private HashMap<Integer,PartitionOutput> partitionOutputs;

    public FusionOutput() { 
        super(); 
        this.partitionOutputs = new HashMap<>();
        ;}
    
    public HashMap<Integer,PartitionOutput> getPartitionOutputs() {
        return partitionOutputs;
    }

    public void setPartitionOutputs(HashMap<Integer,PartitionOutput> partitionOutputs) {
        this.partitionOutputs = partitionOutputs;
    }

    // public void addPartitionOutput(PartitionOutput partitionOutput) {
    //     this.partitionOutputs.put(partitionOutput);
    // }

    public void addFirstPartitionOutput(PartitionOutput partitionOutput) {
        this.partitionOutputs.put(0, partitionOutput);
    }

    public void addSecondPartitionOutput(PartitionOutput partitionOutput) {
        
        this.partitionOutputs.put(1, partitionOutput);
    }

    public PartitionOutput getFirstPartitionOutput() {
        return this.partitionOutputs.get(0);
    }

    public PartitionOutput getSecondPartitionOutput() {
        return this.partitionOutputs.get(1);
    }


}
