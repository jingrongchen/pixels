package io.pixelsdb.pixels.planner.plan.physical.domain;

import java.util.ArrayList;

public class FusionOutputInfo {
    
    private ArrayList<String> partitionOutputs = new ArrayList<>();

    /**
     * The path that the output file is written into.
     */
    private ArrayList<String> ScanOutputPath;
    /**
     * The information of the storage endpoint.
     */
    private StorageInfo storageInfo;
    /**
     * Whether the output file should be encoded.
     */
    private boolean encoding;

    public FusionOutputInfo() { }

    public ArrayList<String> getPartitionOutputs() {
        return partitionOutputs;
    }

    public void setPartitionOutputs(ArrayList<String> partitionOutputs) {
        this.partitionOutputs = partitionOutputs;
    }

    

    

}
