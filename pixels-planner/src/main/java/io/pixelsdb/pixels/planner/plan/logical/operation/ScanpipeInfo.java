package io.pixelsdb.pixels.planner.plan.logical.operation;

import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
public class ScanpipeInfo {

    private String scanroot;

    private String[] includeCols;

    private String[] projectFidlds;

    private int[] ProjectFieldIds;

    private boolean isPartialAggregation;

    private PartialAggregationInfo partialAggregationInfo;

    public ScanpipeInfo(String scanroot, String[] includeCols, String[] projectFidlds) {
        this.scanroot = scanroot;
        this.includeCols = includeCols;
        this.projectFidlds = projectFidlds;
    }

    public ScanpipeInfo(String scanroot, String[] includeCols, String[] projectFidlds, boolean isPartialAggregation,
            PartialAggregationInfo partialAggregationInfo) {
        this.scanroot = scanroot;
        this.includeCols = includeCols;
        this.projectFidlds = projectFidlds;
        this.isPartialAggregation = isPartialAggregation;
        this.partialAggregationInfo = partialAggregationInfo;
    }
    
    


    public ScanpipeInfo() {
    }

    public boolean isPartialAggregation() {
        return isPartialAggregation;
    }

    public void setPartialAggregation(boolean isPartialAggregation) {
        this.isPartialAggregation = isPartialAggregation;
    }

    public boolean[] getProjectFieldIds(String [] tablescanColtoRead) {
        int[] projectFieldIds = new int[projectFidlds.length];
        boolean[] tablescanColtoReadFlag = new boolean[tablescanColtoRead.length];

        for (int i = 0; i < projectFidlds.length; i++) {
            for (int j = 0; j < tablescanColtoRead.length; j++) {
                if (projectFidlds[i].equals(tablescanColtoRead[j])) {
                    projectFieldIds[i] = j;
                    break;
                }
            }
        }

        for (int i = 0; i < projectFieldIds.length; i++) {
            tablescanColtoReadFlag[projectFieldIds[i]] = true;
        }
        
        return tablescanColtoReadFlag;
    }

    public PartialAggregationInfo getPartialAggregationInfo() {
        return partialAggregationInfo;
    }

    public void setPartialAggregationInfo(PartialAggregationInfo partialAggregationInfo) {
        this.partialAggregationInfo = partialAggregationInfo;
    }

    public String getScanroot() {
        return scanroot;
    }


    public void setScanroot(String scanroot) {
        this.scanroot = scanroot;
    }

    public String[] getIncludeCols() {
        return includeCols;
    }

    public void setIncludeCols(String[] includeCols) {
        this.includeCols = includeCols;
    }

    public String[] getProjectFidlds() {
        return projectFidlds;
    }

    public void setProjectFidlds(String[] projectFidlds) {
        this.projectFidlds = projectFidlds;
    }

    public int[] getProjectFieldIds() {
        return ProjectFieldIds;
    }   

    public void setProjectFieldIds(int[] ProjectFieldIds) {
        this.ProjectFieldIds = ProjectFieldIds;
    }

    

    public ScanpipeInfo(String root) {
        this.scanroot = root;
    }

    public ScanpipeInfo(String root, String[] includeCols) {
        this.scanroot = root;
        this.includeCols = includeCols;
    }



    
}
