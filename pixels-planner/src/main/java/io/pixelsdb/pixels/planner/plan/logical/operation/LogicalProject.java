package io.pixelsdb.pixels.planner.plan.logical.operation;


/**
 * 
 * logical project operation
 * @author Jingrong
 * @date 2023-07-19
 */
public class LogicalProject {
    
    private String[] projectFidlds;

    private Integer[] ProjectFieldIds;
    
    public LogicalProject(String[] projectFidlds, Integer[] ProjectFieldIds) {
        this.projectFidlds = projectFidlds;
        this.ProjectFieldIds = ProjectFieldIds;
    }

    public String[] getProjectFidlds() {
        return projectFidlds;
    }

    public void setProjectFidlds(String[] projectFidlds) {
        this.projectFidlds = projectFidlds;
    }

    public Integer[] getProjectFieldIds() {
        return ProjectFieldIds;
    }

    public void setProjectFieldIds(Integer[] ProjectFieldIds) {
        this.ProjectFieldIds = ProjectFieldIds;
    }


}
