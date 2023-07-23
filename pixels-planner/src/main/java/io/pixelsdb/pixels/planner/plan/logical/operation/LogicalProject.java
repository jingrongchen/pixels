package io.pixelsdb.pixels.planner.plan.logical.operation;

import java.util.Arrays;
/**
 * 
 * logical project operation
 * @author Jingrong
 * @date 2023-07-19
 */
public class LogicalProject {
    
    private String[] projectFidlds;

    private int[] ProjectFieldIds;
    
    public LogicalProject() {
    }

    public LogicalProject(String[] projectFidlds, int[] ProjectFieldIds) {
        this.projectFidlds = projectFidlds;
        this.ProjectFieldIds = ProjectFieldIds;
    }

    public String[] getProjectFidlds() {
        return projectFidlds;
    }

    public void setProjectFidlds(String[] projectFidlds) {
        this.projectFidlds = projectFidlds;
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


    public void setProjectFieldIds(int[] ProjectFieldIds) {
        this.ProjectFieldIds = ProjectFieldIds;
    }

    @Override
    public String toString() {
        return "LogicalProject {ProjectFieldIds=" + Arrays.toString(ProjectFieldIds) + ", projectFidlds="
                + Arrays.toString(projectFidlds) + "}";
    }

}
