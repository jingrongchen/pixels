/*
 * Copyright 2022 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.planner.plan.physical.domain;

import java.util.LinkedList;

/**
 * consist of the information of scan pipe
 * scan pipe means the random combination among Filter,aggragtion,project  
 * the pipe result should store in the memory for future use, most for the next brocast join.
 * the data size after piping should be small.
 * 
 * @author Jingrong
 * @create 2023-07-19
 */
public class ScanPipeInfo {
    /**
     * the root of the scan pipe
     */
    private String root;

    /**
     * the Linklist contains the each operation
     */
    private LinkedList<Object> objectList;

    public ScanPipeInfo() {
        this.objectList = new LinkedList<>();
    }

    public ScanPipeInfo(String root, LinkedList<Object> objectList) {
        this.root = root;
        this.objectList = objectList;
        objectList.add(root);
    }

    public String getRoot() {
        return root;
    }

    public LinkedList<Object> getObjectList() {
        return objectList;
    }

    public void setRoot(String root) {
        this.root = root;
        objectList.add(root);
    }

    public void setObjectList(LinkedList<Object> objectList) {
        this.objectList = objectList;
    }

    public void addOperation(Object operation) {
        this.objectList.add(operation);
    }

}
