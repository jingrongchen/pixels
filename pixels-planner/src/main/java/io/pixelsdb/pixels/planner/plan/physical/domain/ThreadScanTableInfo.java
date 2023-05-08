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

import java.util.List;

/**
 * @author Jingrong
 * @create 2023-05-08
 */

public class ThreadScanTableInfo extends TableInfo  {
    /**
     * The scan inputs of the table.
     */
    private List<InputSplit> inputSplits;
    /**
     * The json string List of the filter (i.e., predicates) to be used in scan in different thread.
     */
    private List<String> filter;

    /**
     * Default constructor for Jackson.
     */
    public ThreadScanTableInfo() { }

    public ThreadScanTableInfo(String tableName, boolean base, String[] columnsToRead,
                         StorageInfo storageInfo, List<InputSplit> inputSplits, List<String> filter)
    {
        super(tableName, base, columnsToRead, storageInfo);
        this.inputSplits = inputSplits;
        this.filter = filter;
    }

    public List<InputSplit> getInputSplits()
    {
        return inputSplits;
    }

    public void setInputSplits(List<InputSplit> inputSplits)
    {
        this.inputSplits = inputSplits;
    }

    public List<String> getFilter()
    {
        return filter;
    }

    public void setFilter(List<String> filter)
    {
        this.filter = filter;
    }
}
