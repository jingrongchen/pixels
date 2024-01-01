/*
 * Copyright 2023 PixelsDB.
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
package io.pixelsdb.pixels.planner.plan.physical;

import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinInput;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author hank
 * @create 2023-09-19
 */
public class SingleStageJoinStreamOperator extends SingleStageJoinOperator
{
    public SingleStageJoinStreamOperator(String name, boolean complete,
                                         JoinInput joinInput, JoinAlgorithm joinAlgo)
    {
        super(name, complete, joinInput, joinAlgo);
    }

    public SingleStageJoinStreamOperator(String name, boolean complete,
                                         List<JoinInput> joinInputs, JoinAlgorithm joinAlgo)
    {
        super(name, complete, joinInputs, joinAlgo);
    }

    @Override
    public CompletableFuture<CompletableFuture<?>[]> execute()
    {
        // TODO: implement
        return null;
    }

    @Override
    public CompletableFuture<Void> executePrev()
    {
        // TODO: implement
        return null;
    }
}
