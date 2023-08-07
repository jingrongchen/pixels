package io.pixelsdb.pixels.invoker.lambda;

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
public class CombinedPartitionInvoker extends LambdaInvoker
{
    protected CombinedPartitionInvoker(String functionName)
    {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson)
    {
        return JSON.parseObject(outputJson, JoinOutput.class);
    }
}
