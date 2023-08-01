package io.pixelsdb.pixels.invoker.lambda;
import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;

/**
 * Created at: 2023-05-08
 * Author: jingrong
 */
public class JoinFusionScanInvoker extends LambdaInvoker{
    protected JoinFusionScanInvoker(String functionName)
    {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson)
    {
        return JSON.parseObject(outputJson, FusionOutput.class);
    }
}
