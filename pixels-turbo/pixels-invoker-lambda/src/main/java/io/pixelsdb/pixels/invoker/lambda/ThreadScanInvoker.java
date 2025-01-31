package io.pixelsdb.pixels.invoker.lambda;
import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.turbo.Output;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;


/**
 * Created at: 2023-05-08
 * Author: jingrong
 */
public class ThreadScanInvoker extends LambdaInvoker{
    protected ThreadScanInvoker(String functionName)
    {
        super(functionName);
    }

    @Override
    public Output parseOutput(String outputJson)
    {
        return JSON.parseObject(outputJson, ScanOutput.class);
    }
}
