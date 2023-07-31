package io.pixelsdb.pixels.invoker.lambda;
import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static io.pixelsdb.pixels.common.turbo.FunctionService.lambda;
import static io.pixelsdb.pixels.common.turbo.WorkerType.JOINSCANFUSION;

public class JoinFusionScanInvokerProvider implements InvokerProvider{
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker()
    {
        String joinscanfusionWorker = config.getProperty("joinscanfusion.worker.name");
        return new JoinFusionScanInvoker(joinscanfusionWorker);
    }

    @Override
    public WorkerType workerType()
    {
        return JOINSCANFUSION;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService)
    {
        return functionService.equals(lambda);
    }
}
