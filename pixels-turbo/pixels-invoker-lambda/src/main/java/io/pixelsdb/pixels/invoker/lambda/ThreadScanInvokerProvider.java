package io.pixelsdb.pixels.invoker.lambda;
import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static io.pixelsdb.pixels.common.turbo.FunctionService.lambda;
import static io.pixelsdb.pixels.common.turbo.WorkerType.THREAD_SCAN;

/**
 * Created at: 2023-05-08
 * Author: jingrong
 */
public class ThreadScanInvokerProvider implements InvokerProvider{
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker()
    {
        String threadscanWorker = config.getProperty("threadscan.worker.name");
        return new ThreadScanInvoker(threadscanWorker);
    }

    @Override
    public WorkerType workerType()
    {
        return THREAD_SCAN;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService)
    {
        return functionService.equals(lambda);
    }
}
