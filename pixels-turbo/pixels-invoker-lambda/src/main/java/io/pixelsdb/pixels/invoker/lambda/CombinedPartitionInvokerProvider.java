package io.pixelsdb.pixels.invoker.lambda;

import io.pixelsdb.pixels.common.turbo.FunctionService;
import io.pixelsdb.pixels.common.turbo.Invoker;
import io.pixelsdb.pixels.common.turbo.InvokerProvider;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import static io.pixelsdb.pixels.common.turbo.FunctionService.lambda;
import static io.pixelsdb.pixels.common.turbo.WorkerType.COMBINED_PARTITION;;

public class CombinedPartitionInvokerProvider implements InvokerProvider{
    private static final ConfigFactory config = ConfigFactory.Instance();

    @Override
    public Invoker createInvoker()
    {
        String partitionedJoinWorker = config.getProperty("combinedpartition.worker.name");
        return new CombinedPartitionInvoker(partitionedJoinWorker);
    }

    @Override
    public WorkerType workerType()
    {
        return COMBINED_PARTITION;
    }

    @Override
    public boolean compatibleWith(FunctionService functionService)
    {
        return functionService.equals(lambda);
    }
}
