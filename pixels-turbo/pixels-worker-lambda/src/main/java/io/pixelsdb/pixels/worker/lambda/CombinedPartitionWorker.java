package io.pixelsdb.pixels.worker.lambda;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BaseCombinedPartitionWorker;
import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import io.pixelsdb.pixels.planner.plan.physical.input.CombinedPartitionInput;

public class CombinedPartitionWorker implements RequestHandler<CombinedPartitionInput, JoinOutput>{
    
    private static final Logger logger = LogManager.getLogger(CombinedPartitionWorker.class);
    private final WorkerMetrics workerMetrics = new WorkerMetrics();

    @Override
    public JoinOutput handleRequest(CombinedPartitionInput event, Context context)
    {
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
        BaseCombinedPartitionWorker baseWorker = new BaseCombinedPartitionWorker(workerContext);
        return baseWorker.process(event);
    }
}
