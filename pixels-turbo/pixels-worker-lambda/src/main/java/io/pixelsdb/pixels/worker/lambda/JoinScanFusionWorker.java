package io.pixelsdb.pixels.worker.lambda;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class JoinScanFusionWorker implements RequestHandler<JoinScanFusionInput, FusionOutput>{
    private static final Logger logger = LogManager.getLogger(JoinScanFusionWorker.class);
    private final WorkerMetrics workerMetrics = new WorkerMetrics();

    @Override
    public FusionOutput handleRequest(JoinScanFusionInput event, Context context)
    {
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
        BaseJoinScanFusionWorker baseWorker = new BaseJoinScanFusionWorker(workerContext);
        return baseWorker.process(event);
    }
}
