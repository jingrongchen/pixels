package io.pixelsdb.pixels.worker.lambda;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.output.JoinOutput;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JoinScanFusionWorker {
    private static final Logger logger = LoggerFactory.getLogger(JoinScanFusionWorker.class);
    private final WorkerMetrics workerMetrics = new WorkerMetrics();

    @Override
    public JoinOutput handleRequest(JoinScanFusionInput event, Context context)
    {
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
        BaseJoinScanFusionWorker baseWorker = new BaseJoinScanFusionWorker(workerContext);
        return baseWorker.process(event);
    }
}
