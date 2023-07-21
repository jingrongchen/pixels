package io.pixelsdb.pixels.worker.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
// import org.slf4j.LoggerFactory;


public class ThreadScanWorker implements RequestHandler<ThreadScanInput, ScanOutput>
{
    private static final Logger logger = LogManager.getLogger(ThreadScanWorker.class);
    private final WorkerMetrics workerMetrics = new WorkerMetrics();

    @Override
    public ScanOutput handleRequest(ThreadScanInput event, Context context)
    {
        WorkerContext workerContext = new WorkerContext(logger, workerMetrics, context.getAwsRequestId());
        BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
        return baseWorker.process(event);
    }
}

