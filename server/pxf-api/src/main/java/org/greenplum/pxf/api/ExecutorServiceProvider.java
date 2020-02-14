package org.greenplum.pxf.api;

import org.greenplum.pxf.api.model.RequestContext;

import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ExecutorServiceProvider {

    // TODO: pick a better executor thread pool, maybe some elastic threadpool
    // TODO: maybe 10X number of cores

    public static final ExecutorService EXECUTOR_SERVICE =
            new ThreadPoolExecutor(16, 16, 1,
                    TimeUnit.SECONDS, new LinkedBlockingDeque<>(10), new ThreadPoolExecutor.CallerRunsPolicy());

    public static ExecutorService get(RequestContext context) {
        // TODO: implement executor service per server / read Configuration here as well
        // TODO: a value can thread pool size can be specified in the pxf-site.xml file

        return EXECUTOR_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
    }
}
