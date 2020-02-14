package org.greenplum.pxf.api;

import org.greenplum.pxf.api.model.RequestContext;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceProvider {

    // TODO: pick a better executor thread pool, maybe some elastic threadpool
    // TODO: maybe 10X number of cores
    public static final ExecutorService EXECUTOR_SERVICE =
            new ThreadPoolExecutor(16, 16, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(10));

    public static ExecutorService get(RequestContext context) {
        // TODO: implement executor service per server / read Configuration here as well
        // TODO: a value can thread pool size can be specified in the pxf-site.xml file

        return EXECUTOR_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
    }
}
