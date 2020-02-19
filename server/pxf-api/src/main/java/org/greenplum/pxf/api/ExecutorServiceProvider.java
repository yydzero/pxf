package org.greenplum.pxf.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceProvider {

    // TODO: pick a better executor thread pool, maybe some elastic threadpool
    // TODO: maybe 10X number of cores

    public static final ExecutorService EXECUTOR_SERVICE =
            new ThreadPoolExecutor(16, 16, 1,
                    TimeUnit.SECONDS, new LinkedBlockingDeque<>(10), new ThreadPoolExecutor.CallerRunsPolicy());

    public static ExecutorService get() {
        // TODO: implement executor service per server / read Configuration here as well
        // TODO: a value can thread pool size can be specified in the pxf-site.xml file

        return EXECUTOR_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
    }
}
