package org.greenplum.pxf.api;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Comparator;
import java.util.concurrent.*;

public class ExecutorServiceProvider {

    // TODO: pick a better executor thread pool, maybe some elastic threadpool
    // TODO: maybe 10X number of cores

    public static final ThreadFactory NAMED_THREAD_FACTORY =
            new ThreadFactoryBuilder().setNameFormat("pxf-worker-%d").build();

    public static final ExecutorService EXECUTOR_SERVICE =
            new ThreadPoolExecutor(32, 32, 1,
                    TimeUnit.SECONDS, new PriorityBlockingQueue<>(1000 ), NAMED_THREAD_FACTORY, new ThreadPoolExecutor.CallerRunsPolicy());

    public static ExecutorService get() {
        // TODO: implement executor service per server / read Configuration here as well
        // TODO: a value can thread pool size can be specified in the pxf-site.xml file

        return EXECUTOR_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
    }
}
