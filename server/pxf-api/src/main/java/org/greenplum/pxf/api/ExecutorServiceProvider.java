package org.greenplum.pxf.api;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceProvider {

    // TODO: pick a better executor thread pool, maybe some elastic threadpool
    // TODO: maybe 10X number of cores

    public static final int MACHINE_CORES = Runtime.getRuntime().availableProcessors();

    public static final int THREAD_POOL_SIZE = MACHINE_CORES * 2;

    public static final ThreadFactory NAMED_THREAD_FACTORY =
            new ThreadFactoryBuilder().setNameFormat("pxf-worker-%d").build();

    public static final ExecutorService EXECUTOR_SERVICE = MoreExecutors.getExitingExecutorService(
            new ThreadPoolExecutor(THREAD_POOL_SIZE, THREAD_POOL_SIZE, 1,
                    TimeUnit.SECONDS, new PriorityBlockingQueue<>(1000),
                    NAMED_THREAD_FACTORY, new ThreadPoolExecutor.CallerRunsPolicy()));

    public static ExecutorService get() {
        // TODO: implement executor service per server / read Configuration here as well
        // TODO: a value can thread pool size can be specified in the pxf-site.xml file

        return EXECUTOR_SERVICE;
    }

    public static void shutdown() {
        EXECUTOR_SERVICE.shutdown();
    }
}
