package org.greenplum.pxf.service.processor;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.greenplum.pxf.api.concurrent.TaskAwareBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Factor class for the creation of {@link Cache}
 * output queues.
 */
public class ProcessorQueueCacheFactory {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    /**
     * Singleton instance of the FragmenterCacheFactory
     */
    private static final ProcessorQueueCacheFactory instance = new ProcessorQueueCacheFactory();

    private final Cache<String, TaskAwareBlockingQueue<?>> outputQueueCache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.SECONDS)
            .removalListener((RemovalListener<String, TaskAwareBlockingQueue<?>>) notification ->
                    LOG.debug("Removed output queue cache entry for transactionId {} with {} fragments with cause {}",
                            notification.getKey(),
                            (notification.getValue() != null ? notification.getValue().size() : 0),
                            notification.getCause().toString()))
            .build();

    /**
     * @return a singleton instance of the factory.
     */
    public static ProcessorQueueCacheFactory getInstance() {
        return instance;
    }

    /**
     * @return the cache for the fragmenter
     */
    public Cache<String, TaskAwareBlockingQueue<?>> getCache() {
        return outputQueueCache;
    }
}
