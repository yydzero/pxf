package org.greenplum.pxf.service.processor;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.greenplum.pxf.api.model.QuerySession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Factory class for the creation of {@link Cache} QuerySession
 */
public class QuerySessionCacheFactory {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    /**
     * Singleton instance of the QuerySessionCacheFactory
     */
    private static final QuerySessionCacheFactory instance = new QuerySessionCacheFactory();

    private final Cache<String, QuerySession<?>> outputQueueCache = CacheBuilder.newBuilder()
            .expireAfterAccess(10, TimeUnit.SECONDS)
            .removalListener((RemovalListener<String, QuerySession<?>>) notification ->
                    LOG.debug("Removed output queue cache entry for queryId {} with cause {}",
                            notification.getKey(),
                            notification.getCause().toString()))
            .build();

    /**
     * @return a singleton instance of the factory.
     */
    public static QuerySessionCacheFactory getInstance() {
        return instance;
    }

    /**
     * @return the cache for the QuerySession
     */
    public Cache<String, QuerySession<?>> getCache() {
        return outputQueueCache;
    }
}
