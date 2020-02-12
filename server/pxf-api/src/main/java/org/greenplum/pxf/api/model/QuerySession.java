package org.greenplum.pxf.api.model;

import com.google.common.base.Objects;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Objects.requireNonNull;

/**
 * Maintains state of the query. The state is shared across multiple threads
 * for the same query slice.
 *
 * @param <T> the type of the tuple that is returned by the query
 */
public class QuerySession<T> {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private final AtomicBoolean queryCancelled;
    private final AtomicBoolean queryErrored;
    private final AtomicInteger activeSegments;

    private final String queryId;
    private final Instant startTime;
    private Instant endTime;
    private Instant cancelTime;

    private Instant errorTime;

    private UserGroupInformation effectiveUgi;

    /**
     * Main lock guarding all access
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition for waiting takes
     */
    private final Condition moreTasks = lock.newCondition();

    public QuerySession(String queryId) {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.startTime = Instant.now();
        this.queryCancelled = new AtomicBoolean();
        this.queryErrored = new AtomicBoolean();
        this.activeSegments = new AtomicInteger();
    }

    /**
     * Cancels the query, the first thread to cancel the query sets the cancel
     * time
     */
    public void cancelQuery() {
        if (!queryCancelled.getAndSet(true)) {
            cancelTime = Instant.now();
        }
    }

    /**
     * Check whether the query was cancelled
     *
     * @return true if the query was cancelled, false otherwise
     */
    public boolean isQueryCancelled() {
        return queryCancelled.get();
    }

    /**
     * Marks the query as errored, the first thread to error the query sets
     * the error time
     */
    public void errorQuery() {
        if (!queryErrored.getAndSet(true)) {
            errorTime = Instant.now();
        }
    }

    /**
     * Check whether the query has errors
     *
     * @return true if the query has errors, false otherwise
     */
    public boolean isQueryErrored() {
        return queryErrored.get();
    }

    /**
     * Registers a segment
     *
     * @param segmentId the segment identifier
     */
    public void registerSegment(int segmentId) {
        activeSegments.incrementAndGet();
    }

    /**
     * Deregisters a segment, and the last segment to deregister the endTime
     * is recorded.
     *
     * @param segmentId the segment identifier
     */
    public void deregisterSegment(int segmentId) {
        if (activeSegments.decrementAndGet() == 0) {
            endTime = Instant.now();
        }
    }

    /**
     * Determines whether the query session is active. The query session
     * becomes inactive if the query is errored or the query is cancelled.
     *
     * @return true if the query is active, false when the query has errors or is cancelled
     */
    public boolean isActive() {
        return !isQueryErrored() && !isQueryCancelled();
    }

    public void requestMoreTasks() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            moreTasks.signal();
        } finally {
            lock.unlock();
        }
    }

    public void waitForMoreTasks() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            LOG.debug("Waiting for more tasks");
            moreTasks.await();
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "QuerySession{" +
                "queryId='" + queryId + '\'' +
                '}';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QuerySession<?> that = (QuerySession<?>) o;
        return Objects.equal(queryId, that.queryId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(queryId);
    }
}
