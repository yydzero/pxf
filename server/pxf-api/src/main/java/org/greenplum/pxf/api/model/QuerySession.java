package org.greenplum.pxf.api.model;

import com.google.common.base.Objects;
import org.apache.hadoop.security.UserGroupInformation;

import java.time.Instant;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * Maintains state of the query. The state is shared across multiple threads
 * for the same query slice.
 *
 * @param <T> the type of the tuple that is returned by the query
 */
public class QuerySession<T> {

    private final AtomicBoolean queryCancelled;

    private final AtomicBoolean queryErrored;

    private final BlockingDeque<T> outputQueue;
    private final AtomicInteger activeTasks;
    private final AtomicInteger activeSegments;

    private final String queryId;
    private final Instant startTime;
    private Instant endTime;
    private Instant cancelTime;

    private Instant errorTime;

    private UserGroupInformation effectiveUgi;

    public QuerySession(String queryId, BlockingDeque<T> outputQueue) {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.outputQueue = requireNonNull(outputQueue, "outputQueue is null");
        this.startTime = Instant.now();
        this.queryCancelled = new AtomicBoolean();
        this.queryErrored = new AtomicBoolean();
        this.activeTasks = new AtomicInteger();
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
     * Returns the shared {@link BlockingQueue} for the given query. Multiple
     * requests share the queue and produce tuples that are stored in this
     * queue.
     *
     * @return the blocking output queue for the given query
     */
    public BlockingDeque<T> getOutputQueue() {
        return outputQueue;
    }

    /**
     * Returns true if pending tasks are registered to the queue, false otherwise
     *
     * @return true if pending tasks are registered to the queue, false otherwise
     */
    public boolean hasPendingTasks() {
        return activeTasks.get() > 0;
    }

    /**
     * Returns the number of active tasks registered to this queue
     *
     * @return the number of active tasks registered to this queue
     */
    public int activeTaskCount() {
        return activeTasks.get();
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
    public void unregisterSegment(int segmentId) {
        if (activeSegments.decrementAndGet() == 0) {
            endTime = Instant.now();
        }
    }

    /**
     * Registers a task to the queue
     */
    public void registerTask() {
        activeTasks.incrementAndGet();
    }

    /**
     * De-registers a task from the queue
     */
    public void deregisterTask() {
        activeTasks.decrementAndGet();
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
