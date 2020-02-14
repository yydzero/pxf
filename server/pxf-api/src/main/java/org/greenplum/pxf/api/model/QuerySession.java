package org.greenplum.pxf.api.model;

import com.google.common.base.Objects;
import org.apache.hadoop.security.UserGroupInformation;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
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
    private final AtomicInteger activeSegments;

    private final String queryId;
    private final Instant startTime;
    private Instant endTime;
    private Instant cancelTime;

    private Instant errorTime;

    private UserGroupInformation effectiveUgi;

    private List<QuerySplit> querySplitList;

    private final BlockingDeque<List<T>> outputQueue;

    public QuerySession(String queryId) {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.startTime = Instant.now();
        this.queryCancelled = new AtomicBoolean();
        this.queryErrored = new AtomicBoolean();
        this.activeSegments = new AtomicInteger();
        this.outputQueue = new LinkedBlockingDeque<>(200);
    }

    public BlockingDeque<List<T>> getOutputQueue() {
        return outputQueue;
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
            querySplitList = null;
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

    /**
     * Returns a list of splits for the query. The list of splits is only set
     * when the "fragmenter" cache is enabled
     *
     * @return the list of splits for the query
     */
    public List<QuerySplit> getQuerySplitList() {
        return querySplitList;
    }

    /**
     * Sets a list of splits for the query.
     *
     * @param querySplitList the list of query splits
     */
    public void setQuerySplitList(List<QuerySplit> querySplitList) {
        this.querySplitList = querySplitList;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "QuerySession@" +
                Integer.toHexString(System.identityHashCode(this)) +
                "{queryId='" + queryId + '\'' +
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
