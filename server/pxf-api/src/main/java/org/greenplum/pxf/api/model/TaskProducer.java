package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.ExecutorServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class TaskProducer<T> extends Thread {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ExecutorService executor = ExecutorServiceProvider.get();
    private final QuerySession<T> querySession;

    public TaskProducer(QuerySession<T> querySession) {
        this.querySession = requireNonNull(querySession, "querySession cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try {
            BlockingDeque<Iterator<QuerySplit>> queue = querySession.getSplitIteratorQueue();
            Iterator<QuerySplit> iterator;

            LOG.debug("fetching QuerySplit iterator");
            /* Wait for 11 seconds until segments have registered their iterators */
            while (querySession.isActive() && (iterator = queue.poll(11, TimeUnit.SECONDS)) != null) {
                LOG.debug("new QuerySplit iterator fetched");
                while (iterator.hasNext() && querySession.isActive()) {
                    QuerySplit split = iterator.next();

                    LOG.debug("Submitting {} to the pool for query {}", split, querySession);

                    executor.submit(new TupleReaderTask<>(split, querySession));
                    // Increase the number of jobs submitted to the executor
                    querySession.registerTask();
                    // Need to mark the session as started producing
                    querySession.markAsStartedProducing();
                }
            }
            /* Edge case when there are no splits or an error occurs */
            querySession.markAsStartedProducing();
            LOG.debug("task producer completed");
        } catch (Exception ex) {
            querySession.errorQuery();
            throw new RuntimeException(ex);
        }
    }
}