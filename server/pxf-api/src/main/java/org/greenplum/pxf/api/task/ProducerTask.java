package org.greenplum.pxf.api.task;

import com.google.common.collect.Lists;
import org.greenplum.pxf.api.ExecutorServiceProvider;
import org.greenplum.pxf.api.concurrent.BoundedExecutor;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.QuerySplit;
import org.greenplum.pxf.api.model.QuerySplitSegmentIterator;
import org.greenplum.pxf.api.model.QuerySplitter;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class ProducerTask<T, M> extends Thread {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    private final ExecutorService executor = ExecutorServiceProvider.get();
    private final QuerySession<T, M> querySession;
    private final BoundedExecutor boundedExecutor;
    private int processorCount;

    public ProducerTask(QuerySession<T, M> querySession) {
        this.querySession = requireNonNull(querySession, "querySession cannot be null");
        // TODO: allow the maxThreads to be configurable
        this.boundedExecutor = new BoundedExecutor(executor,
                Math.max(1, Runtime.getRuntime().availableProcessors()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try {
            BlockingDeque<Processor<T>> queue = querySession.getProcessorQueue();
            Processor<T> processor;

            LOG.debug("fetching QuerySplit iterator");
            while (querySession.isActive()) {
                processor = queue.poll(250, TimeUnit.MILLISECONDS);
                if (processor == null) {
                    if (processorCount > 0) {
                        break;
                    } else {
                        // We expect at least one processor, since the query
                        // session creation is tied to the creation of a
                        // producer task
                        continue;
                    }
                }

                processorCount++;
                Iterator<QuerySplit> iterator = new QuerySplitSegmentIterator(processor.getSegmentId(), querySession.getTotalSegments(), getQuerySplitterIterator(processor));
                LOG.debug("new QuerySplit iterator fetched");
                while (iterator.hasNext() && querySession.isActive()) {
                    QuerySplit split = iterator.next();
                    LOG.debug("Submitting {} to the pool for query {}", split, querySession);
                    boundedExecutor.execute(new TupleReaderTask<>(processor, split, querySession));
                    // Increase the number of jobs submitted to the executor
                    querySession.registerTask();
                }
            }
            LOG.debug("task producer completed");
        } catch (Exception ex) {
            querySession.errorQuery(ex);
            throw new RuntimeException(ex);
        } finally {
            querySession.markAsFinishedProducing();
        }
    }

    /**
     * Gets the {@link QuerySplit} iterator. If the "fragmenter cache" is
     * enabled, the first thread will process the list of fragments and store
     * the query split list in the querySession. All other threads will use
     * the "cached" query split list for the given query. If the "fragmenter
     * cache" is disabled, return the initialized QuerySplitter for the given
     * processor.
     *
     * @return a {@link QuerySplit} iterator
     */
    public Iterator<QuerySplit> getQuerySplitterIterator(Processor<T> processor) {
        if (Utilities.isFragmenterCacheEnabled()) {
            if (querySession.getQuerySplitList() == null) {
                synchronized (querySession) {
                    if (querySession.getQuerySplitList() == null) {
                        QuerySplitter splitter = processor.getQuerySplitter();
                        querySession.setQuerySplitList(Lists.newArrayList(splitter));
                    }
                }
            }
            return querySession.getQuerySplitList().iterator();
        } else {
            return processor.getQuerySplitter();
        }
    }
}