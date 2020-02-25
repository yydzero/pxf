package org.greenplum.pxf.api.task;

import com.google.common.collect.Lists;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.ExecutorServiceProvider;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.QuerySession;
import org.greenplum.pxf.api.model.QuerySplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;

/**
 * Processes a {@link QuerySplit} and generates 0 or more tuples. Stores
 * tuples in the buffer, until the buffer is full, then it adds the buffer to
 * the outputQueue.
 */
public class TupleReaderTask<T, M> implements Runnable, Comparable<TupleReaderTask> {

//implements Callable<Void>, Comparable<Runnable> {

    private final Logger LOG = LoggerFactory.getLogger(TupleReaderTask.class);
    private final QuerySplit split;
    private final BlockingDeque<List<List<Object>>> outputQueue;
    private final QuerySession<T, M> querySession;
    private final String uniqueResourceName;
    private final Processor<T> processor;
    private Iterator<T> iterator;
    private List<List<Object>> suspendedBatch;
    private final ExecutorService executor = ExecutorServiceProvider.get();

    public TupleReaderTask(Processor<T> processor, QuerySplit split, QuerySession<T, M> querySession) {
        this.split = split;
        this.querySession = querySession;
        this.outputQueue = querySession.getOutputQueue();
        this.uniqueResourceName = split.toString();
        this.processor = processor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
//    public Void call() {
    public void run() {

        // TODO: control the batch size through query param to see if we get better throughput
        int batchSize = 250, totalRows = 0;
        boolean suspendExecution = false;
        try {
            if (iterator == null) {
                iterator = processor.getTupleIterator(split);
            }

            if (suspendedBatch != null) {
                if (!addBatch(suspendedBatch)) {
                    suspendExecution = true;
                    return;
                } else {
                    suspendedBatch = null;
                }
            }
            List<List<Object>> batch = new ArrayList<>(batchSize);
            while (iterator.hasNext() && querySession.isActive()) {
                T tuple = iterator.next();
                List<Object> fields = Lists.newArrayList(processor.getFields(tuple));
                batch.add(fields);
                if (batch.size() == batchSize) {
                    totalRows += batchSize;
                    if (!addBatch(batch)) {
                        suspendExecution = true;
                        break;
                    }
                    outputQueue.put(batch);
                    batch = new ArrayList<>(batchSize);
                }
            }
            if (querySession.isActive() && !batch.isEmpty()) {
                totalRows += batch.size();
                if (!addBatch(batch)) {
                    suspendExecution = true;
                }
            }
        } catch (ClientAbortException e) {
            querySession.cancelQuery(e);
        } catch (IOException e) {
            querySession.errorQuery(e);
            LOG.info(String.format("error while processing split %s for query %s",
                    uniqueResourceName, querySession), e);
        } catch (InterruptedException e) {
            querySession.errorQuery(e);
        } finally {
            if (!suspendExecution) {
                querySession.registerCompletedTask();
            }
        }

        // Keep track of the number of records processed by this task
        LOG.debug("completed processing {} row{} {} for query {}",
                totalRows, totalRows == 1 ? "" : "s", uniqueResourceName, querySession);
//        return null;
    }

    private boolean addBatch(List<List<Object>> batch) {
        try {
            outputQueue.add(batch);
            return true;
        } catch (IllegalStateException e) {
            LOG.debug("Output queue is full", e);
            suspendedBatch = batch;
            executor.submit(this);
        }
        return false;
    }

    @Override
    public int compareTo(TupleReaderTask o) {
        return Integer.compare(this.outputQueue.size(), o.outputQueue.size());
    }
}
