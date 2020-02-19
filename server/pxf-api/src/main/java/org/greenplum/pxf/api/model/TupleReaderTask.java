package org.greenplum.pxf.api.model;

import org.apache.catalina.connector.ClientAbortException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Callable;

/**
 * Processes a {@link QuerySplit} and generates 0 or more tuples. Stores
 * tuples in the buffer, until the buffer is full, then it adds the buffer to
 * the outputQueue.
 */
public class TupleReaderTask<T> implements Callable<TupleReaderTask.Result> {

    private final Logger LOG = LoggerFactory.getLogger(TupleReaderTask.class);
    private final QuerySplit split;
    private final BlockingDeque<List<T>> outputQueue;
    private final QuerySession<T> querySession;
    private final String uniqueResourceName;
    private final Processor<T> processor;

    public TupleReaderTask(QuerySplit split, QuerySession<T> querySession) {
        this.split = split;
        this.querySession = querySession;
        this.outputQueue = querySession.getOutputQueue();
        this.uniqueResourceName = split.toString();
        this.processor = querySession.getProcessor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Result call() {
        Result result = new Result();
        Iterator<T> iterator;
        // TODO: control the batch size through query param to see if we get better throughput
        int batchSize = 1000;
        try {
            iterator = processor.getTupleIterator(split);
            List<T> batch = new ArrayList<>(batchSize);
            while (iterator.hasNext() && querySession.isActive()) {
                batch.add(iterator.next());
                if (batch.size() == batchSize) {
                    outputQueue.put(batch);
                    batch = new ArrayList<>(batchSize);
                }
            }
            if (querySession.isActive() && !batch.isEmpty()) {
                outputQueue.put(batch);
            }
        } catch (ClientAbortException e) {
            querySession.cancelQuery();
            result.addError(e);
        } catch (IOException e) {
            querySession.errorQuery();
            result.addError(e);
            LOG.info(String.format("error while processing split %s for query %s",
                    uniqueResourceName, querySession), e);
        } catch (InterruptedException e) {
            querySession.errorQuery();
        }

        // Decrease the number of jobs after completing processing the split
        querySession.deregisterTask();

        // Keep track of the number of records processed by this task
        LOG.debug("completed processing {} for query {}", uniqueResourceName, querySession);
        return result;
    }

    public static class Result {
        private List<IOException> errors;

        void addError(IOException ioe) {
            if (errors == null) {
                errors = new ArrayList<>();
            }
            errors.add(ioe);
        }
    }
}
