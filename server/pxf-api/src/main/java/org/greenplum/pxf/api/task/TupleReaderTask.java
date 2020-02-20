package org.greenplum.pxf.api.task;

import org.apache.catalina.connector.ClientAbortException;
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
import java.util.concurrent.Callable;

/**
 * Processes a {@link QuerySplit} and generates 0 or more tuples. Stores
 * tuples in the buffer, until the buffer is full, then it adds the buffer to
 * the outputQueue.
 */
public class TupleReaderTask<T> implements Callable<Void> {

    private final Logger LOG = LoggerFactory.getLogger(TupleReaderTask.class);
    private final QuerySplit split;
    private final BlockingDeque<List<T>> outputQueue;
    private final QuerySession<T> querySession;
    private final String uniqueResourceName;
    private final Processor<T> processor;

    public TupleReaderTask(Processor<T> processor, QuerySplit split, QuerySession<T> querySession) {
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
    public Void call() {
        Iterator<T> iterator;
        // TODO: control the batch size through query param to see if we get better throughput
        int batchSize = 500, totalRows = 0;
        try {
            iterator = processor.getTupleIterator(split);
            List<T> batch = new ArrayList<>(batchSize);
            while (iterator.hasNext() && querySession.isActive()) {
                batch.add(iterator.next());
                if (batch.size() == batchSize) {
                    totalRows += batchSize;
                    outputQueue.put(batch);
                    batch = new ArrayList<>(batchSize);
                }
            }
            if (querySession.isActive() && !batch.isEmpty()) {
                totalRows += batch.size();
                outputQueue.put(batch);
            }
        } catch (ClientAbortException e) {
            querySession.cancelQuery(e);
        } catch (IOException e) {
            querySession.errorQuery(e);
            LOG.info(String.format("error while processing split %s for query %s",
                    uniqueResourceName, querySession), e);
        } catch (InterruptedException e) {
            querySession.errorQuery(e);
        }

        // Decrease the number of jobs after completing processing the split
        querySession.deregisterTask();

        // Keep track of the number of records processed by this task
        LOG.debug("completed processing {} row{} {} for query {}",
                totalRows, totalRows == 1 ? "" : "s", uniqueResourceName, querySession);
        return null;
    }
}
