package org.greenplum.pxf.api.model;

import com.google.common.collect.Lists;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SerializerFactory;
import org.greenplum.pxf.api.utilities.Utilities;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseProcessor<T> extends BasePlugin implements Processor<T> {

    /**
     * A factory for serializers
     */
    private final SerializerFactory serializerFactory;

    /**
     * A query session shared among all segments participating in this query
     */
    protected QuerySession<T> querySession;

    public BaseProcessor() {
        this(SerializerFactory.getInstance());
    }

    BaseProcessor(SerializerFactory serializerFactory) {
        this.serializerFactory = serializerFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQuerySession(QuerySession<T> querySession) {
        this.querySession = querySession;
    }

    /**
     * Write the processed fragments to the output stream in the desired
     * wire output format
     *
     * @param output the output stream
     * @throws IOException             when an IOException occurs
     * @throws WebApplicationException when a WebApplicationException occurs
     */
    @Override
    public void write(OutputStream output) throws IOException, WebApplicationException {
        int recordCount = 0;
        final String resource = context.getDataSource();
        final Iterator<QuerySplit> splitter = getQuerySplitterIterator();
        final int segmentId = querySession.nextSegmentId();

        LOG.info("{}-{}: {}-- Starting session for query {}", context.getTransactionId(),
                segmentId, context.getDataSource(), querySession);

        BlockingDeque<List<T>> outputQueue = querySession.getOutputQueue();

        try (Serializer serializer = serializerFactory.getSerializer(context)) {
            serializer.open(output);

            querySession.registerQuerySplitter(segmentId, splitter);

            while (!querySession.hasStartedProducing() && querySession.isActive()) {
                // wait until producer has started producing
                querySession.waitUntilTaskStartProcessing(100, TimeUnit.MILLISECONDS);
            }

            // querySession.isActive determines whether an error or cancellation of the query occurred
            while (querySession.isActive()) {

                // Exit if there is no more work to process
                if (querySession.getRunningTasks() == 0 && outputQueue.isEmpty())
                    break;

                /* Block until more tasks are requested */
                List<T> tuples = outputQueue.poll(100, TimeUnit.MILLISECONDS);

                if (tuples == null) continue;

                for (T tuple : tuples) {
                    writeTuple(serializer, tuple);
                    recordCount++;
                }
            }
        } catch (ClientAbortException e) {
            querySession.cancelQuery();
            // Occurs whenever client (Greenplum) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.debug("Remote connection closed by client", e);
            } else {
                LOG.error("{}-{}: {}-- Remote connection closed by client (Enable debug for stacktrace)", context.getTransactionId(),
                        segmentId, context.getDataSource());
            }
        } catch (Exception e) {
            querySession.errorQuery();
            throw new IOException(e.getMessage(), e);
        } finally {
            querySession.deregisterSegment(segmentId);
            LOG.info("{}-{}: {}-- Stopped streaming {} record{} for resource {}",
                    context.getTransactionId(), segmentId,
                    context.getDataSource(), recordCount,
                    recordCount == 1 ? "" : "s", resource);
        }
    }

    /**
     * Write the tuple to the serializer. The method retrieves an array of
     * fields for the given tuple and serializes each field using information
     * from the column descriptor to determine the type of the field
     *
     * @param serializer the serializer for the output format
     * @param tuple      the tuple
     * @throws IOException when an IOException occurs
     */
    protected void writeTuple(Serializer serializer, T tuple) throws IOException {
        int i = 0;
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        Iterator<Object> fieldsIterator = getFields(tuple);

        serializer.startRow(tupleDescription.size());
        while (fieldsIterator.hasNext()) {
            ColumnDescriptor columnDescriptor = tupleDescription.get(i++);
            Object field = fieldsIterator.next();
            serializer.startField();
            serializer.writeField(columnDescriptor.getDataType(), field);
            serializer.endField();
        }
        serializer.endRow();
    }

    /**
     * Return a list of fields for the the row
     *
     * @param row the row
     * @return the list of fields for the given row
     */
    protected abstract Iterator<Object> getFields(T row);

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
    private Iterator<QuerySplit> getQuerySplitterIterator() {
        if (Utilities.isFragmenterCacheEnabled()) {
            // TODO: querySplit List needs volatile?
            if (querySession.getQuerySplitList() == null) {
                synchronized (querySession) {
                    if (querySession.getQuerySplitList() == null) {
                        LOG.error("retrieving fragments");
                        QuerySplitter splitter = getQuerySplitter();
                        splitter.initialize(context);
                        querySession.setQuerySplitList(Lists.newArrayList(splitter));
                        LOG.error("completed retrieving fragments");
                    }
                }
            }
            return querySession.getQuerySplitList().iterator();
        } else {
            QuerySplitter splitter = getQuerySplitter();
            splitter.initialize(context);
            return splitter;
        }
    }
}
