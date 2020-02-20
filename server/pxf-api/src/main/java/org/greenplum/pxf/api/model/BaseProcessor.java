package org.greenplum.pxf.api.model;

import com.google.common.collect.Lists;
import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SerializerFactory;
import org.greenplum.pxf.api.utilities.Utilities;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Deque;
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

    /**
     * Default constructor. Initializes with the singleton instance of the
     * {@link SerializerFactory}
     */
    public BaseProcessor() {
        this(SerializerFactory.getInstance());
    }

    /**
     * Constructs a BaseProcessor with the given {@link SerializerFactory}
     * instance
     *
     * @param serializerFactory the SerializerFactory instance
     */
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
     * {@inheritDoc}
     */
    @Override
    public int getSegmentId() {
        return context.getSegmentId();
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

        LOG.info("{}-{}-- Starting streaming for {}", context.getTransactionId(), context.getSegmentId(), querySession);

        BlockingDeque<List<T>> outputQueue = querySession.getOutputQueue();
        try (Serializer serializer = serializerFactory.getSerializer(context)) {
            serializer.open(output);
            querySession.registerProcessor(this);

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

//            if (!querySession.isActive()) {
//                Exception firstException = null;
//                Deque<Exception> errorQueue = querySession.getErrors();
//
//                while (!errorQueue.isEmpty()) {
//                    Exception
//                }
//            }

        } catch (ClientAbortException e) {
            // Occurs whenever client (Greenplum) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.debug("Remote connection closed by client", e);
            } else {
                LOG.error("{}-{}: {}-- Remote connection closed by client (Enable debug for stacktrace)", context.getTransactionId(),
                        context.getSegmentId(), context.getDataSource());
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            querySession.deregisterSegment(context.getSegmentId());
            LOG.info("{}-{}-- Stopped streaming {} record{} for {}",
                    context.getTransactionId(), context.getSegmentId(), recordCount,
                    recordCount == 1 ? "" : "s", querySession);
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
     * Return a list of fields for the the tuple
     *
     * @param tuple the tuple
     * @return the list of fields for the given tuple
     */
    protected abstract Iterator<Object> getFields(T tuple);
}
