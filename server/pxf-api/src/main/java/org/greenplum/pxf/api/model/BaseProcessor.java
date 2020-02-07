package org.greenplum.pxf.api.model;

import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.ExecutorServiceProvider;
import org.greenplum.pxf.api.concurrent.TaskAwareBlockingQueue;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.FragmenterFactory;
import org.greenplum.pxf.api.utilities.SerializerFactory;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public abstract class BaseProcessor<T> extends BasePlugin implements Processor<T> {

    private static final int THRESHOLD = 2;
    private final FragmenterFactory fragmenterFactory;
    private final SerializerFactory serializerFactory;

    private TaskAwareBlockingQueue<T> outputQueue;

    protected Fragmenter fragmenter;

    public BaseProcessor() {
        this(FragmenterFactory.getInstance(), SerializerFactory.getInstance());
    }

    public BaseProcessor(FragmenterFactory fragmenterFactory, SerializerFactory serializerFactory) {
        this.fragmenterFactory = fragmenterFactory;
        this.serializerFactory = serializerFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        this.fragmenter = fragmenterFactory.getPlugin(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setOutputQueue(TaskAwareBlockingQueue<T> outputQueue) {
        this.outputQueue = outputQueue;
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
        long recordCount = 0;
        final String resource = context.getDataSource();
        final ExecutorService executor = ExecutorServiceProvider.get(context);

        try (Serializer serializer = serializerFactory.getSerializer(context)) {
            serializer.open(output);

            // receive a common output queue from the controller resource
            // submit one task per fragment that this thread will work on to the threadpool
            // poll the output queue for results

            // we need to submit more work only if the output queue has slots available
            while (true) {
                while (outputQueue.activeTaskCount() < THRESHOLD && fragmenter.hasNext()) {
                    // Queue more work
                    Fragment fragment = fragmenter.next();

                    if (doesSegmentProcessThisFragment(fragment)) {
                        // Increase the number of jobs submitted to the executor
                        outputQueue.registerTask();
                        executor.submit(() -> {
                            // TODO: handle errors
                            Iterator<T> iterator = processFragment(fragment);
                            while (iterator.hasNext()) {
                                outputQueue.push(iterator.next());
                            }
                            // Decrease the number of jobs after completing
                            // processing the fragment
                            outputQueue.deregisterTask();
                        });
                    }
                }

                if (!outputQueue.hasPendingTasks() && outputQueue.isEmpty())
                    break; // no more work to do

                // output queue is a blocking queue
                writeTuple(serializer, outputQueue.take());
                recordCount++;
            }

            LOG.debug("Finished streaming {} record{} for resource {}", recordCount, recordCount == 1 ? "" : "s", resource);
        } catch (ClientAbortException e) {
            // Occurs whenever client (Greenplum) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.debug("Remote connection closed by GPDB", e);
            } else {
                LOG.error("Remote connection closed by GPDB (Enable debug for stacktrace)");
            }
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        } finally {
            LOG.debug("Stopped streaming for resource {}, {} records.", resource, recordCount);
        }
    }

    /**
     * Write the tuple to the serializer. The method retrieves an array of
     * fields for the given row and serializes each field using information
     * from the column descriptor to determine the type of the field
     *
     * @param serializer the serializer for the output format
     * @param row        the row
     * @throws IOException when an IOException occurs
     */
    protected void writeTuple(Serializer serializer, T row) throws IOException {
        Object[] fields = getFields(row);

        serializer.startRow(fields.length);
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        for (int i = 0; i < tupleDescription.size(); i++) {
            ColumnDescriptor columnDescriptor = tupleDescription.get(i);
            Object field = fields[i];
            serializer.startField();
            serializer.writeField(columnDescriptor.getDataType(), field);
            serializer.endField();
        }
        serializer.endRow();
    }

    /**
     * Determine whether this thread will handle the fragment. To determine
     * which thread should process an element at a given index I for the source
     * SOURCE_NAME, use a MOD function
     * <p>
     * S = MOD( hash(SOURCE_NAME), N)
     *
     * <p>This hash function is deterministic for a given SOURCE_NAME, and allows
     * the same thread processing for segment S to always process the same
     * source. This allows for caching the Fragment at the segment S, as
     * segment S is guaranteed to always process the same fragment.
     *
     * @param fragment the fragment
     * @return true if the thread handles the fragment, false otherwise
     */
    protected boolean doesSegmentProcessThisFragment(Fragment fragment) {
        // TODO: use a consistent hash algorithm here, for when the total segments is elastic
        return context.getSegmentId() == fragment.getSourceName().hashCode() % context.getTotalSegments();
    }

    /**
     * Process the current fragment and return an iterator to retrieve rows
     * from the external system.
     *
     * @param fragment the fragment
     * @return an iterator of rows of type T
     */
    protected abstract Iterator<T> processFragment(Fragment fragment);

    /**
     * Return a list of fields for the the row
     *
     * @param row the row
     * @return the list of fields for the given row
     */
    protected abstract Object[] getFields(T row);
}
