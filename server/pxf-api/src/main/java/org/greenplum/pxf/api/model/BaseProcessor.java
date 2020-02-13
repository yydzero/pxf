package org.greenplum.pxf.api.model;

import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.ExecutorServiceProvider;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SerializerFactory;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BaseProcessor<T> extends BasePlugin implements Processor<T> {

    private static final int THRESHOLD = 2;

    private final SerializerFactory serializerFactory;
    private QuerySession<T> querySession;

    /**
     * Main lock guarding all access
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * Main lock guarding all access
     */
//    final ReentrantLock writeLock = new ReentrantLock();

    /**
     * Condition for waiting takes
     */
    private final Condition moreTasks = lock.newCondition();

    public BaseProcessor() {
        this(SerializerFactory.getInstance());
    }

    public BaseProcessor(SerializerFactory serializerFactory) {
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
        int splitsProcessed = 0;
        final AtomicInteger activeTaskCount = new AtomicInteger();
        final AtomicLong recordCount = new AtomicLong();
        final String resource = context.getDataSource();
        final ExecutorService executor = ExecutorServiceProvider.get(context);
        final QuerySplitter splitter = getQuerySplitter();
        final int threshold = configuration.getInt("pxf.query.active.task.threshold", THRESHOLD);
        splitter.initialize(context);

        LOG.info("{}-{}: {}-- Starting session for query {}", context.getTransactionId(),
                context.getSegmentId(), context.getDataSource(), querySession);

        try (Serializer serializer = serializerFactory.getSerializer(context)) {
            serializer.open(output);

            while (querySession.isActive()) {
                // we need to submit more work only if the output queue has slots available
                while (splitter.hasNext() && querySession.isActive() && activeTaskCount.get() < threshold) {
                    // Queue more work
                    QuerySplit split = splitter.next();

                    if (doesSegmentProcessThisSplit(split)) {
                        splitsProcessed++;
                        LOG.info("{}-{}: {}-- Submitting {} to the pool", context.getTransactionId(),
                                context.getSegmentId(), context.getDataSource(), getUniqueResourceName(split));
                        // Increase the number of jobs submitted to the executor
                        activeTaskCount.incrementAndGet();

                        executor.submit(() -> {
                            Iterator<T> iterator = null;
                            try {
                                iterator = readTuples(split);
                            } catch (IOException e) {
                                querySession.errorQuery();
                                LOG.info(String.format("%s-%d: %s-- processing was interrupted",
                                        context.getTransactionId(),
                                        context.getSegmentId(),
                                        context.getDataSource()), e);
                            }
                            if (iterator != null) {
                                int minBufferSize = 5;//, maxBufferSize = 15, bufferSize;
                                List<T> miniBuffer = new ArrayList<>(minBufferSize);

                                while (iterator.hasNext() && querySession.isActive()) {
                                    miniBuffer.add(iterator.next());
//                                    bufferSize = miniBuffer.size();
//                                    if (((bufferSize >= minBufferSize && bufferSize < maxBufferSize) && writeLock.tryLock()) ||
//                                            bufferSize == maxBufferSize) {
//                                        if (bufferSize == maxBufferSize) writeLock.lock();
                                    if (miniBuffer.size() == minBufferSize) {
                                        try {
                                            flushBuffer(serializer, miniBuffer);
                                            recordCount.addAndGet(miniBuffer.size());
                                        } catch (IOException e) {
                                            querySession.errorQuery();
                                            LOG.info(String.format("%s-%d: %s-- processing was interrupted",
                                                    context.getTransactionId(),
                                                    context.getSegmentId(),
                                                    context.getDataSource()), e);
                                            break;
                                        } finally {
//                                            writeLock.unlock();
                                            miniBuffer.clear();
                                        }
                                    }
                                }
                                if (querySession.isActive()) {
                                    try {
//                                        writeLock.lock();
                                        flushBuffer(serializer, miniBuffer);
                                        recordCount.addAndGet(miniBuffer.size());
                                    } catch (IOException e) {
                                        querySession.errorQuery();
                                        LOG.info(String.format("%s-%d: %s-- processing was interrupted",
                                                context.getTransactionId(),
                                                context.getSegmentId(),
                                                context.getDataSource()), e);
                                    }
//                                    finally {
//                                        writeLock.unlock();
//                                    }
                                }
                                miniBuffer.clear();
                            }
                            // Decrease the number of jobs after completing
                            // processing the split
                            activeTaskCount.decrementAndGet();
                            requestMoreTasks();
                            LOG.info("{}-{}: {}-- Completed processing {}", context.getTransactionId(),
                                    context.getSegmentId(), context.getDataSource(), getUniqueResourceName(split));
                        });
                    }
                }

                if (activeTaskCount.get() == 0 && !splitter.hasNext()) {
                    break;
                }

                /* Block until more tasks are requested */
                waitForMoreTasks();
            }

            querySession.deregisterSegment(context.getSegmentId());

            LOG.info("{}-{}: {}-- Finished streaming {} record{} for resource {}. Processed {} split{}",
                    context.getTransactionId(), context.getSegmentId(),
                    context.getDataSource(), recordCount.get(),
                    recordCount.get() == 1 ? "" : "s", resource,
                    splitsProcessed,
                    splitsProcessed == 1 ? "" : "s");
        } catch (ClientAbortException e) {
            querySession.cancelQuery();
            // Occurs whenever client (Greenplum) decides to end the connection
            if (LOG.isDebugEnabled()) {
                // Stacktrace in debug
                LOG.debug("Remote connection closed by GPDB", e);
            } else {
                LOG.error("{}-{}: {}-- Remote connection closed by GPDB (Enable debug for stacktrace)", context.getTransactionId(),
                        context.getSegmentId(), context.getDataSource());
            }
        } catch (Exception e) {
            querySession.errorQuery();
            throw new IOException(e.getMessage(), e);
        } finally {
            LOG.debug("Stopped streaming for resource {}, {} records.", resource, recordCount);
        }
    }

    public void requestMoreTasks() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            moreTasks.signal();
        } finally {
            lock.unlock();
        }
    }

    private void waitForMoreTasks() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            LOG.debug("Waiting for more tasks");
            moreTasks.await();
        } finally {
            lock.unlock();
        }
    }

    private void flushBuffer(Serializer serializer, List<T> miniBuffer) throws IOException {
        if (miniBuffer.isEmpty()) return;
        synchronized (serializer) {
            for (T t : miniBuffer)
                writeTuple(serializer, t);
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
        int i = 0;
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        Iterator<Object> fieldsIterator = getFields(row);

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
     * Determine whether this thread will handle the split. To determine
     * which thread should process an element at a given index I for the source
     * SOURCE_NAME, use a MOD function
     * <p>
     * S = MOD(hash(SOURCE_NAME), N)
     *
     * <p>This hash function is deterministic for a given SOURCE_NAME, and allows
     * the same thread processing for segment S to always process the same
     * source. This allows for caching the Fragment at the segment S, as
     * segment S is guaranteed to always process the same split.
     *
     * @param split the split
     * @return true if the thread handles the split, false otherwise
     */
    protected boolean doesSegmentProcessThisSplit(QuerySplit split) {
        // TODO: use a consistent hash algorithm here, for when the total segments is elastic
        return context.getSegmentId() == Math.floorMod(Objects.hash(getUniqueResourceName(split)), context.getTotalSegments());
    }

    protected abstract String getUniqueResourceName(QuerySplit split);

    /**
     * Process the current split and return an iterator to retrieve rows
     * from the external system.
     *
     * @param split the split
     * @return an iterator of rows of type T
     */
    protected abstract Iterator<T> readTuples(QuerySplit split) throws IOException;

    /**
     * Return a list of fields for the the row
     *
     * @param row the row
     * @return the list of fields for the given row
     */
    protected abstract Iterator<Object> getFields(T row);
}
