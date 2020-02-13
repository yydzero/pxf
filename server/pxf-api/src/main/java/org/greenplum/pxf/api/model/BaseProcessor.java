package org.greenplum.pxf.api.model;

import org.apache.catalina.connector.ClientAbortException;
import org.greenplum.pxf.api.ExecutorServiceProvider;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SerializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BaseProcessor<T> extends BasePlugin implements Processor<T> {

    private static final int THRESHOLD = 2;

    private final SerializerFactory serializerFactory;
    private QuerySession<T> querySession;

    /**
     * Tracks number of active tasks
     */
    final AtomicInteger runningTasks = new AtomicInteger();

    /**
     * Main lock guarding all access
     */
    final ReentrantLock lock = new ReentrantLock();

    /**
     * Condition for waiting for more tasks
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
        int splitsProcessed = 0, recordCount = 0;
        final String resource = context.getDataSource();
        final ExecutorService executor = ExecutorServiceProvider.get(context);
        final QuerySplitter splitter = getQuerySplitter();
        final int threshold = configuration.getInt("pxf.query.active.task.threshold", THRESHOLD);
        splitter.initialize(context);

        LOG.info("{}-{}: {}-- Starting session for query {}", context.getTransactionId(),
                context.getSegmentId(), context.getDataSource(), querySession);

        List<Future<ProcessQuerySplitCallable.Result>> futures = new ArrayList<>();

        try (Serializer serializer = serializerFactory.getSerializer(context)) {
            serializer.open(output);

            // querySession.isActive determines whether an error or cancellation of the query occurred
            while (querySession.isActive()) {
                // we need to submit more work only if we are under the max threshold
                while (splitter.hasNext() && querySession.isActive() && runningTasks.get() < threshold) {
                    QuerySplit split = splitter.next();
                    // skip if this thread does not process the split
                    if (!doesSegmentProcessThisSplit(split)) continue;

                    LOG.info("{}-{}: {}-- Submitting {} to the pool", context.getTransactionId(),
                            context.getSegmentId(), context.getDataSource(), getUniqueResourceName(split));

                    // Keep track of the number of splits processed
                    splitsProcessed++;
                    // Increase the number of jobs submitted to the executor
                    runningTasks.incrementAndGet();
                    // Submit more work
                    futures.add(executor
                            .submit(new ProcessQuerySplitCallable(split, serializer, this)));
                }

                // Exit if there is no more work to process
                if (runningTasks.get() == 0 && !splitter.hasNext()) break;

                /* Block until more tasks are requested */
                waitForMoreTasks();
            }

            if (querySession.isActive()) {
                IOException exception = null;
                for (Future<ProcessQuerySplitCallable.Result> f : futures) {
                    ProcessQuerySplitCallable.Result result = f.get();
                    if (result.errors != null) {
                        for (IOException ex : result.errors) {
                            if (exception == null) {
                                exception = ex;
                            }
                            LOG.error("Error while processing", ex);
                        }
                    }
                    // collect total rows processed
                    recordCount += result.recordCount;
                }

                if (exception != null) {
                    // Throw first error if present
                    throw exception;
                }
            }

            querySession.deregisterSegment(context.getSegmentId());
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
            cleanup();
            LOG.info("{}-{}: {}-- Stopped streaming {} record{} for resource {}. Processed {} split{}",
                    context.getTransactionId(), context.getSegmentId(),
                    context.getDataSource(), recordCount,
                    recordCount == 1 ? "" : "s", resource,
                    splitsProcessed,
                    splitsProcessed == 1 ? "" : "s");
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

    /**
     * Waits until the moreTasks is signaled
     *
     * @throws InterruptedException when an InterruptedException occurs
     */
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

    /**
     * Signals the moreTasks condition for the waiting threads
     */
    private void requestMoreTasks() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            moreTasks.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Do any cleanup
     */
    protected void cleanup() {

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

    /**
     * Processes a {@link QuerySplit} and generates 0 or more tuples. Stores
     * a mini buffer of tuples and flushes the buffer to the serializer when
     * the buffer is full or processing has completed.
     */
    private class ProcessQuerySplitCallable implements
            Callable<ProcessQuerySplitCallable.Result> {

        private final Logger LOG = LoggerFactory.getLogger(ProcessQuerySplitCallable.class);
        private final QuerySplit split;
        private final Serializer serializer;
        private final BaseProcessor<T> processor;

        public ProcessQuerySplitCallable(QuerySplit split,
                                         Serializer serializer,
                                         BaseProcessor<T> processor) {
            this.split = split;
            this.serializer = serializer;
            this.processor = processor;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Result call() {
            Result result = new Result();
            Iterator<T> iterator = null;
            try {
                iterator = processor.readTuples(split);
            } catch (IOException e) {
                querySession.errorQuery();
                result.addError(e);
                LOG.info(String.format("%s-%d: %s-- processing was interrupted",
                        context.getTransactionId(),
                        context.getSegmentId(),
                        context.getDataSource()), e);
            }

            if (iterator == null) {
                // Decrease the number of jobs after completing
                // processing the split
                runningTasks.decrementAndGet();
                requestMoreTasks();
                LOG.info("{}-{}: {}-- Completed processing {}", context.getTransactionId(),
                        context.getSegmentId(), context.getDataSource(), getUniqueResourceName(split));
                return result;
            }

            int minBufferSize = 5, recordCount = 0;
            List<T> miniBuffer = new ArrayList<>(minBufferSize);

            while (iterator.hasNext() && querySession.isActive()) {
                miniBuffer.add(iterator.next());
                if (miniBuffer.size() == minBufferSize) {
                    try {
                        flushBuffer(serializer, miniBuffer);
                        recordCount += miniBuffer.size();
                    } catch (IOException e) {
                        querySession.errorQuery();
                        result.addError(e);
                        LOG.info(String.format("%s-%d: %s-- processing was interrupted",
                                context.getTransactionId(),
                                context.getSegmentId(),
                                context.getDataSource()), e);
                        break;
                    } finally {
                        miniBuffer.clear();
                    }
                }
            }
            if (querySession.isActive()) {
                try {
                    flushBuffer(serializer, miniBuffer);
                    recordCount += miniBuffer.size();
                } catch (IOException e) {
                    querySession.errorQuery();
                    result.addError(e);
                    LOG.info(String.format("%s-%d: %s-- processing was interrupted",
                            context.getTransactionId(),
                            context.getSegmentId(),
                            context.getDataSource()), e);
                }
            }
            miniBuffer.clear();
            // Decrease the number of jobs after completing processing the split
            runningTasks.decrementAndGet();
            // Keep track of the number of records processed by this task
            result.recordCount = recordCount;
            LOG.info("{}-{}: {}-- Completed processing {}", context.getTransactionId(),
                    context.getSegmentId(), context.getDataSource(), getUniqueResourceName(split));
            // Signal for more tasks
            requestMoreTasks();
            return result;
        }

        /**
         * Write all the tuples in the miniBuffer to the serializer
         *
         * @param serializer the serializer
         * @param miniBuffer the buffer
         * @throws IOException when an IOException occurs
         */
        private void flushBuffer(Serializer serializer, List<T> miniBuffer) throws IOException {
            if (miniBuffer.isEmpty()) return;
            synchronized (serializer) {
                for (T tuple : miniBuffer)
                    processor.writeTuple(serializer, tuple);
            }
        }

        private class Result {
            private List<IOException> errors;
            private int recordCount;

            void addError(IOException ioe) {
                if (errors == null) {
                    errors = new ArrayList<>();
                }
                errors.add(ioe);
            }
        }
    }
}
