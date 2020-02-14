package org.greenplum.pxf.api.model;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.catalina.connector.ClientAbortException;
import org.apache.commons.codec.binary.Hex;
import org.greenplum.pxf.api.ExecutorServiceProvider;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.SerializerFactory;
import org.greenplum.pxf.api.utilities.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
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

    /**
     * A factory for serializers
     */
    private final SerializerFactory serializerFactory;

    /**
     * A query session shared among all segments participating in this query
     */
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
        final Iterator<QuerySplit> splitter = getQuerySplitterIterator();
        final int threshold = configuration.getInt("pxf.query.active.task.threshold", THRESHOLD);

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

                    LOG.debug("{}-{}: {}-- Submitting {} to the pool", context.getTransactionId(),
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
                LOG.debug("Remote connection closed by client", e);
            } else {
                LOG.error("{}-{}: {}-- Remote connection closed by client (Enable debug for stacktrace)", context.getTransactionId(),
                        context.getSegmentId(), context.getDataSource());
            }
        } catch (Exception e) {
            querySession.errorQuery();
            throw new IOException(e.getMessage(), e);
        } finally {
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
     * Determine whether this thread will handle the split. To determine
     * which thread should process an element at a given index I for the source
     * SOURCE_NAME, use a MOD function
     * <p>
     * S = MOD(hash(SOURCE_NAME[+META_DATA[+USER_DATA]]), N)
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
        HashFunction hf = Hashing.goodFastHash(128); // 29 26 29
        Hasher hasher = hf.newHasher()
                .putString(split.getResource(), StandardCharsets.UTF_8);

        if (split.getMetadata() != null) {
            hasher = hasher.putBytes(split.getMetadata());
        }

        if (split.getUserData() != null) {
            hasher = hasher.putBytes(split.getUserData());
        }
        return context.getSegmentId() == Hashing.consistentHash(hasher.hash(), context.getTotalSegments());
    }

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
            if (querySession.getQuerySplitList() == null) {
                synchronized (querySession) {
                    if (querySession.getQuerySplitList() == null) {
                        QuerySplitter splitter = getQuerySplitter();
                        splitter.initialize(context);
                        querySession.setQuerySplitList(Lists.newArrayList(splitter));
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
     * Returns a unique resource name for the given split
     *
     * @param split the split
     * @return a unique resource name for the given split
     */
    private String getUniqueResourceName(QuerySplit split) {
        StringBuilder sb = new StringBuilder();
        sb.append(split.getResource());

        if (split.getMetadata() != null) {
            sb.append(":").append(Hex.encodeHex(split.getMetadata()));
        }

        if (split.getUserData() != null) {
            sb.append(":").append(Hex.encodeHex(split.getUserData()));
        }

        return sb.toString();
    }

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
            Iterator<T> iterator;
            int recordCount = 0, minBufferSize = 5;
            try {
                iterator = processor.readTuples(split);
                List<T> miniBuffer = new ArrayList<>(minBufferSize);
                while (iterator.hasNext() && querySession.isActive()) {
                    miniBuffer.add(iterator.next());
                    if (miniBuffer.size() == minBufferSize) {
                        recordCount += flushBuffer(serializer, miniBuffer);
                    }
                }
                if (querySession.isActive()) {
                    // flush the rest of the buffer
                    recordCount += flushBuffer(serializer, miniBuffer);
                }
            } catch (ClientAbortException e) {
                querySession.cancelQuery();
                result.addError(e);
            } catch (IOException e) {
                querySession.errorQuery();
                result.addError(e);
                LOG.info(String.format("%s-%d: %s-- error while processing split %s for query %s",
                        context.getTransactionId(),
                        context.getSegmentId(),
                        context.getDataSource(),
                        split.getResource(),
                        querySession), e);
            }

            // Decrease the number of jobs after completing processing the split
            runningTasks.decrementAndGet();
            // Keep track of the number of records processed by this task
            result.recordCount = recordCount;
            LOG.debug("{}-{}: {}-- Completed processing {}", context.getTransactionId(),
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
         * @return the number or tuples written
         * @throws IOException when an IOException occurs
         */
        private int flushBuffer(Serializer serializer, List<T> miniBuffer) throws IOException {
            if (miniBuffer.isEmpty()) return 0;
            synchronized (serializer) {
                for (T tuple : miniBuffer)
                    processor.writeTuple(serializer, tuple);
            }
            int count = miniBuffer.size();
            miniBuffer.clear();
            return count;
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
