package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseQuerySplitter;
import org.greenplum.pxf.api.model.QuerySplit;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Splits resources on Hadoop-compatible FileSystem (HCFS).
 *
 * <p>Given a HCFS data source (file, directory, wildcard pattern), split the
 * data into {@link QuerySplit}s and return an iterator. The QuerySplit returns
 * a resource, which is a relative path to the source. The base path is
 * stripped from the resource because it is redundant and available later for
 * retrieval. The QuerySplit also returns information about the split start
 * and split length.
 */
public class HcfsDataSplitter extends BaseQuerySplitter {

    protected JobConf jobConf;
    protected HcfsType hcfsType;

    private Iterator<InputSplit> inputSplitIterator;

    private int basePathLength;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, context);
        jobConf = new JobConf(configuration, this.getClass());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (inputSplitIterator == null) {
            Path path = new Path(hcfsType.getDataUri(jobConf, context));
            basePathLength = path.toString().length();
            try {
                inputSplitIterator = getSplits(path);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Unable to retrieve splits for path %s", path.toString()), e);
            }

            if (inputSplitIterator == null) {
                // There are no splits
                return false;
            }
        }
        return inputSplitIterator.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QuerySplit next() {
        FileSplit fsp = (FileSplit) inputSplitIterator.next();
        String resource = fsp.getPath().toString().substring(basePathLength);

        /* metadata information includes: file split's start and length */
        byte[] fragmentMetadata = serializeFragmentMetadata(fsp);
        return new QuerySplit(resource, fragmentMetadata);
    }

    /**
     * Returns an {@link InputSplit} iterator for the given path
     *
     * @param path the path
     * @return an {@link InputSplit} iterator
     * @throws IOException when {@link FileInputFormat#getSplits(JobConf, int)} returns an IOException
     */
    protected Iterator<InputSplit> getSplits(Path path) throws IOException {
        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);
        final InputSplit[] splits = pxfInputFormat.getSplits(jobConf, 1);

        if (splits == null) {
            return null;
        }

        LOG.info("{}-{}: {}-- Total number of splits = {}",
                context.getTransactionId(), context.getSegmentId(),
                context.getDataSource(), splits.length);

        return new Iterator<InputSplit>() {
            private int currentSplit = 0;

            @Override
            public boolean hasNext() {
                return currentSplit < splits.length;
            }

            @Override
            public InputSplit next() {
                return splits[currentSplit++];
            }
        };
    }

    /**
     * Serialize the split start and length information into a byte array
     *
     * @param fileSplit the file split information
     * @return a byte array that encapsulates the serialized split start and length information
     */
    private byte[] serializeFragmentMetadata(FileSplit fileSplit) {
        ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
        buffer.putLong(fileSplit.getStart());
        buffer.putLong(fileSplit.getLength());
        return buffer.array();
    }
}
