package org.greenplum.pxf.api.model;

import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.util.Iterator;

public interface Processor<T> extends Plugin, StreamingOutput {

    /**
     * Returns the query splitter for this {@link Processor}
     *
     * @return the query splitter
     */
    QuerySplitter getQuerySplitter();

    /**
     * Process the current split and return an iterator to retrieve tuples
     * from the external system.
     *
     * @param split the split
     * @return an iterator of tuples of type T
     */
    Iterator<T> getTupleIterator(QuerySplit split) throws IOException;

    /**
     * Returns the segment ID for this processor
     *
     * @return the segment ID for this processor
     */
    int getSegmentId();
}
