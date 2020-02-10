package org.greenplum.pxf.api.model;

import javax.ws.rs.core.StreamingOutput;

public interface Processor<T> extends Plugin, StreamingOutput {

    /**
     * Returns the query splitter for this {@link Processor}
     *
     * @return the query splitter
     */
    QuerySplitter getQuerySplitter();

    /**
     * Register the {@link QuerySession<T>} for the given query
     *
     * @param querySession state for the query
     */
    void setQuerySession(QuerySession<T> querySession);
}
