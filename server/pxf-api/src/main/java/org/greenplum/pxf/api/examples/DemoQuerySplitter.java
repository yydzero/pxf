package org.greenplum.pxf.api.examples;

import org.greenplum.pxf.api.model.BaseQuerySplitter;
import org.greenplum.pxf.api.model.QuerySplit;

/**
 * Class that defines the splitting of a data resource into splits that can
 * be processed in parallel. next() returns the split information of a given
 * path (resource and location of each split).
 *
 * <p>Demo implementation
 */
public class DemoQuerySplitter extends BaseQuerySplitter {
    private static final int TOTAL_FRAGMENTS = 3;
    private int currentFragment = 1;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return currentFragment <= TOTAL_FRAGMENTS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QuerySplit next() {
        QuerySplit split = new QuerySplit(context.getDataSource() + "." + currentFragment,
                ("fragment" + currentFragment).getBytes());
        currentFragment++;
        return split;
    }
}
