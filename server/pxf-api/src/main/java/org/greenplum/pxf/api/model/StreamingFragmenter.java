package org.greenplum.pxf.api.model;

import java.util.Iterator;

public interface StreamingFragmenter extends Fragmenter, Iterator<Fragment> {
    public int getChunkSize();
    public void setChunkSize(int chunkSize);
}
