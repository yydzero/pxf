package org.greenplum.pxf.api.model;

public interface StreamingResolver extends Resolver {
    String getNext();

    boolean hasNext();
}
