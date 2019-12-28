package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;

import java.util.List;

public interface StreamingResolver extends Resolver {
    String getNext();

    boolean hasNext();
}
