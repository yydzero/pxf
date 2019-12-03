package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;

import java.util.List;

public interface BatchResolver extends Resolver {

    List<OneField> startBatch(OneRow row);

    byte[] getNextBatchedItem(OneRow row);
}
