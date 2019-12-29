package org.greenplum.pxf.api;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.StreamingResolver;

public class StreamingArrayField extends ArrayField {
    StreamingResolver resolver;

    public StreamingArrayField(StreamingResolver streamingResolver) {
        super(DataType.TEXTARRAY.getOID(), null);
        resolver = streamingResolver;
    }

    public StreamingResolver getResolver() {
        return resolver;
    }

    public boolean hasNext() {
        return resolver.hasNext();
    }
}
