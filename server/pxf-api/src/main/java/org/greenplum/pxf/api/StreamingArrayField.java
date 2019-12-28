package org.greenplum.pxf.api;

import org.greenplum.pxf.api.model.StreamingResolver;

public class StreamingArrayField extends ArrayField {
    StreamingResolver resolver;

    public StreamingArrayField(StreamingResolver streamingResolver) {
        resolver = streamingResolver;
    }

    public StreamingResolver getResolver() {
        return resolver;
    }

    public boolean hasNext() {
        return resolver.hasNext();
    }
}
