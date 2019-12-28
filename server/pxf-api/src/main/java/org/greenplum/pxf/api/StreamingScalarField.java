package org.greenplum.pxf.api;

import org.greenplum.pxf.api.model.StreamingResolver;

public class StreamingScalarField extends ScalarField {
    StreamingResolver resolver;

    public StreamingScalarField(StreamingResolver streamingResolver) {
        resolver = streamingResolver;
    }

    public StreamingResolver getResolver() {
        return resolver;
    }

    public boolean hasNext() {
        return resolver.hasNext();
    }
}
