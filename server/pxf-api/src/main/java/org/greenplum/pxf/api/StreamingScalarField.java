package org.greenplum.pxf.api;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.model.StreamingResolver;

public class StreamingScalarField extends ScalarField {
    StreamingResolver resolver;
    GreenplumCSV greenplumCSV;

    public StreamingScalarField(StreamingResolver streamingResolver) {
        super(DataType.TEXT.getOID(), null);
        resolver = streamingResolver;
    }


    public String getPrefix() {
        return String.valueOf(greenplumCSV.getQuote());
    }

    public String getSuffix() {
        return String.valueOf(greenplumCSV.getQuote());
    }

    public StreamingResolver getResolver() {
        return resolver;
    }
}
