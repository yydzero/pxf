package org.greenplum.pxf.api;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.model.StreamingResolver;

public class StreamingArrayField extends ArrayField {
    StreamingResolver resolver;
    private GreenplumCSV greenplumCSV;

    public StreamingArrayField(StreamingResolver streamingResolver) {
        super(DataType.TEXTARRAY.getOID(), null);
        resolver = streamingResolver;
    }

    public String getPrefix() {
        return String.valueOf(greenplumCSV.getQuote()) + greenplumCSV.getOpenArray();
    }

    public String getSuffix() {
        return greenplumCSV.getCloseArray() + String.valueOf(greenplumCSV.getQuote());
    }

    public StreamingResolver getResolver() {
        return resolver;
    }

}
