package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamingCsvValueHandler extends BaseCsvValueHandler<InputStream> {

    @Override
    protected void internalHandle(OutputStream buffer, InputStream value) throws IOException {
        throw new UnsupportedOperationException();
    }
}
