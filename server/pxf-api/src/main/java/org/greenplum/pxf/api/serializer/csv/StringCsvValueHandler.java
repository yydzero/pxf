package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StringCsvValueHandler extends BaseCsvValueHandler<String> {

    @Override
    protected void internalHandle(OutputStream buffer, String value) throws IOException {
        buffer.write(value.getBytes(StandardCharsets.UTF_8));
    }
}
