package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class ObjectCsvValueHandler extends BaseCsvValueHandler<Object> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void internalHandle(OutputStream buffer, Object value) throws IOException {
        buffer.write(Objects.toString(value, null).getBytes(StandardCharsets.UTF_8));
    }
}
