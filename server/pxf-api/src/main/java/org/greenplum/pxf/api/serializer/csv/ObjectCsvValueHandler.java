package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ObjectCsvValueHandler extends BaseCsvValueHandler<Object> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void internalHandle(DataOutputStream buffer, Object value) throws IOException {
        byte[] b = value.toString().getBytes(StandardCharsets.UTF_8);
        buffer.write(b, 0, b.length);
    }
}
