package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class ObjectCsvValueHandler extends BaseCsvValueHandler<Object> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void internalHandle(OutputStream buffer, Object value) throws IOException {
        buffer.write(Utilities.getUtf8Bytes(Objects.toString(value, null)));
    }
}
