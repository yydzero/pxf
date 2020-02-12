package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class ObjectCsvValueHandler extends BaseCsvValueHandler<Object> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected void internalHandle(OutputStreamWriter writer, Object value) throws IOException {
        writer.write(value.toString());
    }
}
