package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;
import java.io.OutputStream;

public class StringCsvValueHandler extends BaseCsvValueHandler<String> {

    @Override
    protected void internalHandle(OutputStream buffer, String value) throws IOException {
        buffer.write(Utilities.getUtf8Bytes(value));
    }
}
