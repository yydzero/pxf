package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class StringCsvValueHandler extends BaseCsvValueHandler<String> {

    @Override
    protected void internalHandle(OutputStreamWriter writer, String value) throws IOException {
        writer.write(value);
    }
}
