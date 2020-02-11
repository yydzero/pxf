package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringCsvValueHandler extends BaseCsvValueHandler<String> {

    @Override
    protected void internalHandle(DataOutputStream buffer, String value) throws IOException {
        byte[] b = value.getBytes(StandardCharsets.UTF_8);
        buffer.write(b, 0, b.length);
    }
}
