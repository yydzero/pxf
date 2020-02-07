package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;
import java.io.OutputStream;

public class IntArrayCsvValueHandler extends BaseCsvValueHandler<int[]> {

    @Override
    protected void internalHandle(OutputStream buffer, int[] value) throws IOException {

        buffer.write(Utilities.getUtf8Bytes("{"));
        for (int i = 0; i < value.length; i++) {
            buffer.write(Utilities.getUtf8Bytes(String.valueOf(value[i])));
            if (i < value.length - 1) {
                buffer.write(Utilities.getUtf8Bytes(","));
            }
        }
        buffer.write(Utilities.getUtf8Bytes("}"));
    }
}
