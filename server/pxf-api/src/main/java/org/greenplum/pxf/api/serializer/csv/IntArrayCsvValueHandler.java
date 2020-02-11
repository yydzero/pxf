package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class IntArrayCsvValueHandler extends BaseCsvValueHandler<int[]> {

    @Override
    protected void internalHandle(DataOutputStream buffer, int[] value) throws IOException {

        buffer.write("{".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < value.length; i++) {
            buffer.write(Integer.toString(value[i]).getBytes(StandardCharsets.UTF_8));
            if (i < value.length - 1) {
                buffer.write(",".getBytes(StandardCharsets.UTF_8));
            }
        }
        buffer.write("}".getBytes(StandardCharsets.UTF_8));
    }
}
