package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class IntArrayCsvValueHandler extends BaseCsvValueHandler<int[]> {

    @Override
    protected void internalHandle(OutputStreamWriter writer, int[] value) throws IOException {

        writer.write("{");
        for (int i = 0; i < value.length; i++) {
            writer.write(Integer.toString(value[i]));
            if (i < value.length - 1) {
                writer.write(",");
            }
        }
        writer.write("}");
    }
}
