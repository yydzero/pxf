package org.greenplum.pxf.api.serializer.csv;

import org.apache.commons.codec.binary.Hex;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class ByteArrayCsvValueHandler extends BaseCsvValueHandler<byte[]> {

    @Override
    protected void internalHandle(OutputStreamWriter writer, byte[] value) throws IOException {
        writer.write("\\x");
        writer.write(Hex.encodeHex(value));
    }
}
