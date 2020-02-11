package org.greenplum.pxf.api.serializer.csv;

import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class ByteArrayCsvValueHandler extends BaseCsvValueHandler<byte[]> {

    @Override
    protected void internalHandle(OutputStream buffer, byte[] value) throws IOException {
        buffer.write("\\x".getBytes(StandardCharsets.UTF_8));
        buffer.write(Hex.encodeHexString(value).getBytes(StandardCharsets.UTF_8));
    }
}
