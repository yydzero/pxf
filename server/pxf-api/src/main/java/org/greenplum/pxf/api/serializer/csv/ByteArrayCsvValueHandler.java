package org.greenplum.pxf.api.serializer.csv;

import org.apache.commons.codec.binary.Hex;
import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;
import java.io.OutputStream;

public class ByteArrayCsvValueHandler extends BaseCsvValueHandler<byte[]> {

    @Override
    protected void internalHandle(OutputStream buffer, byte[] value) throws IOException {
        buffer.write(Utilities.getUtf8Bytes("\\x"));
        buffer.write(Utilities.getUtf8Bytes(Hex.encodeHexString(value)));
    }
}
