package org.greenplum.pxf.api.serializer.csv;

import org.apache.commons.codec.binary.Hex;

import java.io.DataOutputStream;
import java.io.IOException;

public class ByteArrayCsvValueHandler extends BaseCsvValueHandler<byte[]> {

    @Override
    protected void internalHandle(DataOutputStream buffer, byte[] value) throws IOException {
        buffer.writeChars("\\x");
        buffer.writeChars(Hex.encodeHexString(value));
    }
}
