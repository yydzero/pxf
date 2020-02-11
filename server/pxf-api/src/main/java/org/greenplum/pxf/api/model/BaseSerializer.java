package org.greenplum.pxf.api.model;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public abstract class BaseSerializer implements Serializer, AutoCloseable {

    protected static final int OneKB = 1024;
    private final int bufferSize;

    protected transient DataOutputStream buffer;

    protected BaseSerializer() {
        this(512 * OneKB);
    }

    public BaseSerializer(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public void open(final OutputStream out) {
        buffer = new DataOutputStream(new BufferedOutputStream(out, bufferSize));
    }

    @Override
    public void close() throws IOException {
        buffer.flush();
        buffer.close();
    }
}
