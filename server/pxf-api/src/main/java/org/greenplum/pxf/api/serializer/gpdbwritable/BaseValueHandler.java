package org.greenplum.pxf.api.serializer.gpdbwritable;

import org.greenplum.pxf.api.serializer.ValueHandler;

import java.io.IOException;
import java.io.OutputStream;

public abstract class BaseValueHandler<T> implements ValueHandler<T> {

    @Override
    public void handle(OutputStream buffer, T value) {
        try {
            if (value != null) {
                internalHandle(buffer, value);
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize value", e);
        }
    }

    protected abstract void internalHandle(OutputStream buffer, final T value)
            throws IOException;
}
