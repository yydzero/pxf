package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.serializer.ValueHandler;

import java.io.DataOutputStream;
import java.io.IOException;

public abstract class BaseCsvValueHandler<T> implements ValueHandler<T> {

    @Override
    public void handle(DataOutputStream buffer, T value) {
        try {
            internalHandle(buffer, value);
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize value", e);
        }
    }

    protected abstract void internalHandle(DataOutputStream buffer, final T value)
            throws IOException;
}
