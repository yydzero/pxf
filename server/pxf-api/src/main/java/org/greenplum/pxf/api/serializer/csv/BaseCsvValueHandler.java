package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.serializer.ValueHandler;

import java.io.IOException;
import java.io.OutputStreamWriter;

public abstract class BaseCsvValueHandler<T> implements ValueHandler<T> {

    @Override
    public void handle(OutputStreamWriter writer, T value) {
        try {
            internalHandle(writer, value);
        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize value", e);
        }
    }

    protected abstract void internalHandle(OutputStreamWriter writer, final T value)
            throws IOException;
}
