package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.serializer.ValueHandler;

import java.io.IOException;
import java.io.OutputStreamWriter;

public abstract class BaseCsvValueHandler<T> implements ValueHandler<T> {

    @Override
    public void handle(OutputStreamWriter writer, T value) throws IOException {
        internalHandle(writer, value);
    }

    protected abstract void internalHandle(OutputStreamWriter writer, final T value)
            throws IOException;
}
