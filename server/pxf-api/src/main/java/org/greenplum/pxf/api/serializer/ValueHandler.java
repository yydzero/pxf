package org.greenplum.pxf.api.serializer;

import java.io.IOException;
import java.io.OutputStreamWriter;

public interface ValueHandler<T> {

    void handle(OutputStreamWriter writer, final T value) throws IOException;
}
