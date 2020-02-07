package org.greenplum.pxf.api.serializer;

import java.io.OutputStream;

public interface ValueHandler<T>  {

    void handle(OutputStream buffer, final T value);
}
