package org.greenplum.pxf.api.serializer;

import java.io.DataOutputStream;

public interface ValueHandler<T>  {

    void handle(DataOutputStream buffer, final T value);
}
