package org.greenplum.pxf.api;

import java.util.List;

public class ArrayField extends OneField {
    public ArrayField(int type, List<?> val) {
        super(type, val);
    }
}
