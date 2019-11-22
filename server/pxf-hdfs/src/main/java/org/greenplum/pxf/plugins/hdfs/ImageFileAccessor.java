package org.greenplum.pxf.plugins.hdfs;


import org.greenplum.pxf.api.OneRow;

import java.io.IOException;

public class ImageFileAccessor extends HdfsAtomicDataAccessor {
    private boolean served;
    @Override
    public OneRow readNextObject() throws IOException {
        /* check if working segment */
        if (served || super.readNextObject() == null) {
            return null;
        }

        served = true;
        return new OneRow(uri, inputStream);
    }

    @Override
    public boolean openForWrite() throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForWrite() throws Exception {
        throw new UnsupportedOperationException();
    }

}
