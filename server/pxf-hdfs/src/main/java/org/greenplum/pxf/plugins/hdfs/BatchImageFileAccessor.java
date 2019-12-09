package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchImageFileAccessor extends BatchHdfsAtomicDataAccessor {
    private boolean served = false;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
    }

    @Override
    public OneRow readNextObject() throws IOException {
        /* check if working segment */
        if (served || super.readNextObject() == null) {
            return null;
        }

        served = true;
        return new OneRow(paths, inputStreams);
    }

    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException();
    }

}
