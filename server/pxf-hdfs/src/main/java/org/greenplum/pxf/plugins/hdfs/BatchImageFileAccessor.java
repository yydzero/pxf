package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchImageFileAccessor extends BatchHdfsAtomicDataAccessor {
    private List<BufferedImage> images;
    private boolean served = false;
    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        images = new ArrayList<>();
    }
    @Override
    public OneRow readNextObject() throws IOException {
        /* check if working segment */
        if (served || super.readNextObject() == null) {
            return null;
        }

        served = true;
        return new OneRow(uri, inputStreams);
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
