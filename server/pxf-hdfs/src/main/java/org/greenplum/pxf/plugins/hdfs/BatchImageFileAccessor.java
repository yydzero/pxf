package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class BatchImageFileAccessor extends BatchHdfsAtomicDataAccessor {

    private List<BufferedImage> images;
    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        images = new ArrayList<>();
    }
    @Override
    public OneRow readNextObject() throws IOException {
        /* check if working segment */
        if (super.readNextObject() == null) {
            return null;
        }

        for (InputStream stream : inputStreams) {
            BufferedImage image = ImageIO.read(stream);

            if (image == null) {
                return null;
            }
            images.add(image);
            // ImageIO.read should read the image fully, so we can safely close the stream
            try {
                stream.close();
            } catch (IOException ex) {
                // do not error, just log error
                LOG.error(String.format("%s-%s: Unable to close inputStream for %s",
                        context.getTransactionId(), context.getServerName(), context.getDataSource()),
                        ex);
            }
        }

        return new OneRow(uri, images);
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
