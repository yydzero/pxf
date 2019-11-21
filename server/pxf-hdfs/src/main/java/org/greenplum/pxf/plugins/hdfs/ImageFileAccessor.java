package org.greenplum.pxf.plugins.hdfs;


import org.greenplum.pxf.api.OneRow;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;

public class ImageFileAccessor extends HdfsAtomicDataAccessor {

    @Override
    public OneRow readNextObject() throws IOException {
        /* check if working segment */
        if (super.readNextObject() == null) {
            return null;
        }

        BufferedImage image = ImageIO.read(inputStream);

        if (image == null) {
            return null;
        }
        // ImageIO.read should read the image fully, so we can safely close the stream
        try {
            inputStream.close();
        } catch (IOException ex) {
            // do not error, just log error
            LOG.error(String.format("%s-%s: Unable to close inputStream for %s",
                    context.getTransactionId(), context.getServerName(), context.getDataSource()),
                    ex);
        }
        return new OneRow(uri, image);
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
