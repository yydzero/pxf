package org.greenplum.pxf.plugins.hdfs;


import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.GreenplumCSV;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class ImageFileAccessor extends HdfsAtomicDataAccessor {

    @Override
    public OneRow readNextObject() throws IOException {
        /* check if working segment */
        if (super.readNextObject() == null) {
            return null;
        }

        Iterator readers = ImageIO.getImageReadersByFormatName("JPEG");
        ImageReader reader = null;
        while (readers.hasNext()) {
            reader = (ImageReader) readers.next();
            if (reader.canReadRaster()) {
                break;
            }
        }

        BufferedImage image = ImageIO.read(inputStream);

        if (image == null) {
            return null;
        }

        int w = image.getWidth();
        int h = image.getHeight();

        LOG.debug("Image size {}w {}h", w, h);

        StringBuilder sb = new StringBuilder();

        Path path = Paths.get(uri.getPath());

        sb.append(context.getGreenplumCSV().toCsvField(uri.toString(), true, true, true))
                .append(context.getGreenplumCSV().getDelimiter())
                .append(context.getGreenplumCSV().toCsvField(path.getParent().getFileName().toString(), true, true, true))
                .append(context.getGreenplumCSV().getDelimiter())
                .append(context.getGreenplumCSV().toCsvField(path.getFileName().toString(), true, true, true))
                .append(context.getGreenplumCSV().getDelimiter())
                .append("\"{");

        for (int i = 0; i < h; i++) {
            if (i > 0) sb.append(",");
            sb.append("{");
            for (int j = 0; j < w; j++) {
                if (j > 0) sb.append(",");
                int pixel = image.getRGB(j, i);
                sb
                        .append("{")
                        .append(getRGBFromPixel(pixel))
                        .append("}");
            }
            sb.append("}");
        }
        sb.append("}\"");

        return new OneRow(null, sb.toString());
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

    private String getRGBFromPixel(int pixel) {
//        int alpha = (pixel >> 24) & 0xff;
        int red = (pixel >> 16) & 0xff;
        int green = (pixel >> 8) & 0xff;
        int blue = (pixel) & 0xff;
        return String.format("%d,%d,%d", red, green, blue);
    }
}
