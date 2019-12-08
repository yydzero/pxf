package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.BatchResolver;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unchecked")
public class ImageResolver extends BasePlugin implements BatchResolver {
    private int currentImage;
    List<InputStream> inputStreams;
    // cache of strings for RGB arrays going to Greenplum
    private static String[] r = new String[256];
    private static String[] g = new String[256];
    private static String[] b = new String[256];

    static {
        for (int i = 0; i < 256; i++) {
            r[i] = "{" + i;
            g[i] = "," + i + ",";
            b[i] = i + "}";
        }
    }


    @Override
    public List<OneField> getFields(OneRow row) {
        return null;
    }

    /**
     * Returns a Postgres-style array with RGB values
     */
    @Override
    public List<OneField> startBatch(OneRow row) {
        URI uri = (URI) row.getKey();
        Path path = Paths.get(uri.getPath());

        List<OneField> payload = new ArrayList<>();
        payload.add(new OneField(0, uri.toString()));
        payload.add(new OneField(0, path.getParent().getFileName().toString()));
        payload.add(new OneField(0, path.getFileName().toString()));

        inputStreams = (ArrayList) row.getData();
        return payload;
    }

    @Override
    public byte[] getNextBatchedItem(OneRow row) {
        if (currentImage == inputStreams.size()) {
            return null; // already sent over the last image
        }
        StringBuilder sb;
        InputStream stream = inputStreams.get(currentImage);
        try {
            BufferedImage image = ImageIO.read(stream);
            int h = image.getHeight();
            int w = image.getWidth();
            // avoid arrayCopy() in sb.append() by pre-calculating max image size
            sb = new StringBuilder(
                    w * h * 13 +  // each RGB is at most 13 chars: {255,255,255}
                            (w - 1) * h + // commas separating RGBs
                            h * 2 +       // curly braces surrounding each row of RGBs
                            h - 1 +       // commas separating each row
                            2 +           // outer curly braces for the image
                            2 +           // any combination of image-separating commas and/or outer braces
                            1 +           // opening CSV comma
                            2 +           // double quote chars
                            1             // newline
            );
            LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());
            if (currentImage++ == 0) {
                sb.append(",\"{");
            }
            processImage(sb, image, h, w);
            stream.close();
        } catch (IOException e) {
            LOG.info(e.getMessage());
            return null;
        }
        if (currentImage != inputStreams.size()) {
            sb.append(",");
        } else {
            sb.append("}\"\n");
        }
        LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());

        return sb.toString().getBytes();
    }

    private void processImage(StringBuilder sb, BufferedImage image, int h, int w) {
        if (image == null) {
            return;
        }

        LOG.debug("Image size {}w {}h", w, h);

        sb.append("{");
        for (int i = 0; i < h; i++) {
            sb.append("{");
            for (int j = 0; j < w; j++) {
                int pixel = image.getRGB(j, i);
                sb.append(r[(pixel >> 16) & 0xff]).append(g[(pixel >> 8) & 0xff]).append(b[pixel & 0xff]).append(",");
            }
            sb.setLength(sb.length() - 1);
            sb.append("},");
        }
        sb.setLength(sb.length() - 1);
        sb.append("}");
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException();
    }

}
