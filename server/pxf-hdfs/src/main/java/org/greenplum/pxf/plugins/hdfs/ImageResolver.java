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
        String intStr;
        for (int i = 0; i < 256; i++) {
            intStr = String.valueOf(i);
            r[i] = "{" + intStr;
            g[i] = "," + intStr + ",";
            b[i] = intStr + "}";
        }
    }

    @Override
    public List<OneField> getFields(OneRow row) {
        return null;
    }

    /**
     * Returns Postgres-style arrays with full paths, parent directories, and names
     * of image files.
     */
    @Override
    public List<OneField> startBatch(OneRow row) {
        List<String> paths = (ArrayList) row.getKey();
        inputStreams = (ArrayList) row.getData();

        StringBuilder fullPaths = new StringBuilder("{");
        StringBuilder parentDirs = new StringBuilder("{");
        StringBuilder fileNames = new StringBuilder("{");

        for (String pathString : paths) {
            URI uri = URI.create(pathString);
            Path path = Paths.get(uri.getPath());

            fullPaths.append(uri.toString()).append(",");
            parentDirs.append(path.getParent().getFileName().toString()).append(",");
            fileNames.append(path.getFileName().toString()).append(",");
        }

        fullPaths.setLength(fullPaths.length() - 1);
        parentDirs.setLength(parentDirs.length() - 1);
        fileNames.setLength(fileNames.length() - 1);

        fullPaths.append("}");
        parentDirs.append("}");
        fileNames.append("}");

        return new ArrayList<OneField>() {
            {
                add(new OneField(0, fullPaths.toString()));
                add(new OneField(0, parentDirs.toString()));
                add(new OneField(0, fileNames.toString()));
            }
        };
    }

    /**
     * Returns Postgres-style multi-dimensional array, piece by piece. Each
     * time this method is called it returns another image, where multiple images
     * will end up in the same tuple.
     */
    @Override
    public byte[] getNextBatchedItem(OneRow row) {
        if (currentImage == inputStreams.size()) {
            return null; // already sent over the last image
        }
        StringBuilder sb;
        InputStream stream = inputStreams.get(currentImage);
        try {
            BufferedImage image = ImageIO.read(stream);
            int w = image.getWidth();
            int h = image.getHeight();
            LOG.debug("Image size {}w {}h", w, h);
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
            processImage(sb, image, w, h);
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

    private void processImage(StringBuilder sb, BufferedImage image, int w, int h) {
        if (image == null) {
            return;
        }

        sb.append("{{");
        int cnt = 0;
        for (int pixel : image.getRGB(0, 0, w, h, null, 0, w)) {
            sb.append(r[(pixel >> 16) & 0xff]).append(g[(pixel >> 8) & 0xff]).append(b[pixel & 0xff]).append(",");
            if (++cnt % w == 0) {
                sb.setLength(sb.length() - 1);
                sb.append("},{");
            }
        }
        sb.setLength(sb.length() - 2);
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
