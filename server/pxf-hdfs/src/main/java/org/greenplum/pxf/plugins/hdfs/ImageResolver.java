package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StreamingArrayField;
import org.greenplum.pxf.api.model.Accessor;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.StreamingResolver;

import java.awt.image.BufferedImage;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unchecked")
public class ImageResolver extends BasePlugin implements StreamingResolver {
    Accessor accessor;
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

    /**
     * Returns Postgres-style arrays with full paths, parent directories, and names
     * of image files.
     */
    @Override
    public List<OneField> getFields(OneRow row) {
        List<String> paths = (ArrayList) row.getKey();
        accessor = (StreamingImageAccessor) row.getData();

        StringBuilder fullPaths = new StringBuilder("{");
        StringBuilder parentDirs = new StringBuilder("{");
        StringBuilder fileNames = new StringBuilder("{");

        for (String pathString : paths) {
            URI uri = URI.create(pathString);
            Path path = Paths.get(uri.getPath());

            fullPaths.append(uri.getPath()).append(",");
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
                add(new StreamingArrayField(ImageResolver.this));
            }
        };
    }

    @Override
    public boolean hasNext() {
        return ((StreamingImageAccessor) accessor).hasNext();
    }

    /**
     * Returns Postgres-style multi-dimensional array, piece by piece. Each
     * time this method is called it returns another image, where multiple images
     * will end up in the same tuple.
     */
    @Override
    public String getNext() {
        BufferedImage image = ((StreamingImageAccessor) accessor).readNextImage();
        if (image == null) {
            return null;
        }

        StringBuilder sb;
        int w = image.getWidth();
        int h = image.getHeight();
        LOG.debug("Image size {}w {}h", w, h);
        // avoid arrayCopy() in sb.append() by pre-calculating max image size
        sb = new StringBuilder(
                w * h * 13 +  // each RGB is at most 13 chars: {255,255,255}
                        (w - 1) * h + // commas separating RGBs
                        h * 2 +       // curly braces surrounding each row of RGBs
                        h - 1 +       // commas separating each row
                        2             // outer curly braces for the image
        );
        LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());
        processImage(sb, image, w, h);
        LOG.debug("Image length: {}, cap: {}", sb.length(), sb.capacity());

        return sb.toString();
    }

    private static void processImage(StringBuilder sb, BufferedImage image, int w, int h) {
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
