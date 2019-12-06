package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.BatchResolver;
import org.greenplum.pxf.api.model.Resolver;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
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
    // number of images going over the wire at a time
    private int imagesGroupSize = 25;

    /**
     * Returns a Postgres-style array with RGB values
     */
    @Override
    public List<OneField> getFields(OneRow row) throws IOException {
        URI uri = (URI) row.getKey();
        Path path = Paths.get(uri.getPath());

        List<OneField> payload = new ArrayList<>();
        payload.add(new OneField(0, uri.toString()));
        payload.add(new OneField(0, path.getParent().getFileName().toString()));
        payload.add(new OneField(0, path.getFileName().toString()));

        StringBuilder sb = new StringBuilder();
        Object data = row.getData();
        if (data instanceof InputStream) {
            InputStream stream = (InputStream) row.getData();
            processImage(sb, ImageIO.read(stream));
            stream.close();
        } else if (data instanceof ArrayList) {
            int cnt = 0;
            final ArrayList<InputStream> inputStreams = (ArrayList) data;
            sb.append("{");
            for (InputStream stream : inputStreams) {
                processImage(sb, ImageIO.read(stream));
                stream.close();
                if (++cnt == inputStreams.size()) {
                    continue;
                }
                sb.append(",");
            }
            sb.append("}");
        } else {
            return null;
        }

        payload.add(new OneField(0, sb.toString()));
        return payload;
    }

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
            return null;
        }
        StringBuilder sb = new StringBuilder();
        if (currentImage == 0) {
            sb.append(",\"{");
        }
        InputStream stream;
        for (int i = 0; i < imagesGroupSize; i++) {
            stream = inputStreams.get(currentImage++);
            try {
                processImage(sb, ImageIO.read(new BufferedInputStream(stream)));
                stream.close();
            } catch (IOException e) {
                LOG.info(e.getMessage());
            }
            if (currentImage != inputStreams.size()) {
                sb.append(",");
            } else {
                sb.append("}\"\n");
                break;
            }
        }

        return sb.toString().getBytes();
    }

    private void processImage(StringBuilder sb, BufferedImage image) {
        if (image == null) {
            return;
        }
        int w = image.getWidth();
        int h = image.getHeight();

        LOG.debug("Image size {}w {}h", w, h);

        sb.append("{");

        for (int i = 0; i < h; i++) {
            if (i > 0) sb.append(",");
            sb.append("{");
            for (int j = 0; j < w; j++) {
                if (j > 0) sb.append(",");
                int pixel = image.getRGB(j, i);
                sb
                        .append("{")
                        .append((pixel >> 16) & 0xff)
                        .append(",")
                        .append((pixel >> 8) & 0xff)
                        .append(",")
                        .append(pixel & 0xff)
                        .append("}");
            }
            sb.append("}");
        }
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
