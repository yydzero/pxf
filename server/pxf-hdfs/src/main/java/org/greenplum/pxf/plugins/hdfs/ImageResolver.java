package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;

import java.awt.image.BufferedImage;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


public class ImageResolver extends BasePlugin implements Resolver {
    /**
     * Returns a Postgres-style array with RGB values
     */
    @Override
    public List<OneField> getFields(OneRow row) {
        URI uri = (URI) row.getKey();
        BufferedImage image = (BufferedImage) row.getData();
        int w = image.getWidth();
        int h = image.getHeight();

        LOG.debug("Image size {}w {}h", w, h);

        Path path = Paths.get(uri.getPath());

        List<OneField> payload = new ArrayList<>();
        payload.add(new OneField(0, uri.toString()));
        payload.add(new OneField(1, path.getParent().getFileName().toString()));
        payload.add(new OneField(2, path.getFileName().toString()));

        StringBuilder sb = new StringBuilder();

        sb.append("{");

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
        sb.append("}");

        payload.add(new OneField(3, sb.toString()));
        return payload;

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

    private String getRGBFromPixel(int pixel) {
//        int alpha = (pixel >> 24) & 0xff;
        int red = (pixel >> 16) & 0xff;
        int green = (pixel >> 8) & 0xff;
        int blue = (pixel) & 0xff;
        return String.format("%d,%d,%d", red, green, blue);
    }
}
