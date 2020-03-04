package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class StreamingImageAccessorTest {
    private StreamingImageAccessor accessor;
    private List<BufferedImage> images;
    private List<String> paths;
    private static final int NUM_IMAGES = 5;
    private static final String imageLocation = "/tmp/publicstage/pxf/StreamingImageAccessorTest";

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        paths = new ArrayList<>();
        images = ImageTestHelper.generateRandomImages(8, 4, NUM_IMAGES, paths, imageLocation);
        accessor = new StreamingImageAccessor();
        RequestContext context = new RequestContext();
        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setSegmentId(0);
        context.setTransactionId("testID");
        context.setProfileScheme("localfile");
        context.setDataSource(String.join(",", paths));
        accessor.initialize(context);
    }

    @Test
    public void testOpenForRead() throws Exception {
        List<InputStream> streams = accessor.getInputStreams();
        assertEquals(0, streams.size());

        assertTrue(accessor.openForRead());
        streams = accessor.getInputStreams();
        assertEquals(5, streams.size());
    }

    @Test
    public void testCloseForRead() throws Exception {
        exceptionRule.expect(IOException.class);
        exceptionRule.expectMessage("Stream is closed!");
        assertTrue(accessor.openForRead());
        accessor.closeForRead();

        List<InputStream> streams = accessor.getInputStreams();
        assertEquals(NUM_IMAGES, streams.size());
        streams.get(0).read();
    }

    @Test
    public void testReadNextObject() throws Exception {
        assertTrue(accessor.openForRead());
        OneRow row = accessor.readNextObject();
        assertNotNull(row);
        assertTrue(row.getKey() instanceof List);
        assertTrue(row.getData() instanceof StreamingImageAccessor);
        int cnt = 0;
        for (Object path : (List<?>) row.getKey()) {
            assertEquals(paths.get(cnt++), path);
        }
        assertEquals(NUM_IMAGES, cnt);
        assertNull(accessor.readNextObject());
    }

    @Test
    public void testReadNextImage() throws Exception {
        assertTrue(accessor.openForRead());

        OneRow row = accessor.readNextObject();

        StreamingImageAccessor passedAccessor = (StreamingImageAccessor) row.getData();
        for (BufferedImage image : images) {
            assertTrue(accessor.hasNext());
            assertImageEquals(image, passedAccessor.readNextImage());
        }
        assertFalse(accessor.hasNext());
        assertNull(passedAccessor.readNextImage());
    }

    private static void assertImageEquals(BufferedImage image, BufferedImage readImage) {
        int h = image.getHeight();
        int w = image.getWidth();
        for (int i = 0; i < w; i++) {
            for (int j = 0; j < h; j++) {
                assertEquals(image.getRGB(i, j), readImage.getRGB(i, j));
            }
        }
    }
}