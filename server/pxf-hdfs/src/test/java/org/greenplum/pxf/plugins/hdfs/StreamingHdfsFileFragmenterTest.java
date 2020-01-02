package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StreamingHdfsFileFragmenterTest {
    StreamingHdfsFileFragmenter streamingHdfsFileFragmenter;
    private Fragment fragment;
    private RequestContext context;
    private String path;

    @Before
    public void setup() {
        streamingHdfsFileFragmenter = new StreamingHdfsFileFragmenter();
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("user");
        path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("streamed_fragments/")).getPath();
        context.setProfileScheme("localfile");
        context.setDataSource(path);
    }

    @Test
    public void testInitializeBatchSizeNotGiven() {
        streamingHdfsFileFragmenter.initialize(context);

        assertEquals(1, streamingHdfsFileFragmenter.getBatchSize());
    }

    @Test
    public void testInitializeBatchSizeGiven() {
        context.addOption("BATCH_SIZE", "100");
        streamingHdfsFileFragmenter.initialize(context);

        assertEquals(100, streamingHdfsFileFragmenter.getBatchSize());
    }

    @Test
    public void testGetFragmentsBatchSizeNotGiven() throws Exception {
        streamingHdfsFileFragmenter.initialize(context);

        assertFragment(new Fragment("file://" + path + "dir1/1.csv"));
        assertFragment(new Fragment("file://" + path + "dir1/2.csv"));
        assertFragment(new Fragment("file://" + path + "dir1/3.csv"));
        assertFragment(new Fragment("file://" + path + "dir1/nested_dir/1.csv"));
        assertFragment(new Fragment("file://" + path + "dir1/nested_dir/2.csv"));
        assertFragment(new Fragment("file://" + path + "dir1/nested_dir/3.csv"));
        assertFragment(new Fragment("file://" + path + "dir2/1.csv"));
        assertFragment(new Fragment("file://" + path + "dir2/2.csv"));
        assertFragment(new Fragment("file://" + path + "dir2/3.csv"));
        assertFalse(streamingHdfsFileFragmenter.hasNext());
        assertNull(streamingHdfsFileFragmenter.next());
    }

    @Test
    public void testGetFragmentsLargerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "100");
        streamingHdfsFileFragmenter.initialize(context);

        assertFragment(new Fragment(
                "file://" + path + "dir1/1.csv" + ","
                        + "file://" + path + "dir1/2.csv" + ","
                        + "file://" + path + "dir1/3.csv" + ","
                        + "file://" + path + "dir1/nested_dir/1.csv" + ","
                        + "file://" + path + "dir1/nested_dir/2.csv" + ","
                        + "file://" + path + "dir1/nested_dir/3.csv" + ","
                        + "file://" + path + "dir2/1.csv" + ","
                        + "file://" + path + "dir2/2.csv" + ","
                        + "file://" + path + "dir2/3.csv"
        ));
        assertFalse(streamingHdfsFileFragmenter.hasNext());
        assertNull(streamingHdfsFileFragmenter.next());
    }

    @Test
    public void testGetFragmentsSmallerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "2");
        streamingHdfsFileFragmenter.initialize(context);

        assertFragment(new Fragment(
                "file://" + path + "dir1/1.csv" + ","
                        + "file://" + path + "dir1/2.csv"
        ));
        assertFragment(new Fragment(
                "file://" + path + "dir1/3.csv" + ","
                        + "file://" + path + "dir1/nested_dir/1.csv"
        ));
        assertFragment(new Fragment(
                "file://" + path + "dir1/nested_dir/2.csv" + ","
                        + "file://" + path + "dir1/nested_dir/3.csv"
        ));
        assertFragment(new Fragment(
                "file://" + path + "dir2/1.csv" + ","
                        + "file://" + path + "dir2/2.csv"
        ));
        assertFragment(new Fragment(
                "file://" + path + "dir2/3.csv"
        ));
        assertFalse(streamingHdfsFileFragmenter.hasNext());
        assertNull(streamingHdfsFileFragmenter.next());
    }

    private void assertFragment(Fragment correctFragment) {
        assertTrue(streamingHdfsFileFragmenter.hasNext());
        fragment = streamingHdfsFileFragmenter.next();
        assertNotNull(fragment);
        assertEquals(correctFragment.getSourceName(), fragment.getSourceName());
    }
}