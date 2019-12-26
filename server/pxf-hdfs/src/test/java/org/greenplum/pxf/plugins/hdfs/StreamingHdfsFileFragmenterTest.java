package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class StreamingHdfsFileFragmenterTest {

    StreamingHdfsFileFragmenter streamingHdfsFileFragmenter;
    private List<Fragment> fragments;
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

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment("file://" + path + "dir1/simple1.csv"), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment("file://" + path + "dir1/simple2.csv"), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment("file://" + path + "dir1/simple3.csv"), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment("file://" + path + "dir2/simple1.csv"), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment("file://" + path + "dir2/simple2.csv"), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment("file://" + path + "dir2/simple3.csv"), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNull(fragments);
    }

    @Test
    public void testGetFragmentsLargerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "100");
        streamingHdfsFileFragmenter.initialize(context);

        fragments = streamingHdfsFileFragmenter.getFragments();

        assertNotNull(fragments);
        assertFragmentFromList(new Fragment(
                "file://" + path + "dir1/simple1.csv" + ","
                        + "file://" + path + "dir1/simple2.csv" + ","
                        + "file://" + path + "dir1/simple3.csv" + ","
                        + "file://" + path + "dir2/simple1.csv" + ","
                        + "file://" + path + "dir2/simple2.csv" + ","
                        + "file://" + path + "dir2/simple3.csv"
        ), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNull(fragments);
    }

    @Test
    public void testGetFragmentsSmallerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "2");
        streamingHdfsFileFragmenter.initialize(context);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment(
                "file://" + path + "dir1/simple1.csv" + ","
                        + "file://" + path + "dir1/simple2.csv"
        ), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment(
                "file://" + path + "dir1/simple3.csv" + ","
                        + "file://" + path + "dir2/simple1.csv"
        ), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNotNull(fragments);
        assertFragmentFromList(new Fragment(
                "file://" + path + "dir2/simple2.csv" + ","
                        + "file://" + path + "dir2/simple3.csv"
        ), fragments);

        fragments = streamingHdfsFileFragmenter.getFragments();
        assertNull(fragments);
    }

    private static void assertFragmentFromList(Fragment correctFragment, List<Fragment> fragments) {
        assertEquals(1, fragments.size());
        assertEquals(correctFragment.getSourceName(), fragments.get(0).getSourceName());
    }
}