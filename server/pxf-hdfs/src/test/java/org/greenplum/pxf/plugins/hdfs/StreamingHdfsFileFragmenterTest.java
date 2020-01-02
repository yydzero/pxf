package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StreamingHdfsFileFragmenterTest {
    StreamingHdfsFileFragmenter streamingHdfsFileFragmenter;
    private RequestContext context;
    private String path;

    @Rule
    public TemporaryFolder tempFolder;

    @Before
    public void setup() throws IOException {
        streamingHdfsFileFragmenter = new StreamingHdfsFileFragmenter();
        context = new RequestContext();
        context.setConfig("default");
        context.setUser("user");
        context.setProfileScheme("localfile");
        tempFolder = new TemporaryFolder();
        tempFolder.create();
        path = tempFolder.getRoot().toString() + "/";
        context.setDataSource(path);
        // important to test empty directories, but empty dirs are not tracked in git
        tempFolder.newFolder("empty_dir");
        tempFolder.newFolder("dir1", "nested_dir");
        tempFolder.newFolder("dir2", "empty_nested_dir");
        tempFolder.newFile("dir1/1.csv");
        tempFolder.newFile("dir1/2.csv");
        tempFolder.newFile("dir1/3.csv");
        tempFolder.newFile("dir2/1.csv");
        tempFolder.newFile("dir2/2.csv");
        tempFolder.newFile("dir2/3.csv");
        tempFolder.newFile("dir1/nested_dir/1.csv");
        tempFolder.newFile("dir1/nested_dir/2.csv");
        tempFolder.newFile("dir1/nested_dir/3.csv");
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
    public void testNextAndHasNext_BatchSizeNotGiven() throws Exception {
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
        assertNoMoreFragments();
    }

    @Test
    public void testNextAndHasNext_LastDirIsNotEmpty() throws Exception {
        tempFolder.newFolder("test", "dir1", "empty_dir");
        tempFolder.newFolder("test", "dir2");
        tempFolder.newFile("test/dir1/1.csv");
        tempFolder.newFile("test/dir1/2.csv");
        tempFolder.newFile("test/dir1/3.csv");
        tempFolder.newFile("test/dir1/4.csv");
        tempFolder.newFile("test/dir1/5.csv");
        tempFolder.newFile("test/dir2/1.csv");
        tempFolder.newFile("test/dir2/2.csv");
        tempFolder.newFile("test/dir2/3.csv");
        tempFolder.newFile("test/dir2/4.csv");
        tempFolder.newFile("test/dir2/5.csv");
        context.addOption("BATCH_SIZE", "10");
        context.setDataSource(path + "test");
        streamingHdfsFileFragmenter.initialize(context);

        assertFragment(new Fragment(
                "file://" + path + "test/dir1/1.csv" + ","
                        + "file://" + path + "test/dir1/2.csv" + ","
                        + "file://" + path + "test/dir1/3.csv" + ","
                        + "file://" + path + "test/dir1/4.csv" + ","
                        + "file://" + path + "test/dir1/5.csv" + ","
                        + "file://" + path + "test/dir2/1.csv" + ","
                        + "file://" + path + "test/dir2/2.csv" + ","
                        + "file://" + path + "test/dir2/3.csv" + ","
                        + "file://" + path + "test/dir2/4.csv" + ","
                        + "file://" + path + "test/dir2/5.csv"
        ));
        assertNoMoreFragments();
    }

    @Test
    public void testNextAndHasNext_LargerBatchSizeGiven() throws Exception {
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
        assertNoMoreFragments();
    }

    @Test
    public void testNextAndHasNext_SmallerBatchSizeGiven() throws Exception {
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
        assertNoMoreFragments();
    }

    private void assertFragment(Fragment correctFragment) {
        assertTrue(streamingHdfsFileFragmenter.hasNext());
        Fragment fragment = streamingHdfsFileFragmenter.next();
        assertNotNull(fragment);
        assertEquals(correctFragment.getSourceName(), fragment.getSourceName());
    }

    private void assertNoMoreFragments() {
        assertFalse(streamingHdfsFileFragmenter.hasNext());
        assertNull(streamingHdfsFileFragmenter.next());
    }
}