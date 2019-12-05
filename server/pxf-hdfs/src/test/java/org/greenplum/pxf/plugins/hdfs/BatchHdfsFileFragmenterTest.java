package org.greenplum.pxf.plugins.hdfs;

import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BatchHdfsFileFragmenterTest {

    BatchHdfsFileFragmenter batchHdfsFileFragmenter;
    RequestContext context;

    @Before
    public void setup() {
        batchHdfsFileFragmenter = new BatchHdfsFileFragmenter();
        context = new RequestContext();
        context.setConfig("default");
    }

    @Test
    public void testInitializeBatchSizeNotGiven() {
        batchHdfsFileFragmenter.initialize(context);

        assertEquals(1, batchHdfsFileFragmenter.getBatchSize());
    }

    @Test
    public void testInitializeBatchSizeGiven() {
        context.addOption("BATCH_SIZE", "100");
        batchHdfsFileFragmenter.initialize(context);

        assertEquals(100, batchHdfsFileFragmenter.getBatchSize());
    }

    @Test
    public void testGetFragmentsBatchSizeNotGiven() throws Exception {
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();
        context.setProfileScheme("localfile");
        context.setDataSource(path);
        batchHdfsFileFragmenter.initialize(context);
        List<Fragment> fragments = batchHdfsFileFragmenter.getFragments();

        assertNotNull(fragments);
        assertEquals(4, fragments.size());
    }

    @Test
    public void testGetFragmentsLargerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "100");
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();
        context.setProfileScheme("localfile");
        context.setDataSource(path);
        batchHdfsFileFragmenter.initialize(context);
        List<Fragment> fragments = batchHdfsFileFragmenter.getFragments();

        assertNotNull(fragments);
        assertEquals(1, fragments.size());
    }

    @Test
    public void testGetFragmentsSmallerBatchSizeGiven() throws Exception {
        context.addOption("BATCH_SIZE", "2");
        String path = this.getClass().getClassLoader().getResource("csv/").getPath();
        context.setProfileScheme("localfile");
        context.setDataSource(path);
        batchHdfsFileFragmenter.initialize(context);
        List<Fragment> fragments = batchHdfsFileFragmenter.getFragments();

        assertNotNull(fragments);
        assertEquals(2, fragments.size());
    }
}