package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class BatchHdfsFileFragmenter extends HdfsDataFragmenter {

    private int batchSize;

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);
        batchSize = 1;
        final String batchSizeOption = context.getOption("BATCH_SIZE");
        if (batchSizeOption != null) {
            batchSize = Integer.parseInt(batchSizeOption);
        }
    }

    /**
     * Gets the fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        String fileName = hcfsType.getDataUri(jobConf, context);
        Path path = new Path(fileName);

        PxfInputFormat pxfInputFormat = new PxfInputFormat();
        PxfInputFormat.setInputPaths(jobConf, path);

        final FileStatus[] fileStatuses = pxfInputFormat.listStatus(jobConf);

        Arrays.sort(fileStatuses, Comparator.comparing(fs -> fs.getPath().toUri().toString()));
        StringBuilder pathList = new StringBuilder();
        for (int i = 1; i <= fileStatuses.length; i++) {
            pathList.append(fileStatuses[i - 1].getPath().toUri().toString()).append(",");
            fileStatuses[i - 1] = null;
            if (i % batchSize == 0 || i == fileStatuses.length) {
                pathList.setLength(pathList.length() - 1);
                fragments.add(new Fragment(pathList.toString()));
                pathList.setLength(0);
                LOG.debug("Completed fragment batch #{}", (i - 1) / batchSize);
            }
        }

        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }
}
