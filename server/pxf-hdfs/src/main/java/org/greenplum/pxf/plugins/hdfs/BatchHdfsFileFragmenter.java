package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.util.ArrayList;
import java.util.List;

public class BatchHdfsFileFragmenter extends HdfsDataFragmenter {

    private static int batchSize = 10;
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

        final FileStatus[] fileList = pxfInputFormat.listStatus(jobConf);

        List<String> pathList = new ArrayList<>();
        for (int i = 1; i < fileList.length + 1; i++) {
            if (i % batchSize == 0 || i == fileList.length) {
                fragments.add(new Fragment(String.join(",", pathList)));
                pathList.clear();
                continue;
            }
            pathList.add(fileList[i].getPath().toUri().toString());
        }

        LOG.debug("Total number of fragments = {}", fragments.size());

        return fragments;
    }

}
