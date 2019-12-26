package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.plugins.hdfs.utilities.PxfInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class StreamingHdfsFileFragmenter extends HdfsDataFragmenter {
    private int batchSize;
    private List<String> files;
    private List<String> dirs = new ArrayList<>();
    private int currentDir = 0;
    private int currentFile = 0;
    PxfInputFormat pxfInputFormat = new PxfInputFormat();
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
        files = new ArrayList<>(batchSize);
        String fileName = hcfsType.getDataUri(jobConf, context);
        Path path = new Path(fileName);

        jobConf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false);
        PxfInputFormat.setInputPaths(jobConf, path);

        try {
            dirs = Arrays
                    .stream(pxfInputFormat.listStatus(jobConf))
                    .filter(FileStatus::isDirectory)
                    .map(fs -> fs.getPath().toUri().toString())
                    .sorted()
                    .collect(Collectors.toList());
            getMoreFiles(new Path(dirs.get(currentDir++)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets a batch of fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        if (currentDir == dirs.size() && currentFile == files.size()) {
            return null;
        }

        StringBuilder pathList = new StringBuilder();
        for (int i = 0; i < batchSize; i++) {
            if (currentFile == files.size()) {
                if (currentDir == dirs.size()) {
                    break;
                }
                getMoreFiles(new Path(dirs.get(currentDir++)));
            }
            pathList.append(files.set(currentFile++, null)).append(",");
        }
        pathList.setLength(pathList.length() - 1);
        return new ArrayList<Fragment>() {{
            add(new Fragment(pathList.toString()));
        }};
    }

    private void getMoreFiles(Path path) throws IOException {
        PxfInputFormat.setInputPaths(jobConf, path);
        currentFile = 0;
        files = Arrays
                .stream(pxfInputFormat.listStatus(jobConf))
                .map(fs -> fs.getPath().toUri().toString())
                .sorted()
                .collect(Collectors.toList());
    }
}
