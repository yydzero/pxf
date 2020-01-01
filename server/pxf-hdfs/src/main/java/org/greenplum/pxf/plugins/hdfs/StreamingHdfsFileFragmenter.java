package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StreamingHdfsFileFragmenter extends HdfsDataFragmenter {
    private int batchSize;
    private int chunkSize;
    private List<String> files = new ArrayList<>();
    private List<Path> dirs = new ArrayList<>();
    private int currentDir = 0;
    private int currentFile = 0;
    FileSystem fs;

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
        chunkSize = batchSize;
        String path = hcfsType.getDataUri(jobConf, context);
        try {
            fs = FileSystem.get(new URI(path), jobConf);
            getDirs(new Path(path));
            dirs.sort(Comparator.comparing(Path::toString));
        } catch (IOException e) {
            LOG.info("Failed getting directories for path {}: {}", path, e);
        } catch (URISyntaxException e) {
            LOG.info("Failed getting URI for path {}: {}", path, e);
        }
        jobConf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", false);
    }

    /**
     * Gets a batch of fragments for a data source URI that can appear as a file name,
     * a directory name or a wildcard. Returns the data fragments in JSON
     * format.
     */
    @Override
    public List<Fragment> getFragments() throws Exception {
        StringBuilder pathList = new StringBuilder();
        for (int i = 0; i < chunkSize; i++) {
            if (currentFile == files.size()) {
                if (currentDir == dirs.size()) {
                    break;
                }
                files.clear();
                while (files.isEmpty()) {
                    getMoreFiles();
                }
            }
            pathList.append(files.set(currentFile++, null)).append(",");
        }
        if (pathList.length() == 0) {
            return null;
        }
        pathList.setLength(pathList.length() - 1);
        return new ArrayList<Fragment>() {{
            add(new Fragment(pathList.toString()));
        }};
    }

    private void getMoreFiles() throws IOException {
        currentFile = 0;
        files = Arrays
                .stream(fs.listStatus(dirs.get(currentDir++)))
                .filter(fs -> !fs.isDirectory())
                .map(fs -> fs.getPath().toUri().toString())
                .sorted()
                .collect(Collectors.toList());
    }

    private void getDirs(Path path) throws IOException {
        RemoteIterator<FileStatus> iterator = fs.listStatusIterator(path);
        while (iterator.hasNext()) {
            FileStatus fileStatus = iterator.next();
            if (fileStatus.isDirectory()) {
                Path curPath = fileStatus.getPath();
                dirs.add(curPath);
                if (fs.getContentSummary(curPath).getDirectoryCount() > 1) {
                    getDirs(curPath);
                }
            }
        }
    }
}
