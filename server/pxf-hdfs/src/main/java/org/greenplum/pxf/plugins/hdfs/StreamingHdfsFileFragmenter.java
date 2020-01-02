package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;
import org.greenplum.pxf.api.model.FragmentStats;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.StreamingFragmenter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class StreamingHdfsFileFragmenter extends BaseFragmenter implements StreamingFragmenter {
    private int batchSize;
    private int chunkSize;
    private List<String> files = new ArrayList<>();
    private List<Path> dirs = new ArrayList<>();
    private int currentDir = 0;
    private int currentFile = 0;
    HcfsType hcfsType;
    Configuration jobConf;
    FileSystem fs;

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // Check if the underlying configuration is for HDFS
        hcfsType = HcfsType.getHcfsType(configuration, context);
        jobConf = new JobConf(configuration, this.getClass());

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
    }

    /**
     * Gets a batch of files and returns it as a comma-separated list within a single Fragment.
     */
    @Override
    public Fragment next() {
        StringBuilder pathList = new StringBuilder();
        for (int i = 0; i < chunkSize; i++) {
            if (currentFile == files.size()) {
                getMoreFiles();
                if (currentFile == files.size() && currentDir == dirs.size()) {
                    break;
                }
            }
            pathList.append(files.set(currentFile++, null)).append(",");
        }
        if (pathList.length() == 0) {
            return null;
        }
        pathList.setLength(pathList.length() - 1);

        return new Fragment(pathList.toString());
    }

    @Override
    public boolean hasNext() {
        if (currentFile < files.size()) {
            return true;
        }
        if (currentDir < dirs.size()) {
            getMoreFiles();
        }
        return files.size() > 0 && currentFile < files.size();
    }

    private void getMoreFiles() {
        currentFile = 0;
        files.clear();
        while (currentDir < dirs.size() && files.isEmpty()) {
            final Path dir = dirs.get(currentDir++);
            try {
                files = Arrays
                        .stream(fs.listStatus(dir))
                        .filter(file -> !file.isDirectory())
                        .map(file -> file.getPath().toUri().toString())
                        .sorted()
                        .collect(Collectors.toList());
            } catch (IOException e) {
                LOG.info("Could not get list of FileStatus for directory {}: {}", dir, e);
            }
        }
    }

    @Override
    public int getChunkSize() {
        return chunkSize;
    }

    @Override
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
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

    /**
     * Not implemented, see StreamingHdfsFileFragmenter#next() and StreamingHdfsFileFragmenter#hasNext()
     */
    @Override
    public List<Fragment> getFragments() {
        throw new UnsupportedOperationException("Operation getFragments is not supported, use next() and hasNext()");
    }

    @Override
    public FragmentStats getFragmentStats() {
        throw new UnsupportedOperationException("Operation getFragmentStats is not supported");
    }
}
