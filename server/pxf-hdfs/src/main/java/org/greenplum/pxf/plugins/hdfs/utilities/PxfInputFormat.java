package org.greenplum.pxf.plugins.hdfs.utilities;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * PxfInputFormat is not intended to read a specific format, hence it implements
 * a dummy getRecordReader Instead, its purpose is to apply
 * FileInputFormat.getSplits from one point in PXF and get the splits which are
 * valid for the actual InputFormats, since all of them we use inherit
 * FileInputFormat but do not override getSplits.
 */
public class PxfInputFormat extends FileInputFormat {
//
//    private static final PathFilter hiddenFileFilter = p -> {
//        String name = p.getName();
//        return !name.startsWith("_") && !name.startsWith(".");
//    };

    @Override
    public RecordReader getRecordReader(InputSplit split,
                                        JobConf conf,
                                        Reporter reporter) {
        throw new UnsupportedOperationException("PxfInputFormat should not be used for reading data, but only for obtaining the splits of a file");
    }

    @Override
    public FileStatus[] listStatus(JobConf job) throws IOException {
        return super.listStatus(job);
    }

//    /**
//     * Splits files returned by {@link #listStatus(JobConf)} when
//     * they're too big.
//     */
//    @Override
//    public InputSplit[] getSplits(JobConf job, int numSplits)
//            throws IOException {
//        StopWatch sw = new StopWatch().start();
//        FileStatus[] files = listStatus(job);
//
//        // Save the number of input files for metrics/loadgen
//        job.setLong(NUM_INPUT_FILES, files.length);
//        long totalSize = 0;                           // compute total size
//        for (FileStatus file : files) {                // check we have valid files
//            if (file.isDirectory()) {
//                throw new IOException("Not a file: " + file.getPath());
//            }
//            totalSize += file.getLen();
//        }
//
//        long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
//        long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
//                FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);
//
//        // generate splits
//        ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
//        NetworkTopology clusterMap = new NetworkTopology();
//        for (FileStatus file : files) {
//            Path path = file.getPath();
//            long length = file.getLen();
//            if (length != 0) {
//                FileSystem fs = path.getFileSystem(job);
//                BlockLocation[] blkLocations;
//                if (file instanceof LocatedFileStatus) {
//                    blkLocations = ((LocatedFileStatus) file).getBlockLocations();
//                } else {
//                    blkLocations = fs.getFileBlockLocations(file, 0, length);
//                }
//                if (isSplitable(fs, path)) {
//                    long blockSize = file.getBlockSize();
//                    long splitSize = computeSplitSize(goalSize, minSize, blockSize);
//
//                    long bytesRemaining = length;
//                    while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
//                        String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
//                                length - bytesRemaining, splitSize, clusterMap);
//                        splits.add(makeSplit(path, length - bytesRemaining, splitSize,
//                                splitHosts[0], splitHosts[1]));
//                        bytesRemaining -= splitSize;
//                    }
//
//                    if (bytesRemaining != 0) {
//                        String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
//                                - bytesRemaining, bytesRemaining, clusterMap);
//                        splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
//                                splitHosts[0], splitHosts[1]));
//                    }
//                } else {
//                    if (LOG.isDebugEnabled()) {
//                        // Log only if the file is big enough to be splitted
//                        if (length > Math.min(file.getBlockSize(), minSize)) {
//                            LOG.debug("File is not splittable so no parallelization "
//                                    + "is possible: " + file.getPath());
//                        }
//                    }
//                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, 0, length, clusterMap);
//                    splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
//                }
//            } else {
//                //Create empty hosts array for zero length files
//                splits.add(makeSplit(path, 0, length, new String[0]));
//            }
//        }
//        sw.stop();
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Total # of splits generated by getSplits: " + splits.size()
//                    + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
//        }
//        return splits.toArray(new FileSplit[splits.size()]);
//    }
//
//    public Iterator<FileStatus> listStatusIterator(JobConf job) throws IOException {
//        Path[] dirs = getInputPaths(job);
//        if (dirs.length == 0) {
//            throw new IOException("No input paths specified in job");
//        }
//
//        // get tokens for all the required FileSystems..
//        TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);
//
//        // Whether we need to recursive look into the directory structure
//        boolean recursive = job.getBoolean(INPUT_DIR_RECURSIVE, false);
//
//        // creates a MultiPathFilter with the hiddenFileFilter and the
//        // user provided one (if any).
//        List<PathFilter> filters = new ArrayList<>();
//        filters.add(hiddenFileFilter);
//        PathFilter jobFilter = getInputPathFilter(job);
//        if (jobFilter != null) {
//            filters.add(jobFilter);
//        }
//        PathFilter inputFilter = new MultiPathFilter(filters);
//
//        Iterator<FileStatus> result;
//        int numThreads = job
//                .getInt(
//                        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.LIST_STATUS_NUM_THREADS,
//                        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.DEFAULT_LIST_STATUS_NUM_THREADS);
//
//        StopWatch sw = new StopWatch().start();
//        if (numThreads == 1) {
//            List<FileStatus> locatedFiles = singleThreadedListStatus(job, dirs, inputFilter, recursive);
//            result = locatedFiles.iterator();
//        } else {
//            Iterable<FileStatus> locatedFiles;
//            try {
//
//                LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(
//                        job, dirs, recursive, inputFilter, false);
//                locatedFiles = locatedFileStatusFetcher.getFileStatuses();
//            } catch (InterruptedException e) {
//                throw (IOException)
//                        new InterruptedIOException("Interrupted while getting file statuses")
//                                .initCause(e);
//            }
//            result = locatedFiles.iterator();
//        }
//
//        sw.stop();
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Time taken to get FileStatuses: "
//                    + sw.now(TimeUnit.MILLISECONDS));
//        }
//        return result;
//    }
//
//    private List<FileStatus> singleThreadedListStatus(JobConf job, Path[] dirs,
//                                                      PathFilter inputFilter, boolean recursive) throws IOException {
//        List<FileStatus> result = new ArrayList<>();
//        List<IOException> errors = new ArrayList<>();
//        for (Path p : dirs) {
//            FileSystem fs = p.getFileSystem(job);
//            FileStatus[] matches = fs.globStatus(p, inputFilter);
//            if (matches == null) {
//                errors.add(new IOException("Input path does not exist: " + p));
//            } else if (matches.length == 0) {
//                errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
//            } else {
//                for (FileStatus globStat : matches) {
//                    if (globStat.isDirectory()) {
//                        RemoteIterator<LocatedFileStatus> iter =
//                                fs.listLocatedStatus(globStat.getPath());
//                        while (iter.hasNext()) {
//                            LocatedFileStatus stat = iter.next();
//                            if (inputFilter.accept(stat.getPath())) {
//                                if (recursive && stat.isDirectory()) {
//                                    addInputPathRecursively(result, fs, stat.getPath(),
//                                            inputFilter);
//                                } else {
//                                    result.add(stat);
//                                }
//                            }
//                        }
//                    } else {
//                        result.add(globStat);
//                    }
//                }
//            }
//        }
//        if (!errors.isEmpty()) {
//            throw new InvalidInputException(errors);
//        }
//        return result;
//    }

    /**
     * Returns true if the needed codec is splittable. If no codec is needed
     * returns true as well.
     *
     * @param fs       the filesystem
     * @param filename the name of the file to be read
     * @return if the codec needed for reading the specified path is splittable.
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        CompressionCodecFactory factory = new CompressionCodecFactory(fs.getConf());
        CompressionCodec codec = factory.getCodec(filename);

        return null == codec || codec instanceof SplittableCompressionCodec;
    }
//
//    /**
//     * Proxy PathFilter that accepts a path only if all filters given in the
//     * constructor do. Used by the listPaths() to apply the built-in
//     * hiddenFileFilter together with a user provided one (if any).
//     */
//    private static class MultiPathFilter implements PathFilter {
//        private List<PathFilter> filters;
//
//        public MultiPathFilter(List<PathFilter> filters) {
//            this.filters = filters;
//        }
//
//        public boolean accept(Path path) {
//            for (PathFilter filter : filters) {
//                if (!filter.accept(path)) {
//                    return false;
//                }
//            }
//            return true;
//        }
//    }
}
