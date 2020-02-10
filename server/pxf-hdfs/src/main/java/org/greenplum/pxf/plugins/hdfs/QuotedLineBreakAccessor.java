package org.greenplum.pxf.plugins.hdfs;

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


import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;

/**
 * A (atomic) PXF Accessor for reading \n delimited files with quoted
 * field delimiter, line delimiter, and quotes. This accessor supports
 * multi-line records, that are read from a single source (non-parallel).
 */
public class QuotedLineBreakAccessor extends HdfsAtomicDataAccessor {
    private boolean fileAsRow;
    private boolean firstLine, lastLine;

    BufferedReader reader;
    Queue<String> lineQueue;

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        // true if the files are read as a single row, false otherwise
        fileAsRow = StringUtils.equalsIgnoreCase("true", this.context.getOption("FILE_AS_ROW"));

        if (fileAsRow && this.context.getTupleDescription().size() != 1) {
            throw new IllegalArgumentException(String.format("the FILE_AS_ROW " +
                    "property only supports tables with a single column in " +
                    "the table definition. %d columns were provided",
                    this.context.getTupleDescription().size()));
        }
    }

    @Override
    public boolean openForRead() throws Exception {
        if (!super.openForRead()) {
            return false;
        }
        firstLine = true;
        reader = new BufferedReader(new InputStreamReader(inputStream));
        return true;
    }

    /**
     * Fetches one record (maybe partial) from the file. The record is returned as a Java object.
     */
    @Override
    public OneRow readNextObject() throws IOException {
        if (super.readNextObject() == null) /* check if working segment */ {
            return null;
        }

        String nextLine = readLine();
        if (nextLine == null) /* EOF */ {
            return null;
        }

        if (fileAsRow) {
            // Wrap text around quotes, and escape single quotes
            nextLine = context.getGreenplumCSV().toCsvField(nextLine, firstLine, lastLine);
            firstLine = false;
        }

        return new OneRow(null, nextLine);
    }

    /**
     * Read one line ahead, to determine when the last line occurs
     *
     * @return the next line
     */
    String readLine() throws IOException {
        if (!fileAsRow) {
            // simply readLine when fileAsRow feature is not enabled
            return reader.readLine();
        }

        String line;
        if (lineQueue == null) {
            line = reader.readLine();

            if (line == null) {
                lastLine = true;
                return null;
            }

            lineQueue = new LinkedList<>();
            lineQueue.offer(line);
        }

        line = reader.readLine();
        if (line != null) {
            lineQueue.offer(line);
        } else {
            lastLine = true;
        }
        return lineQueue.poll();
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     */
    @Override
    public boolean openForWrite() {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     */
    @Override
    public boolean writeNextObject(OneRow onerow) {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the resource for write.
     */
    @Override
    public void closeForWrite() {
        throw new UnsupportedOperationException();
    }
}
