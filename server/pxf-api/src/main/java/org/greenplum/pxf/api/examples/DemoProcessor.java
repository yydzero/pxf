package org.greenplum.pxf.api.examples;

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

import com.google.common.collect.Lists;
import org.greenplum.pxf.api.model.BaseProcessor;
import org.greenplum.pxf.api.model.QuerySplit;
import org.greenplum.pxf.api.model.QuerySplitter;

import java.util.Iterator;

/**
 * Internal interface that would defined the access to a file on HDFS, but in
 * this case contains the data required.
 * <p>
 * Demo implementation
 */
public class DemoProcessor extends BaseProcessor<String> {

    private static final int NUM_ROWS = 500000;

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<String> readTuples(QuerySplit split) {
        final String fragmentMetadata = new String(split.getMetadata());
        final int colCount = context.getColumns();

        return new Iterator<String>() {
            private int rowNumber;
            private StringBuilder colValue = new StringBuilder();

            @Override
            public boolean hasNext() {
                return rowNumber < NUM_ROWS;
            }

            @Override
            public String next() {
                colValue.setLength(0);
                colValue.append(fragmentMetadata).append(" row").append(rowNumber + 1);
                for (int colIndex = 1; colIndex < colCount; colIndex++) {
                    colValue.append("|").append("value").append(colIndex);
                }
                rowNumber++;
                return colValue.toString();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<Object> getFields(String row) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QuerySplitter getQuerySplitter() {
        return new DemoQuerySplitter();
    }
}
