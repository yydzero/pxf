package org.greenplum.pxf.plugins.hive;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.greenplum.pxf.api.BasicFilter;
import org.greenplum.pxf.api.LogicalFilter;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.StatsAccessor;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Specialization of HiveAccessor for a Hive table that stores only ORC files.
 * This class replaces the generic HiveAccessor for a case where a table is stored entirely as ORC files.
 * Use together with {@link HiveInputFormatFragmenter}/{@link HiveColumnarSerdeResolver}
 */
public class HiveORCAccessor extends HiveAccessor implements StatsAccessor {

    private static final Log LOG = LogFactory.getLog(HiveORCAccessor.class);

    private final String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";
    private final String READ_ALL_COLUMNS = "hive.io.file.read.all.columns";
    private final String READ_COLUMN_NAMES_CONF_STR = "hive.io.file.readcolumn.names";
    private final String SARG_PUSHDOWN = "sarg.pushdown";
    protected Reader orcReader;

    private boolean useStats;
    private long count;
    private long objectsEmitted;
    private OneRow rowToEmitCount;

    private boolean statsInitialized;

    /**
     * Constructs a HiveORCFileAccessor.
     */
    public HiveORCAccessor() {
        super(new OrcInputFormat());
    }

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);
        useStats = Utilities.aggregateOptimizationsSupported(context);
    }

    @Override
    public boolean openForRead() throws Exception {
        if (useStats) {
            orcReader = getOrcReader();
            if (orcReader == null) {
                return false;
            }
            objectsEmitted = 0;
        } else {
            addColumns();
            addFilters();
        }
        return super.openForRead();
    }

    /**
     * Adds the table tuple description to JobConf ojbect
     * so only these columns will be returned.
     */
    private void addColumns() throws Exception {
        List<Integer> colIds = new ArrayList<Integer>();
        List<String> colNames = new ArrayList<String>();
        for(ColumnDescriptor col: context.getTupleDescription()) {
            if(col.isProjected()) {
                colIds.add(col.columnIndex());
                colNames.add(col.columnName());
            }
        }
        jobConf.set(READ_ALL_COLUMNS, "false");
        jobConf.set(READ_COLUMN_IDS_CONF_STR, StringUtils.join(colIds, ","));
        jobConf.set(READ_COLUMN_NAMES_CONF_STR, StringUtils.join(colNames, ","));
    }

    /**
     * Uses {@link HiveFilterBuilder} to translate a filter string into a
     * Hive {@link SearchArgument} object. The result is added as a filter to
     * JobConf object
     */
    private void addFilters() throws Exception {
        SearchArgument sarg = HiveUtilities.buildSearchArgument(context);
        if (sarg != null) {
            jobConf.set(SARG_PUSHDOWN, sarg.toKryo());
        }
    }

    /**
     * Fetches file-level statistics from an ORC file.
     */
    @Override
    public void retrieveStats() throws Exception {
        if (!this.useStats) {
            throw new IllegalStateException("Accessor is not using statistics in current context.");
        }
        /*
         * We are using file-level stats therefore if file has multiple splits,
         * it's enough to return count for a first split in file.
         * In case file has multiple splits - we don't want to duplicate counts.
         */
        if (context.getFragmentIndex() == 0) {
            this.count = this.orcReader.getNumberOfRows();
            rowToEmitCount = readNextObject();
        }
        statsInitialized = true;

    }

    /**
     * Emits tuple without reading from disk, currently supports COUNT
     */
    @Override
    public OneRow emitAggObject() {
        if(!statsInitialized) {
            throw new IllegalStateException("retrieveStats() should be called before calling emitAggObject()");
        }
        OneRow row = null;
        if (context.getAggType() == null)
            throw new UnsupportedOperationException("Aggregate opration is required");
        switch (context.getAggType()) {
            case COUNT:
                if (objectsEmitted < count) {
                    objectsEmitted++;
                    row = rowToEmitCount;
                }
                break;
            default: {
                throw new UnsupportedOperationException("Aggregation operation is not supported.");
            }
        }
        return row;
    }

}
