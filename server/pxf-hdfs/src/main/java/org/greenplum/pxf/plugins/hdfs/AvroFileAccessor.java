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


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.greenplum.pxf.api.io.DataType.UNSUPPORTED_TYPE;
import static org.greenplum.pxf.api.io.DataType.isArrayType;

/**
 * A PXF Accessor for reading Avro File records
 */
public class AvroFileAccessor extends HdfsSplittableDataAccessor {
    private static String COMMON_NAMESPACE = "public.avro";

    private AvroWrapper<GenericRecord> avroWrapper;
    private DataFileWriter<GenericRecord> writer;
    private long rowsWritten, totalRowsWritten;
    private Schema schema;

    /**
     * Constructs a new instance of the AvroFileAccessor
     */
    public AvroFileAccessor() {
        super(new AvroInputFormat<GenericRecord>());
    }

    /*
     * Initializes an AvroFileAccessor.
     *
     * We need schema to be read or generated before
     * AvroResolver#initialize() is called so that
     * AvroResolver#fields can be set.
     *
     * for READ:
     * creates the job configuration and accesses the data
     * source avro file or a user-provided path to an avro file
     * to fetch the avro schema.
     *
     * for WRITE:
     * We get the schema either from a user-provided path to an
     * avro file or by generating it on the fly.
     */
    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        // 1. Accessing the avro file through the "unsplittable" API just to get the schema.
        //    The splittable API (AvroInputFormat) which is the one we will be using to fetch
        //    the records, does not support getting the avro schema yet.
        try {
            readOrGenerateAvroSchema();
        } catch (IOException e) {
            throw new RuntimeException("Failed to obtain Avro schema for " + context.getDataSource(), e);
        }

        // 2. Add schema to RequestContext's metadata to avoid computing it again in the resolver
        requestContext.setMetadata(schema);
    }

    @Override
    public boolean openForRead() throws Exception {
        boolean ret = super.openForRead();
        // Pass the schema to the AvroInputFormat
        AvroJob.setInputSchema(jobConf, schema);

        // The avroWrapper required for the iteration
        avroWrapper = new AvroWrapper<>();

        return ret;
    }

    @Override
    protected Object getReader(JobConf jobConf, InputSplit split) throws IOException {
        return new AvroRecordReader<>(jobConf, (FileSplit) split);
    }

    /**
     * readNextObject
     * The AVRO accessor is currently the only specialized accessor that
     * overrides this method. This happens, because the special
     * AvroRecordReader.next() semantics (use of the AvroWrapper), so it
     * cannot use the RecordReader's default implementation in
     * SplittableFileAccessor
     */
    @Override
    public OneRow readNextObject() throws IOException {
        /** Resetting datum to null, to avoid stale bytes to be padded from the previous row's datum */
        avroWrapper.datum(null);
        if (reader.next(avroWrapper, NullWritable.get())) { // There is one more record in the current split.
            return new OneRow(null, avroWrapper.datum());
        } else if (getNextSplit()) { // The current split is exhausted. try to move to the next split.
            return reader.next(avroWrapper, NullWritable.get())
                    ? new OneRow(null, avroWrapper.datum())
                    : null;
        }

        // if neither condition was met, it means we already read all the records in all the splits, and
        // in this call record variable was not set, so we return null and thus we are signaling end of
        // records sequence - in this case avroWrapper.datum() will be null
        return null;
    }

    /**
     * Opens the resource for write.
     *
     * @return true if the resource is successfully opened
     * @throws Exception if opening the resource failed
     */
    @Override
    public boolean openForWrite() throws Exception {
        // make writer
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        writer = new DataFileWriter<>(datumWriter);
        Path file = new Path(hcfsType.getUriForWrite(configuration, context, true) + ".avro");
        FileSystem fs = file.getFileSystem(jobConf);
        FSDataOutputStream avroOut = fs.create(file, false);
        writer.create(schema, avroOut);

        return true;
    }

    private void readOrGenerateAvroSchema() throws IOException {
        String userProvidedSchemaFile = context.getOption("DATA-SCHEMA");
        // user-provided schema trumps everything
        if (userProvidedSchemaFile != null) {
            schema = readAvroSchema(jobConf, userProvidedSchemaFile);
            return;
        }

        // if we are writing we must generate the schema if there is none to read
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            schema = generateAvroSchema(context.getTupleDescription());
            return;
        }

        // reading from external: get the schema from data source
        schema = readAvroSchema(jobConf, context.getDataSource());
}

    /**
     * Writes the next object.
     *
     * @param onerow the object to be written
     * @return true if the write succeeded
     * @throws Exception writing to the resource failed
     */
    @Override
    public boolean writeNextObject(OneRow onerow) throws Exception {
        writer.append((GenericRecord) onerow.getData());
        rowsWritten++;
        return true;
    }

    /**
     * Closes the resource for write.
     *
     * @throws Exception if closing the resource failed
     */
    @Override
    public void closeForWrite() throws Exception {
        if (writer != null) {
            writer.close();
            totalRowsWritten += rowsWritten;
        }
        LOG.debug("Wrote a TOTAL of {} rows", totalRowsWritten);
    }

    /**
     * Accessing the Avro file through the "unsplittable" API just to get the
     * schema. The splittable API (AvroInputFormat) which is the one we will be
     * using to fetch the records, does not support getting the Avro schema yet.
     *
     * @param conf       Hadoop configuration
     * @param dataSource Avro file (i.e fileName.avro) path
     * @return the Avro schema
     * @throws IOException if I/O error occurred while accessing Avro schema file
     */
    Schema readAvroSchema(Configuration conf, String dataSource)
            throws IOException {
        Schema schema;
        FsInput inStream = new FsInput(new Path(dataSource), conf);
        DatumReader<GenericRecord> dummyReader = new GenericDatumReader<>();
        // try-with-resources will take care of closing the readers
        try (DataFileReader<GenericRecord> dummyFileReader = new DataFileReader<>(
                inStream, dummyReader)) {
            schema = dummyFileReader.getSchema();
        }
        return schema;
    }

    Schema generateAvroSchema(List<ColumnDescriptor> tupleDescription) throws IOException {
        String colName;
        int colType;

        Schema schema = Schema.createRecord("tableName", "", COMMON_NAMESPACE, false);
        List<Schema.Field> fields = new ArrayList<>();

        for (ColumnDescriptor cd : tupleDescription) {
            colName = cd.columnName();
            colType = cd.columnTypeCode();
            // String delim = context.getOption("delimiter");
            // columnDelimiter = delim == null ? ',' : delim.charAt(0);
            fields.add(new Schema.Field(colName, getFieldSchema(DataType.get(colType), false, 1), "", null));
        }

        schema.setFields(fields);

        return schema;
    }

    private Schema getFieldSchema(DataType type, boolean notNull, int dim) throws IOException {
        List<Schema> unionList = new ArrayList<>();
        // in this version of gpdb, external table should not set 'notnull' attribute
        unionList.add(Schema.create(Schema.Type.NULL));

        switch (type) {
            case BOOLEAN:
                unionList.add(Schema.create(Schema.Type.BOOLEAN));
                break;
            case BYTEA:
                unionList.add(Schema.create(Schema.Type.BYTES));
                break;
            case BIGINT:
                unionList.add(Schema.create(Schema.Type.LONG));
                break;
            case SMALLINT:
            case INTEGER:
                unionList.add(Schema.create(Schema.Type.INT));
                break;
            case REAL:
                unionList.add(Schema.create(Schema.Type.FLOAT));
                break;
            case FLOAT8:
                unionList.add(Schema.create(Schema.Type.DOUBLE));
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                break;
            case VARCHAR:
            case BPCHAR:
            case NUMERIC:
            case DATE:
            case TIME:
            case TIMESTAMP:
            case TEXT:
                unionList.add(Schema.create(Schema.Type.STRING));
                break;
            case INT2ARRAY:
            case INT4ARRAY:
            case INT8ARRAY:
            case BOOLARRAY:
            case TEXTARRAY:
            default:
                if (type == UNSUPPORTED_TYPE) {
                    throw new UnsupportedTypeException("Unsupported type");
                }
                if (!isArrayType(type.getOID())) {
                    unionList.add(Schema.create(Schema.Type.STRING));
                    break;
                }
                // array or other variable length types
                DataType elementType = type.getTypeElem();
                Schema array = Schema.createArray(getFieldSchema(elementType, notNull, 0));
                // for multi-dim array
                for (int i = 1; i < dim; i++) {
                    array = Schema.createArray(array);
                }
                unionList.add(array);

                break;
        }

        return Schema.createUnion(unionList);
    }
}
