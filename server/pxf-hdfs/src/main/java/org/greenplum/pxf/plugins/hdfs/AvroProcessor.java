package org.greenplum.pxf.plugins.hdfs;


import org.apache.avro.Schema;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroRecordReader;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.greenplum.pxf.api.model.QuerySplit;
import org.greenplum.pxf.plugins.hdfs.avro.AvroUtilities;

import java.io.IOException;
import java.util.Iterator;

/**
 * A PXF Processor to support Avro file records
 */
public class AvroProcessor extends HcfsSplittableDataProcessor<AvroWrapper<GenericRecord>, NullWritable, Schema> {

    private Schema readSchema;
    private AvroUtilities avroUtilities;

    /**
     * Constructs a new instance of AvroProcessor
     */
    public AvroProcessor() {
        super(new AvroInputFormat<>());
        avroUtilities = AvroUtilities.getInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RecordReader<AvroWrapper<GenericRecord>, NullWritable> getReader(JobConf conf, InputSplit split) throws IOException {
        return new AvroRecordReader<>(jobConf, (FileSplit) split);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<AvroWrapper<GenericRecord>> getTupleIterator(QuerySplit split) throws IOException {
        ensureReadSchemaInitialized(split);
        return new AvroTupleItr(split);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Object> getFields(GenericRecord tuple) throws IOException {
        return null;
    }


    private class AvroTupleItr extends RecordTupleItr {

        private AvroWrapper<GenericRecord> avroWrapper;

        public AvroTupleItr(QuerySplit split) {
            super(split);

            // Pass the schema to the AvroInputFormat
            AvroJob.setInputSchema(jobConf, readSchema);

            // The avroWrapper required for the iteration
            avroWrapper = new AvroWrapper<>();

        }

        @Override
        public GenericRecord next() {
            return super.next();
        }
    }

    private void ensureReadSchemaInitialized(QuerySplit split) {
        if (readSchema != null) return;

        Schema schema = querySession.getMetadata();
        if (schema == null) {
            if (split == null)
                throw new RuntimeException("Avro schema is unavailable");

            synchronized (querySession) {
                if ((schema = querySession.getMetadata()) == null) {
                    schema = avroUtilities.obtainSchema(context, configuration, hcfsType);
                    querySession.setMetadata(schema);
                }
            }
        }
        readSchema = schema;
    }
}
