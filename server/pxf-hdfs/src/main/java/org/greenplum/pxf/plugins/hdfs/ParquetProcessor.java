package org.greenplum.pxf.plugins.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BaseProcessor;
import org.greenplum.pxf.api.model.QuerySplit;
import org.greenplum.pxf.api.model.QuerySplitter;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverter;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public class ParquetProcessor extends BaseProcessor<Group> {

    private long rowsRead, totalRowsRead;
    private ObjectMapper mapper = new ObjectMapper();
    private MessageType readSchema;

    /**
     * Records the total read time in nanoseconds
     */
    private long totalReadTimeInNanos;

    @Override
    protected Iterator<Group> readTuples(QuerySplit split) throws IOException {
        Path file = new Path(context.getDataSource());
        FileSplit fileSplit = HdfsUtilities.parseFileSplit(context);

        // Read the original schema from the parquet file
        MessageType originalSchema = getSchema(file, fileSplit);
        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = getOriginalFieldsMap(originalSchema);
        // Get the read schema. This is either the full set or a subset (in
        // case of column projection) of the greenplum schema.
        readSchema = buildReadSchema(originalFieldsMap, originalSchema);
        // Get the record filter in case of predicate push-down
        FilterCompat.Filter recordFilter = getRecordFilter(context.getFilterString(), originalFieldsMap, readSchema);

        // add column projection
        configuration.set(PARQUET_READ_SCHEMA, readSchema.toString());

        ParquetReader<Group> fileReader = ParquetReader.builder(new GroupReadSupport(), file)
                .withConf(configuration)
                // Create reader for a given split, read a range in file
                .withFileRange(fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength())
                .withFilter(recordFilter)
                .build();
        context.setMetadata(readSchema);

        totalRowsRead += rowsRead;

        return new Iterator<Group>() {
            private Group group;

            @Override
            public boolean hasNext() {
                try {
                    final long then = System.nanoTime();
                    group = fileReader.read();
                    final long nanos = System.nanoTime() - then;
                    totalReadTimeInNanos += nanos;

                    if (group == null) {
                        closeForRead();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                return group != null;
            }

            @Override
            public Group next() {
                rowsRead++;
                return group;
            }

            private void closeForRead() throws IOException {
                if (LOG.isDebugEnabled()) {
                    final long millis = TimeUnit.NANOSECONDS.toMillis(totalReadTimeInNanos);
                    long average = totalReadTimeInNanos / totalRowsRead;
                    LOG.debug("{}-{}: Read TOTAL of {} rows from file {} on server {} in {} ms. Average speed: {} nanoseconds",
                            context.getTransactionId(),
                            context.getSegmentId(),
                            totalRowsRead,
                            context.getDataSource(),
                            context.getServerName(),
                            millis,
                            average);
                }
                if (fileReader != null) {
                    fileReader.close();
                }
            }
        };
    }

    @Override
    protected Object[] getFields(Group row) {
        int columnIndex = 0;

        Object[] results = new Object[context.getTupleDescription().size()];

        // schema is the readSchema, if there is column projection
        // the schema will be a subset of tuple descriptions
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        for (int i = 0; i < tupleDescription.size(); i++) {
            ColumnDescriptor columnDescriptor = tupleDescription.get(i);
            if (columnDescriptor.isProjected() && readSchema.getType(columnIndex).isPrimitive()) {
                results[i] = resolvePrimitive(row, columnIndex, readSchema.getType(columnIndex), 0);
                columnIndex++;
            } else {
                throw new UnsupportedOperationException("Parquet complex type support is not yet available.");
            }
        }
        return results;
    }

    @Override
    public QuerySplitter getQuerySplitter() {
        return new HcfsDataSplitter();
    }

    private Object resolvePrimitive(Group group, int columnIndex, Type type, int level) {
        Object value;

        // get type converter based on the primitive type
        ParquetTypeConverter converter = ParquetTypeConverter.from(type.asPrimitiveType());

        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        // at the top level (top field), non-repeated primitives will convert to typed OneField
        if (level == 0 && type.getRepetition() != REPEATED) {
            value = repetitionCount == 0 ? null : converter.getValue(group, columnIndex, 0, type);
        } else if (type.getRepetition() == REPEATED) {
            // repeated primitive at any level will convert into JSON
            ArrayNode jsonArray = mapper.createArrayNode();
            for (int repeatIndex = 0; repeatIndex < repetitionCount; repeatIndex++) {
                converter.addValueToJsonArray(group, columnIndex, repeatIndex, type, jsonArray);
            }
            // but will become a string only at top level
            if (level == 0) {
                try {
                    value = mapper.writeValueAsString(jsonArray);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize repeated parquet type " + type.asPrimitiveType().getName(), e);
                }
            } else {
                // just return the array node within OneField container
                value = jsonArray;
            }
        } else {
            // level > 0 and type != REPEATED -- primitive type as a member of complex group -- NOT YET SUPPORTED
            throw new UnsupportedOperationException("Parquet complex type support is not yet available.");
        }
        return value;
    }
}
