package org.greenplum.pxf.plugins.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.model.BaseProcessor;
import org.greenplum.pxf.api.model.QuerySplit;
import org.greenplum.pxf.api.model.QuerySplitter;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.FragmentMetadata;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetRecordFilterBuilder;
import org.greenplum.pxf.plugins.hdfs.parquet.ParquetTypeConverter;
import org.greenplum.pxf.plugins.hdfs.parquet.SupportedParquetPrimitiveTypePruner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.parquet.hadoop.api.ReadSupport.PARQUET_READ_SCHEMA;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public class ParquetProcessor extends BaseProcessor<Group> {

    private long rowsRead, totalRowsRead;
    private ObjectMapper mapper = new ObjectMapper();
    private MessageType readSchema;
    protected HcfsType hcfsType;

    public static final EnumSet<Operator> SUPPORTED_OPERATORS = EnumSet.of(
            Operator.NOOP,
            Operator.LESS_THAN,
            Operator.GREATER_THAN,
            Operator.LESS_THAN_OR_EQUAL,
            Operator.GREATER_THAN_OR_EQUAL,
            Operator.EQUALS,
            Operator.NOT_EQUALS,
            Operator.IS_NULL,
            Operator.IS_NOT_NULL,
            // Operator.IN,
            Operator.OR,
            Operator.AND,
            Operator.NOT
    );

    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    /**
     * Records the total read time in nanoseconds
     */
    private long totalReadTimeInNanos;

    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        hcfsType = HcfsType.getHcfsType(configuration, context);
    }

    @Override
    protected Iterator<Group> readTuples(QuerySplit split) throws IOException {
        Path file = new Path(hcfsType.getDataUri(configuration, context.getDataSource() + split.getResource()));
        FragmentMetadata metadata = deserializeFragmentMetadata(split.getMetadata());
        FileSplit fileSplit = new FileSplit(file, metadata.getStart(), metadata.getEnd(), (String[]) null);

        readSchema = getReadSchema(file, fileSplit);
        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = getOriginalFieldsMap(readSchema);
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


        totalRowsRead += rowsRead;

        return new Iterator<Group>() {
            private Group group;

            @Override
            public boolean hasNext() {
                if (group == null) {
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
                }
                return group != null;
            }

            @Override
            public Group next() {
                rowsRead++;
                Group result = group;
                group = null;
                return result;
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

    private synchronized MessageType getReadSchema(Path file, FileSplit fileSplit) throws IOException {
        if (readSchema != null) {
            return readSchema;
        }
        // Read the original schema from the parquet file
        MessageType originalSchema = getSchema(file, fileSplit);
        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = getOriginalFieldsMap(originalSchema);
        // Get the read schema. This is either the full set or a subset (in
        // case of column projection) of the greenplum schema.
        return buildReadSchema(originalFieldsMap, originalSchema);
    }

    @Override
    protected Iterator<Object> getFields(Group row) {
        // schema is the readSchema, if there is column projection
        // the schema will be a subset of tuple descriptions
        List<ColumnDescriptor> tupleDescription = context.getTupleDescription();
        final int totalColumns = tupleDescription.size();
        return new Iterator<Object>() {
            private int columnIndex = 0;
            private int i = 0;

            @Override
            public boolean hasNext() {
                return i < totalColumns;
            }

            @Override
            public Object next() {
                Object result;
                ColumnDescriptor columnDescriptor = tupleDescription.get(i++);
                if (!columnDescriptor.isProjected()) {
                    return null;
                } else if (readSchema.getType(columnIndex).isPrimitive()) {
                    result = resolvePrimitive(row, columnIndex, readSchema.getType(columnIndex), 0);
                    columnIndex++;
                } else {
                    throw new UnsupportedOperationException("Parquet complex type support is not yet available.");
                }
                return result;
            }
        };
    }

    @Override
    public QuerySplitter getQuerySplitter() {
        return new HcfsDataSplitter();
    }

    /**
     * Reads the original schema from the parquet file.
     *
     * @param parquetFile the path to the parquet file
     * @param fileSplit   the file split we are accessing
     * @return the original schema from the parquet file
     * @throws IOException when there's an IOException while reading the schema
     */
    private MessageType getSchema(Path parquetFile, FileSplit fileSplit) throws IOException {

        final long then = System.nanoTime();
        ParquetMetadataConverter.MetadataFilter filter = ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength());
        ParquetReadOptions parquetReadOptions = HadoopReadOptions
                .builder(configuration)
                .withMetadataFilter(filter)
                .build();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(parquetFile, configuration);
        try (ParquetFileReader parquetFileReader =
                     ParquetFileReader.open(inputFile, parquetReadOptions)) {
            FileMetaData metadata = parquetFileReader.getFileMetaData();
            if (LOG.isDebugEnabled()) {
                LOG.debug("{}-{}: Reading file {} with {} records in {} RowGroups",
                        context.getTransactionId(), context.getSegmentId(),
                        parquetFile.getName(), parquetFileReader.getRecordCount(),
                        parquetFileReader.getRowGroups().size());
            }
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
            LOG.debug("{}-{}: Read schema in {} ms", context.getTransactionId(),
                    context.getSegmentId(), millis);
            return metadata.getSchema();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Builds a map of names to Types from the original schema, the map allows
     * easy access from a given column name to the schema {@link Type}.
     *
     * @param originalSchema the original schema of the parquet file
     * @return a map of field names to types
     */
    private Map<String, Type> getOriginalFieldsMap(MessageType originalSchema) {
        Map<String, Type> originalFields = new HashMap<>(originalSchema.getFieldCount() * 2);

        // We need to add the original name and lower cased name to
        // the map to support mixed case where in GPDB the column name
        // was created with quotes i.e "mIxEd CaSe". When quotes are not
        // used to create a table in GPDB, the name of the column will
        // always come in lower-case
        originalSchema.getFields().forEach(t -> {
            String columnName = t.getName();
            originalFields.put(columnName, t);
            originalFields.put(columnName.toLowerCase(), t);
        });

        return originalFields;
    }

    /**
     * Generates a read schema when there is column projection
     *
     * @param originalFields a map of field names to types
     * @param originalSchema the original read schema
     */
    private MessageType buildReadSchema(Map<String, Type> originalFields, MessageType originalSchema) {
        List<Type> projectedFields = context.getTupleDescription().stream()
                .filter(ColumnDescriptor::isProjected)
                .map(c -> {
                    Type t = originalFields.get(c.columnName());
                    if (t == null) {
                        throw new IllegalArgumentException(
                                String.format("Column %s is missing from parquet schema", c.columnName()));
                    }
                    return t;
                })
                .collect(Collectors.toList());
        return new MessageType(originalSchema.getName(), projectedFields);
    }

    /**
     * Returns the parquet record filter for the given filter string
     *
     * @param filterString      the filter string
     * @param originalFieldsMap a map of field names to types
     * @param schema            the parquet schema
     * @return the parquet record filter for the given filter string
     */
    private FilterCompat.Filter getRecordFilter(String filterString, Map<String, Type> originalFieldsMap, MessageType schema) {
        if (StringUtils.isBlank(filterString)) {
            return FilterCompat.NOOP;
        }

        ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(
                context.getTupleDescription(), originalFieldsMap);
        TreeVisitor pruner = new SupportedParquetPrimitiveTypePruner(
                context.getTupleDescription(), originalFieldsMap, SUPPORTED_OPERATORS);

        try {
            // Parse the filter string into a expression tree Node
            Node root = new FilterParser().parse(filterString);
            // Prune the parsed tree with valid supported operators and then
            // traverse the pruned tree with the ParquetRecordFilterBuilder to
            // produce a record filter for parquet
            TRAVERSER.traverse(root, pruner, filterBuilder);
            return filterBuilder.getRecordFilter();
        } catch (Exception e) {
            LOG.error(String.format("%s-%d: %s--%s Unable to generate Parquet Record Filter for filter",
                    context.getTransactionId(),
                    context.getSegmentId(),
                    context.getDataSource(),
                    context.getFilterString()), e);
            return FilterCompat.NOOP;
        }
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

    private FragmentMetadata deserializeFragmentMetadata(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return new FragmentMetadata(buffer.getLong(), buffer.getLong(), null);
    }

}
