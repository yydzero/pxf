package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ParquetSchemaUtility {

    protected Logger LOG = LoggerFactory.getLogger(this.getClass());

    /**
     * Retrieves the original schema from the parquet file split, and returns
     * the read schema for the projected columns in the query.
     *
     * @param fileSplit         the file split we are accessing
     * @param configuration     the hadoop configuration
     * @param columnDescriptors the list of column descriptors
     * @return the read schema for the given query
     * @throws IOException when there's an IOException while reading the schema
     */
    public MessageType getReadSchema(FileSplit fileSplit, Configuration configuration, List<ColumnDescriptor> columnDescriptors) throws IOException {
        // Read the original schema from the parquet file
        MessageType originalSchema = getOriginalSchema(fileSplit, configuration);
        // Get a map of the column name to Types for the given schema
        Map<String, Type> originalFieldsMap = getOriginalFieldsMap(originalSchema);
        // Get the read schema in case of column projection
        return buildReadSchema(originalFieldsMap, originalSchema, columnDescriptors);
    }

    /**
     * Reads the original schema from the parquet file.
     *
     * @param fileSplit     the file split we are accessing
     * @param configuration the hadoop configuration
     * @return the original schema from the parquet file
     * @throws IOException when there's an IOException while reading the schema
     */
    public MessageType getOriginalSchema(FileSplit fileSplit, Configuration configuration) throws IOException {

        final long then = System.nanoTime();
        ParquetMetadataConverter.MetadataFilter filter = ParquetMetadataConverter.range(
                fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength());
        ParquetReadOptions parquetReadOptions = HadoopReadOptions
                .builder(configuration)
                .withMetadataFilter(filter)
                .build();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(fileSplit.getPath(), configuration);
        try (ParquetFileReader parquetFileReader =
                     ParquetFileReader.open(inputFile, parquetReadOptions)) {
            FileMetaData metadata = parquetFileReader.getFileMetaData();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reading file {} with {} records in {} RowGroups",
                        fileSplit.getPath().getName(), parquetFileReader.getRecordCount(),
                        parquetFileReader.getRowGroups().size());
            }
            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - then);
            LOG.info("Read schema in " + millis + "ms");
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
    public Map<String, Type> getOriginalFieldsMap(MessageType originalSchema) {
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
     * @param originalFields    a map of field names to types
     * @param originalSchema    the original read schema
     * @param columnDescriptors the list of column descriptors
     */
    public MessageType buildReadSchema(Map<String, Type> originalFields, MessageType originalSchema, List<ColumnDescriptor> columnDescriptors) {
        List<Type> projectedFields = columnDescriptors.stream()
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

}
