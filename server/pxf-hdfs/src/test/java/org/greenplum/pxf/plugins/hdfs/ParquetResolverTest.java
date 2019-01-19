package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.*;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class ParquetResolverTest {

    ParquetResolver resolver;
    RequestContext context;
    MessageType schema;

    @Before
    public void setup() {
        resolver = new ParquetResolver();
        context = new RequestContext();
        schema = new MessageType("test", new Type[]{});
        context.setMetadata(schema);
    }

    @Test
    public void testInitialize() {
        resolver.initialize(context);
    }

    @Test
    public void testGetFieldsPrimitive_EmptySchema() throws IOException {
        resolver.initialize(context);

        List<Group> groups = readParquetFile("primitive_types.parquet", 25);
        OneRow row1 = new OneRow(groups.get(0)); // get row 1
        List<OneField> fields = resolver.getFields(row1);
        assertTrue(fields.isEmpty());
    }

    @Test
    public void testGetFieldsPrimitive() throws IOException, ParseException {
        // TODO define correct schema corresponding to the dataset
        schema = getParquetSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        resolver.initialize(context);

        List<Group> groups = readParquetFile("primitive_types.parquet", 25);
        assertEquals(25, groups.size());

        List<OneField> fields = assertRow(groups, 0, 14);
        //s1 : "row1" : TEXT
        assertField(fields, 0, "row1", DataType.TEXT);
        assertField(fields, 1, "s_6", DataType.TEXT);
        assertField(fields, 2, 1, DataType.INTEGER);
        assertField(fields, 3, 6.0d, DataType.FLOAT8);
        assertField(fields, 4, BigDecimal.valueOf(1234560000000000000l, 18), DataType.NUMERIC);
        assertField(fields, 5, java.sql.Timestamp.valueOf(ZonedDateTime.parse("2013-07-13T21:00:05-07:00").toLocalDateTime()), DataType.TIMESTAMP);
        assertField(fields, 6, 7.7f, DataType.REAL);
        assertField(fields, 7, 23456789l, DataType.BIGINT);
        assertField(fields, 8, false, DataType.BOOLEAN);
        assertField(fields, 9, (short) 1, DataType.SMALLINT);
        assertField(fields, 10, (short) 10, DataType.SMALLINT);
        assertField(fields, 11, "abcd", DataType.TEXT);
        assertField(fields, 12, "abc", DataType.TEXT);
        assertField(fields, 13, new byte[]{(byte) 49}, DataType.BYTEA); // 49 is the ascii code for '1'

        // test nulls
        fields = assertRow(groups, 11, 14);
        assertField(fields, 1, null, DataType.TEXT);
        fields = assertRow(groups, 12, 14);
        assertField(fields, 2, null, DataType.INTEGER);
        fields = assertRow(groups, 13, 14);
        assertField(fields, 3, null, DataType.FLOAT8);
        fields = assertRow(groups, 14, 14);
        assertField(fields, 4, null, DataType.NUMERIC);
        fields = assertRow(groups, 15, 14);
        assertField(fields, 5, null, DataType.TIMESTAMP);
        fields = assertRow(groups, 16, 14);
        assertField(fields, 6, null, DataType.REAL);
        fields = assertRow(groups, 17, 14);
        assertField(fields, 7, null, DataType.BIGINT);
        fields = assertRow(groups, 18, 14);
        assertField(fields, 8, null, DataType.BOOLEAN);
        fields = assertRow(groups, 19, 14);
        assertField(fields, 9, null, DataType.SMALLINT);
        fields = assertRow(groups, 20, 14);
        assertField(fields, 10, null, DataType.SMALLINT);
        fields = assertRow(groups, 22, 14);
        assertField(fields, 11, null, DataType.TEXT);
        fields = assertRow(groups, 23, 14);
        assertField(fields, 12, null, DataType.TEXT);
        fields = assertRow(groups, 24, 14);
        assertField(fields, 13, null, DataType.BYTEA);
    }

    private List<OneField> assertRow(List<Group> groups, int desiredRow, int numFields) {
        OneRow row = new OneRow(groups.get(desiredRow)); // get row
        List<OneField> fields = resolver.getFields(row);
        assertEquals(numFields, fields.size());
        return fields;
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fields.get(index).val);
        } else {
            assertEquals(value, fields.get(index).val);
        }

    }

    private MessageType getParquetSchemaForPrimitiveTypes() {
        List<Type> fields = new ArrayList<>();

        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "s1", OriginalType.UTF8));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "s2", OriginalType.UTF8));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT32, "n1", null));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "d1", null));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16, "dc1", OriginalType.DECIMAL, new DecimalMetadata(38, 18), null));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT96, "tm", null));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "f", null));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT64, "bg", null));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "b", null));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT32, "tn", OriginalType.INT_8));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT32, "sml", OriginalType.INT_16));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "vc1", OriginalType.UTF8));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "bin", null));

        return new MessageType("hive_schema", fields);
    }

    private List<Group> readParquetFile(String file, long expectedSize) throws IOException {
        List<Group> result = new ArrayList<>();
        String parquetFile = getClass().getClassLoader().getResource("parquet/" + file).getPath();
        Path path = new Path(parquetFile);

        ParquetFileReader fileReader = new ParquetFileReader(new Configuration(), path, ParquetMetadataConverter.NO_FILTER);
        PageReadStore rowGroup;
        while ((rowGroup = fileReader.readNextRowGroup()) != null) {
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(schema));
            long rowCount = rowGroup.getRowCount();
            for (long i = 0; i < rowCount; i++) {
                result.add(recordReader.read());
            }
        }
        fileReader.close();
        assertEquals(expectedSize, result.size());
        return result;
    }
}
