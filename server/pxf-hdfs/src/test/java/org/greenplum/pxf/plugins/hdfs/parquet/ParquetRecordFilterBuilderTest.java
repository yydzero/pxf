package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNull;

public class ParquetRecordFilterBuilderTest {

    private static final TreeTraverser TRAVERSER = new TreeTraverser();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Map<String, Type> originalFieldsMap;
    private List<ColumnDescriptor> columnDescriptors;

    @Before
    public void setup() {
        columnDescriptors = new ArrayList<>();
        columnDescriptors.add(new ColumnDescriptor("id", DataType.INTEGER.getOID(), 0, "int4", null));
        columnDescriptors.add(new ColumnDescriptor("name", DataType.TEXT.getOID(), 1, "text", null));
        columnDescriptors.add(new ColumnDescriptor("cdate", DataType.DATE.getOID(), 2, "date", null));
        columnDescriptors.add(new ColumnDescriptor("amt", DataType.FLOAT8.getOID(), 3, "float8", null));
        columnDescriptors.add(new ColumnDescriptor("grade", DataType.TEXT.getOID(), 4, "text", null));
        columnDescriptors.add(new ColumnDescriptor("b", DataType.BOOLEAN.getOID(), 5, "bool", null));
        columnDescriptors.add(new ColumnDescriptor("tm", DataType.TIMESTAMP.getOID(), 6, "timestamp", null));
        columnDescriptors.add(new ColumnDescriptor("bg", DataType.BIGINT.getOID(), 7, "bigint", null));
        columnDescriptors.add(new ColumnDescriptor("bin", DataType.BYTEA.getOID(), 8, "bytea", null));
        columnDescriptors.add(new ColumnDescriptor("sml", DataType.SMALLINT.getOID(), 9, "int2", null));
        columnDescriptors.add(new ColumnDescriptor("r", DataType.REAL.getOID(), 10, "real", null));
        columnDescriptors.add(new ColumnDescriptor("vc1", DataType.VARCHAR.getOID(), 11, "varchar", new Integer[]{5}));
        columnDescriptors.add(new ColumnDescriptor("c1", DataType.BPCHAR.getOID(), 12, "char", new Integer[]{3}));
        columnDescriptors.add(new ColumnDescriptor("dec1", DataType.NUMERIC.getOID(), 13, "numeric", null));
        columnDescriptors.add(new ColumnDescriptor("dec2", DataType.NUMERIC.getOID(), 14, "numeric", new Integer[]{5, 2}));
        columnDescriptors.add(new ColumnDescriptor("dec3", DataType.NUMERIC.getOID(), 15, "numeric", new Integer[]{13, 5}));
        columnDescriptors.add(new ColumnDescriptor("num1", DataType.INTEGER.getOID(), 16, "int", null));

        MessageType schema = MessageTypeParser.parseMessageType("message hive_schema {\n" +
                "  optional int32 id;\n" +
                "  optional binary name (UTF8);\n" +
                "  optional int32 cdate (DATE);\n" +
                "  optional double amt;\n" +
                "  optional binary grade (UTF8);\n" +
                "  optional boolean b;\n" +
                "  optional int96 tm;\n" +
                "  optional int64 bg;\n" +
                "  optional binary bin;\n" +
                "  optional int32 sml (INT_16);\n" +
                "  optional float r;\n" +
                "  optional binary vc1 (UTF8);\n" +
                "  optional binary c1 (UTF8);\n" +
                "  optional fixed_len_byte_array(16) dec1 (DECIMAL(38,18));\n" +
                "  optional fixed_len_byte_array(3) dec2 (DECIMAL(5,2));\n" +
                "  optional fixed_len_byte_array(6) dec3 (DECIMAL(13,5));\n" +
                "  optional int32 num1;\n" +
                "}");

        originalFieldsMap = getOriginalFieldsMap(schema);
    }

    @Test
    public void testUnsupportedOperationError() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("not supported IN");

        // a16 in (11, 12)
        helper("a16m1007s2d11s2d12o10");
    }

    @Test
    public void testUnsupportedINT96EqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6c1114s19d2013-07-23 21:00:00o5");
    }

    @Test
    public void testUnsupportedINT96LessThanFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6c1114s19d2013-07-23 21:00:00o1");
    }

    @Test
    public void testUnsupportedINT96GreaterThanFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6c1114s19d2013-07-23 21:00:00o2");
    }

    @Test
    public void testUnsupportedINT96LessThanOrEqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6c1114s19d2013-07-23 21:00:00o3");
    }

    @Test
    public void testUnsupportedINT96GreaterThanOrEqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6c1114s19d2013-07-23 21:00:00o4");
    }

    @Test
    public void testUnsupportedINT96NotEqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6c1114s19d2013-07-23 21:00:00o6");
    }

    @Test
    public void testUnsupportedINT96IsNullFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6o8");
    }

    @Test
    public void testUnsupportedINT96IsNotNullFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column tm of type INT96 is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a6o9");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayEqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // dec2 = 0
        helper("a14c23s1d0o5");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayLessThanFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // dec2 = 0
        helper("a14c23s1d0o1");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayGreaterThanFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // dec2 = 0
        helper("a14c23s1d0o2");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayLessThanOrEqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // dec2 = 0
        helper("a14c23s1d0o3");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayGreaterThanOrEqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // dec2 = 0
        helper("a14c23s1d0o4");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayNotEqualsFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // dec2 = 0
        helper("a14c23s1d0o6");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayIsNullFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a14o8");
    }

    @Test
    public void testUnsupportedFixedLenByteArrayIsNotNullFilter() throws Exception {
        thrown.expect(UnsupportedOperationException.class);
        thrown.expectMessage("Column dec2 of type FIXED_LEN_BYTE_ARRAY is not supported");

        // tm = '2013-07-23 21:00:00'
        helper("a14o9");
    }

    private ParquetRecordFilterBuilder helper(String filterString) throws Exception {

        ParquetRecordFilterBuilder filterBuilder = new ParquetRecordFilterBuilder(
                columnDescriptors, originalFieldsMap);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // traverse the tree with the ParquetRecordFilterBuilder to
        // produce a record filter for parquet
        TRAVERSER.traverse(root, filterBuilder);

        return filterBuilder;
    }

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
}