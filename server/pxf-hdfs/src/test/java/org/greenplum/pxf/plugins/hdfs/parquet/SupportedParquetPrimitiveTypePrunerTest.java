package org.greenplum.pxf.plugins.hdfs.parquet;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeTraverser;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SupportedParquetPrimitiveTypePrunerTest {

    private static final TreeTraverser TRAVERSER = new TreeTraverser();

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
    public void testIntegerFilter() throws Exception {
        // a16 = 11
        Node result = helper("a16c23s2d11o5");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.EQUALS, operatorNode.getOperator());
        assertTrue(operatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(16, ((ColumnIndexOperandNode) operatorNode.getLeft()).index());
        assertTrue(operatorNode.getRight() instanceof OperandNode);
        assertEquals("11", operatorNode.getRight().toString());
    }

    @Test
    public void testNotBooleanFilter() throws Exception {
        // NOT (a5 == true)
        Node result = helper("a5c16s4dtrueo0l2");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.NOT, operatorNode.getOperator());
        assertTrue(result.getLeft() instanceof OperatorNode);
        OperatorNode noopOperatorNode = (OperatorNode) result.getLeft();
        assertEquals(Operator.NOOP, noopOperatorNode.getOperator());
        assertTrue(noopOperatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(5, ((ColumnIndexOperandNode) noopOperatorNode.getLeft()).index());
        assertTrue(noopOperatorNode.getRight() instanceof OperandNode);
        assertEquals("true", noopOperatorNode.getRight().toString());
    }

    @Test
    public void testUnsupportedINT96Filter() throws Exception {
        // tm = '2013-07-23 21:00:00' -> null
        Node result = helper("a6c1114s19d2013-07-23 21:00:00o5");
        assertNull(result);

        // name = 'row2' and tm = '2013-07-23 21:00:00' -> name = 'row2'
        result = helper("a1c25s4drow2o5a6c1114s19d2013-07-23 21:00:00o5l0"
        );
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.EQUALS, operatorNode.getOperator());
        assertTrue(operatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(1, ((ColumnIndexOperandNode) operatorNode.getLeft()).index());
        assertTrue(operatorNode.getRight() instanceof OperandNode);
        assertEquals("row2", operatorNode.getRight().toString());

        // name = 'row2' or tm = '2013-07-23 21:00:00' -> null
        result = helper("a1c25s4drow2o5a6c1114s19d2013-07-23 21:00:00o5l1"
        );
        assertNull(result);
    }

    @Test
    public void testUnsupportedFixedLenByteArrayFilter() throws Exception {
        // dec2 = 0
        Node result = helper("a14c23s1d0o5");
        assertNull(result);

        // name = 'row2' and dec2 = 0 -> name = 'row2'
        result = helper("a1c25s4drow2o5a14c23s1d0o5l0");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.EQUALS, operatorNode.getOperator());
        assertTrue(operatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(1, ((ColumnIndexOperandNode) operatorNode.getLeft()).index());
        assertTrue(operatorNode.getRight() instanceof OperandNode);
        assertEquals("row2", operatorNode.getRight().toString());

        // name = 'row2' or dec2 = 0 -> null
        result = helper("a1c25s4drow2o5a14c23s1d0o5l1");
        assertNull(result);
    }

    @Test
    public void testUnsupportedInOperationFilter() throws Exception {
        // a16 in (11, 12)
        Node result = helper("a16m1007s2d11s2d12o10");
        assertNull(result);
    }

    private Node helper(String filterString) throws Exception {

        TreeVisitor pruner = new SupportedParquetPrimitiveTypePruner(
                columnDescriptors, originalFieldsMap, ParquetFileAccessor.SUPPORTED_OPERATORS);

        // Parse the filter string into a expression tree Node
        Node root = new FilterParser().parse(filterString);
        // Prune the parsed tree with valid supported operators and then
        // traverse the pruned tree with the ParquetRecordFilterBuilder to
        // produce a record filter for parquet
        return TRAVERSER.traverse(root, pruner);
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