package org.greenplum.pxf.plugins.hdfs.parquet;

import org.greenplum.pxf.api.filter.ColumnIndexOperandNode;
import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.greenplum.pxf.api.filter.OperandNode;
import org.greenplum.pxf.api.filter.Operator;
import org.greenplum.pxf.api.filter.OperatorNode;
import org.greenplum.pxf.api.filter.TreeVisitor;
import org.greenplum.pxf.plugins.hdfs.ParquetFileAccessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SupportedParquetPrimitiveTypePrunerTest extends ParquetBaseTest {

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
        result = helper("a1c25s4drow2o5a6c1114s19d2013-07-23 21:00:00o5l0");
        assertNotNull(result);
        assertTrue(result instanceof OperatorNode);
        OperatorNode operatorNode = (OperatorNode) result;
        assertEquals(Operator.EQUALS, operatorNode.getOperator());
        assertTrue(operatorNode.getLeft() instanceof ColumnIndexOperandNode);
        assertEquals(1, ((ColumnIndexOperandNode) operatorNode.getLeft()).index());
        assertTrue(operatorNode.getRight() instanceof OperandNode);
        assertEquals("row2", operatorNode.getRight().toString());

        // name = 'row2' or tm = '2013-07-23 21:00:00' -> null
        result = helper("a1c25s4drow2o5a6c1114s19d2013-07-23 21:00:00o5l1");
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
}