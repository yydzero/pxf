package org.greenplum.pxf.plugins.hdfs.parquet;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.filter.Node;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ParquetRecordFilterBuilderTest extends ParquetBaseTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
}