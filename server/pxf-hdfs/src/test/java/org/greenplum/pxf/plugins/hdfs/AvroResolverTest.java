package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

public class AvroResolverTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private AvroResolver resolver;
    private RequestContext context;
    private Schema schema;

    @Before
    public void setup() {
        resolver = new AvroResolver();
        context = new RequestContext();
        context.setConfig("default");
        // initialize checks that accessor is some kind of avro accessor
        context.setAccessor("avro");
    }

    @Test
    public void testInitialize() {
        resolver.initialize(context);
    }

    @Test
    public void testSetFields_Primitive() throws Exception {
        schema = getAvroSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        resolver.initialize(context);

        List<OneField> fields = new ArrayList<>();
        fields.add(new OneField(DataType.BOOLEAN.getOID(), false));
        fields.add(new OneField(DataType.BYTEA.getOID(), new byte[]{(byte) 49}));
        fields.add(new OneField(DataType.BIGINT.getOID(), 23456789L));
        fields.add(new OneField(DataType.SMALLINT.getOID(), (short) 1));
        fields.add(new OneField(DataType.REAL.getOID(), 7.7f));
        fields.add(new OneField(DataType.FLOAT8.getOID(), 6.0d));
        fields.add(new OneField(DataType.TEXT.getOID(), "row1"));
        OneRow row = resolver.setFields(fields);

        assertNotNull(row);
        Object data = row.getData();
        assertNotNull(data);
        assertTrue(data instanceof GenericRecord);
        GenericRecord genericRecord = (GenericRecord) data;

        // assert column values
        assertEquals(false, genericRecord.get(0));
        assertArrayEquals(new byte[]{(byte) 49}, (byte[]) genericRecord.get(1));
        assertEquals(23456789L, genericRecord.get(2));
        assertEquals((short) 1, genericRecord.get(3));
        assertEquals((float) 7.7, genericRecord.get(4));
        assertEquals(6.0, genericRecord.get(5));
        assertEquals("row1", genericRecord.get(6));
    }

    @Test
    public void testGetFields_Primitive() throws Exception {
        schema = getAvroSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.initialize(context);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put(0, false);
        genericRecord.put(1, ByteBuffer.wrap(new byte[]{(byte) 49}));
        genericRecord.put(2, 23456789L);
        genericRecord.put(3, 1);
        genericRecord.put(4, 7.7f);
        genericRecord.put(5, 6.0d);
        genericRecord.put(6, "row1");
        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, false, DataType.BOOLEAN);
        assertField(fields, 1, new byte[]{(byte) 49}, DataType.BYTEA);
        assertField(fields, 2, 23456789L, DataType.BIGINT);
        assertField(fields, 3, 1, DataType.INTEGER); // shorts should become integers in Greenplum
        assertField(fields, 4, (float) 7.7, DataType.REAL);
        assertField(fields, 5, 6.0, DataType.FLOAT8);
        assertField(fields, 6, "row1", DataType.TEXT);
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fields.get(index).val);
        } else {
            assertEquals(value, fields.get(index).val);
        }
    }

    private Schema getAvroSchemaForPrimitiveTypes() {
        Schema schema = Schema.createRecord("tableName", "", "public.avro", false);
        List<Schema.Field> fields = new ArrayList<>();
        Schema.Type[] types = new Schema.Type[]{
                Schema.Type.BOOLEAN,
                Schema.Type.BYTES,
                Schema.Type.LONG,
                Schema.Type.INT,
                Schema.Type.FLOAT,
                Schema.Type.DOUBLE,
                Schema.Type.STRING,
        };
        for (Schema.Type type : types) {
            fields.add(new Schema.Field(type.getName(), getFieldSchema(type), "", null));
        }
        schema.setFields(fields);

        return schema;
    }

    private Schema getFieldSchema(Schema.Type type) {
        List<Schema> unionList = new ArrayList<>();
        unionList.add(Schema.create(Schema.Type.NULL));
        unionList.add(Schema.create(type));
        return Schema.createUnion(unionList);
    }

    private List<ColumnDescriptor> getColumnDescriptorsFromSchema(Schema schema) {
        return schema
                .getFields()
                .stream()
                .map(f -> {
                    AvroTypeConverter c = AvroTypeConverter.from(f.schema());
                    return new ColumnDescriptor(f.name(), c.getDataType().getOID(), 1, "", new Integer[]{});
                }).collect(Collectors.toList());
    }
}