package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Test
    public void testGetFields_PrimitiveNulls() throws Exception {
        schema = getAvroSchemaForPrimitiveTypes();
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.initialize(context);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put(0, null);
        genericRecord.put(1, null);
        genericRecord.put(2, null);
        genericRecord.put(3, null);
        genericRecord.put(4, null);
        genericRecord.put(5, null);
        genericRecord.put(6, null);
        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));

        assertField(fields, 0, null, DataType.BOOLEAN);
        assertField(fields, 1, null, DataType.BYTEA);
        assertField(fields, 2, null, DataType.BIGINT);
        assertField(fields, 3, null, DataType.INTEGER); // shorts should become integers in Greenplum
        assertField(fields, 4, null, DataType.REAL);
        assertField(fields, 5, null, DataType.FLOAT8);
        assertField(fields, 6, null, DataType.TEXT);
    }

    @Test
    public void getFields_ComplexTypes() throws Exception {
        schema = getAvroSchemaForComplexTypes();
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.initialize(context);
        GenericRecord genericRecord = new GenericData.Record(schema);
        // UNION of NULL and BYTES
        genericRecord.put(0, null);

        // Record with a float, int and a string
        final GenericData.Record nestedRecord = new GenericData.Record(schema.getFields().get(1).schema());
        nestedRecord.put("float", 7.7f);
        nestedRecord.put("int", 7);
        nestedRecord.put("string", "seven");
        genericRecord.put(1, nestedRecord);

        // ARRAY of strings
        final ArrayList<String> strings = new ArrayList<String>() {{
            add("one");
            add("two");
            add("three");
        }};
        genericRecord.put(2, new GenericData.Array<>(schema.getFields().get(2).schema(), strings));
        // ENUM of card suites
        genericRecord.put(3, new GenericData.EnumSymbol(schema.getFields().get(3).schema(), "DIAMONDS"));

        // FIXED 4 byte string
        genericRecord.put(4, new GenericData.Fixed(schema.getFields().get(4).schema(), new byte[]{70, 79, 79, 66, 65, 82}));

        // MAP of string to long
        Map<String, Long> map  = new HashMap<String, Long>() {{
            put("key1", 123456789L);
            put("key2", 234567890L);
        }};
        genericRecord.put(5, map);

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));
        assertField(fields, 0, null, DataType.BYTEA);
        assertField(fields, 1, "{float:7.7,int:7,string:seven}", DataType.TEXT);
        assertField(fields, 2, "[one,two,three]", DataType.TEXT);
        assertField(fields, 3, "DIAMONDS", DataType.TEXT);
        assertField(fields, 4, new byte[]{'F','O','O','B','A','R'}, DataType.BYTEA);
        assertField(fields, 5, "{key1:123456789,key2:234567890}", DataType.TEXT);
    }

    @Test
    public void getFields_ComplexTypesNulls() throws Exception {
        schema = getAvroSchemaForComplexTypes();
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        resolver.initialize(context);
        GenericRecord genericRecord = new GenericData.Record(schema);
        // UNION of NULL and BYTES
        genericRecord.put(0, null);

        // Record with a float, int and a string
        genericRecord.put(1, null);

        // ARRAY of strings
        genericRecord.put(2, null);

        // ENUM of card suites
        genericRecord.put(3, null);

        // FIXED 4 byte string
        genericRecord.put(4, null);

        // MAP of string to long
        genericRecord.put(5, null);

        List<OneField> fields = resolver.getFields(new OneRow(null, genericRecord));
        assertField(fields, 0, null, DataType.BYTEA);
        assertField(fields, 1, null, DataType.TEXT);
        assertField(fields, 2, null, DataType.TEXT);
        assertField(fields, 3, null, DataType.TEXT);
        assertField(fields, 4, null, DataType.BYTEA);
        assertField(fields, 5, null, DataType.TEXT);
    }

    private void assertField(List<OneField> fields, int index, Object value, DataType type) {
        assertEquals(type.getOID(), fields.get(index).type);
        if (type == DataType.BYTEA) {
            assertArrayEquals((byte[]) value, (byte[]) fields.get(index).val);
            return;
        }

        if (fields.get(index).val instanceof GenericData.EnumSymbol) {
            assertEquals(value, fields.get(index).val.toString());
            return;
        }
        assertEquals(value, fields.get(index).val);
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
            fields.add(new Schema.Field(type.getName(), Schema.create(type), "", null));
        }
        schema.setFields(fields);

        return schema;
    }

    private Schema getAvroSchemaForComplexTypes() {
        Schema schema = Schema.createRecord("tableName", "", "public.avro", false);
        List<Schema.Field> fields = new ArrayList<>();

        // add a UNION of NULL with BYTES
        fields.add(new Schema.Field(
                Schema.Type.UNION.getName(),
                createUnion(Schema.Type.BYTES),
                "",
                null)
        );
        // add a RECORD with a float, int, and string inside
        fields.add(new Schema.Field(
                Schema.Type.RECORD.getName(),
                createRecord(new Schema.Type[]{Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.STRING}),
                "",
                null)
        );
        // add an ARRAY of strings
        fields.add(new Schema.Field(
                Schema.Type.ARRAY.getName(),
                Schema.createArray(Schema.create(Schema.Type.STRING)),
                "",
                null)
        );
        // add an ENUM of card suites
        fields.add(new Schema.Field(
                Schema.Type.ENUM.getName(),
                createEnum("suites", new String[]{"SPADES", "HEARTS", "DIAMONDS", "CLUBS"}),
                "",
                null)
        );
        // add a FIXED with 4 byte length
        fields.add(new Schema.Field(
                Schema.Type.FIXED.getName(),
                Schema.createFixed("fixed", "", null, 6),
                "",
                null)
        );
        // add a MAP from string to long
        fields.add(new Schema.Field(
                Schema.Type.MAP.getName(),
                Schema.createMap(Schema.create(Schema.Type.LONG)),
                "",
                null)
        );
        schema.setFields(fields);

        return schema;
    }

    private Schema createEnum(String name, String[] symbols) {
        List<String> values = new ArrayList<>();
        for (String sym : symbols) {
            values.add(sym);
        }
        return Schema.createEnum("enum", "", null, values);
    }

    private Schema createRecord(Schema.Type[] types) {
        List<Schema.Field> fields = new ArrayList<>();
        for (Schema.Type type : types) {
            fields.add(new Schema.Field(type.getName(), Schema.create(type), "", null));
        }
        return Schema.createRecord(fields);
    }

    private Schema createUnion(Schema.Type type) {
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
                    return new ColumnDescriptor(f.name(), c.getDataType(f.schema(), null).getOID(), 1, "", new Integer[]{});
                }).collect(Collectors.toList());
    }
}