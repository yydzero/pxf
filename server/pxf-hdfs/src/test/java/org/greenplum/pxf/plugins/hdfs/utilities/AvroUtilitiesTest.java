package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AvroUtilitiesTest {
    private RequestContext context;
    private Schema schema;
    private Schema testSchema;
    private String avroDirectory;
    private Configuration configuration;

    @Before
    public void setup() {
        avroDirectory = this.getClass().getClassLoader().getResource("avro/").getPath();

        context = new RequestContext();
        configuration = new Configuration();

        context.setDataSource(avroDirectory + "test.avro");

        testSchema = generateTestSchema();
    }

    @Test
    public void testObtainSchemaOnRead() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "example_schema");
    }

    @Test
    public void testObtainSchemaOnReadWithUserProvidedSchema() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnReadWithUserProvidedSchemaJson() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWrite() {
        context.setTupleDescription(AvroTypeConverter.getColumnDescriptorsFromSchema(testSchema));
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifyGeneratedSchema(schema);
    }

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchema() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");
        context.setDataSource(avroDirectory);

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchemaJson() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");
        context.setDataSource(avroDirectory);

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    /**
     * Helper method for testing schema
     *
     * @param schema
     */
    private static void verifySchema(Schema schema, String name) {
        assertNotNull(schema);
        assertEquals(Schema.Type.RECORD, schema.getType());
        assertEquals(name, schema.getName());
        Map<String, String> fieldToType = new HashMap<String, String>() {{
            put("id", "long");
            put("username", "string");
            put("followers", "array");
        }};
        for (String key : fieldToType.keySet()) {
            assertEquals(
                    fieldToType.get(key),
                    schema.getField(key).schema().getType().getName()
            );
        }
    }

    /**
     * Helper method for testing generated schema
     *
     * @param schema
     */
    private static void verifyGeneratedSchema(Schema schema) {
        assertNotNull(schema);
        assertEquals(schema.getType(), Schema.Type.RECORD);
        Map<String, String> fieldToType = new HashMap<String, String>() {{
            put("id", "union");
            put("username", "union");
            put("followers", "union");
        }};
        Map<String, String> unionToInnerType = new HashMap<String, String>() {{
            put("id", "long");
            put("username", "string");
            put("followers", "string"); // arrays become strings
        }};
        for (String key : fieldToType.keySet()) {
            assertEquals(
                    fieldToType.get(key),
                    schema.getField(key).schema().getType().getName()
            );
            // check the union's inner types
            assertEquals(
                    "null",
                    schema.getField(key).schema().getTypes().get(0).getName()
            );
            assertEquals(
                    unionToInnerType.get(key),
                    schema.getField(key).schema().getTypes().get(1).getName()
            );
        }
    }

    /**
     * Generate a schema that matches the avro file
     * server/pxf-hdfs/src/test/resources/avro/test.avro
     *
     * @return
     */
    private Schema generateTestSchema() {
        Schema schema = Schema.createRecord("example_schema", "A basic schema for storing messages", "com.example", false);
        List<Schema.Field> fields = new ArrayList<>();

        Schema.Type type = Schema.Type.LONG;
        fields.add(new Schema.Field("id", Schema.create(type), "Id of the user account", null));

        type = Schema.Type.STRING;
        fields.add(new Schema.Field("username", Schema.create(type), "Name of the user account", null));

        // add an ARRAY of strings
        fields.add(new Schema.Field(
                "followers",
                Schema.createArray(Schema.create(Schema.Type.STRING)),
                "Users followers",
                null)
        );
        schema.setFields(fields);

        return schema;
    }

}