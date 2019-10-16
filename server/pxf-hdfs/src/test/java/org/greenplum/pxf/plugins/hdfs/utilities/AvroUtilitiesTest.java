package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AvroUtilitiesTest {

    private static String pathString = "/path/to/file";
    private static Path path = new Path(pathString);

    private RequestContext context;
    private Schema schema;
    private Schema testSchema;
    private String avroDirectory;
    private Configuration configuration;

    @Mock
    private AvroUtilities.UrlProvider mockUrlProvider;
    @Mock
    private AvroUtilities.FsProvider mockFsProvider;
    @Mock
    private FileSystem mockFileSystem;

    private AvroUtilities avroUtilities;

    @Before
    public void setup() {
        avroDirectory = this.getClass().getClassLoader().getResource("avro/").getPath();

        context = new RequestContext();
        configuration = new Configuration();

        context.setDataSource(avroDirectory + "test.avro");

        testSchema = generateTestSchema();
        avroUtilities = new AvroUtilities(mockUrlProvider, mockFsProvider);
        when(mockFsProvider.getFilesystem(configuration)).thenReturn(mockFileSystem);

    }

    /* READ PATH */

    @Test
    public void testObtainSchemaOnRead() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "example_schema");
    }

    @Test
    public void testObtainSchemaOnReadWithUserProvidedSchema() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnReadWithUserProvidedSchemaJson() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnReadWithUserProvidedSchemaLocalFile() throws IOException {
        when(mockFileSystem.exists(path)).thenReturn(false);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");

        verify(mockFileSystem).exists(path);
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_LocalFile() throws IOException {
        when(mockFileSystem.exists(path)).thenReturn(false);
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Binary_OnClasspath() throws MalformedURLException {
        when(mockUrlProvider.getUrlFromPath(pathString)).thenReturn(new URL("file://" + avroDirectory + "user-provided.avro"));
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Json_OnClasspath() throws MalformedURLException {
        when(mockUrlProvider.getUrlFromPath(pathString)).thenReturn(new URL("file://" + avroDirectory + "user-provided.avsc"));
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test(expected = DataSchemaException.class)
    public void testObtainSchema_OnRead_WithUserProvidedSchema_Binary_NotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, configuration);
    }

    @Test(expected = DataSchemaException.class)
    public void testObtainSchemaOnReadWhenUserProvidedSchemaJsonNotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, configuration);
    }


    /* WRITE PATH */

    @Test
    public void testObtainSchemaOnWrite() {
        context.setTupleDescription(AvroTypeConverter.getColumnDescriptorsFromSchema(testSchema));
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        schema = avroUtilities.obtainSchema(context, configuration);

        verifyGeneratedSchema(schema);
    }

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchema() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchemaJson() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");
        context.setDataSource(avroDirectory);

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchemaLocalFile() throws IOException {
        when(mockFileSystem.exists(path)).thenReturn(false);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchemaJsonLocalFile() throws IOException {
        when(mockFileSystem.exists(path)).thenReturn(false);
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWhenUserProvidedSchemaOnClasspath() throws MalformedURLException {
        when(mockUrlProvider.getUrlFromPath(pathString)).thenReturn(new URL("file://" + avroDirectory + "user-provided.avro"));
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWhenUserProvidedJsonSchemaOnClasspath() throws MalformedURLException {
        when(mockUrlProvider.getUrlFromPath(pathString)).thenReturn(new URL("file://" + avroDirectory + "user-provided.avsc"));
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }
    @Test(expected = DataSchemaException.class)
    public void testObtainSchemaOnWriteWhenUserProvidedSchemaNotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avro");

        schema = avroUtilities.obtainSchema(context, configuration);
    }

    @Test(expected = DataSchemaException.class)
    public void testObtainSchemaOnWriteWhenUserProvidedSchemaJsonNotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", "user-provided.avsc");

        schema = avroUtilities.obtainSchema(context, configuration);
    }

    /**
     * Helper method for testing schema
     *
     * @param schema
     */
    private void verifySchema(Schema schema, String name) {
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
    private void verifyGeneratedSchema(Schema schema) {
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