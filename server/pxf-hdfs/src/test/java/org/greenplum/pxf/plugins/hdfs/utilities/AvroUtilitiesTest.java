package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    /* READ PATH */

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
    public void testObtainSchemaOnReadWithUserProvidedSchemaLocalFile() throws IOException {
        AvroUtilities.FsProvider provider = mock(AvroUtilities.FsProvider.class);
        FileSystem mockFileSystem = mock(FileSystem.class);
        when(mockFileSystem.exists(any())).thenReturn(false);
        when(provider.getFilesystem(any())).thenReturn(mockFileSystem);
        AvroUtilities.setFsProvider(provider);

        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnReadWithUserProvidedSchemaJsonLocalFile() throws IOException {
        AvroUtilities.FsProvider provider = mock(AvroUtilities.FsProvider.class);
        FileSystem mockFileSystem = mock(FileSystem.class);
        when(mockFileSystem.exists(any())).thenReturn(false);
        when(provider.getFilesystem(any())).thenReturn(mockFileSystem);
        AvroUtilities.setFsProvider(provider);

        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnReadWhenUserProvidedSchemaOnClasspath() throws MalformedURLException {
        AvroUtilities.UrlProvider provider = mock(AvroUtilities.UrlProvider.class);
        when(provider.getUrlFromPath(any(), any())).thenReturn(new URL("file://" + avroDirectory + "user-provided.avro"));
        AvroUtilities.setUrlProvider(provider);

        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avro");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnReadWhenUserProvidedJsonSchemaOnClasspath() throws MalformedURLException {
        AvroUtilities.UrlProvider provider = mock(AvroUtilities.UrlProvider.class);
        when(provider.getUrlFromPath(any(), any())).thenReturn(new URL("file://" + avroDirectory + "user-provided.avsc"));
        AvroUtilities.setUrlProvider(provider);

        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avsc");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test(expected = DataSchemaException.class)
    public void testObtainSchemaOnReadWhenUserProvidedSchemaNotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avro");

        schema = AvroUtilities.obtainSchema(context, configuration);
    }

    @Test(expected = DataSchemaException.class)
    public void testObtainSchemaOnReadWhenUserProvidedSchemaJsonNotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avsc");

        schema = AvroUtilities.obtainSchema(context, configuration);
    }


    /* WRITE PATH */

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

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchemaLocalFile() throws IOException {
        AvroUtilities.FsProvider provider = mock(AvroUtilities.FsProvider.class);
        FileSystem mockFileSystem = mock(FileSystem.class);
        when(mockFileSystem.exists(any())).thenReturn(false);
        when(provider.getFilesystem(any())).thenReturn(mockFileSystem);
        AvroUtilities.setFsProvider(provider);

        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        context.addOption("SCHEMA", avroDirectory + "user-provided.avro");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWithUserProvidedSchemaJsonLocalFile() throws IOException {
        AvroUtilities.FsProvider provider = mock(AvroUtilities.FsProvider.class);
        FileSystem mockFileSystem = mock(FileSystem.class);
        when(mockFileSystem.exists(any())).thenReturn(false);
        when(provider.getFilesystem(any())).thenReturn(mockFileSystem);
        AvroUtilities.setFsProvider(provider);

        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);
        context.addOption("SCHEMA", avroDirectory + "user-provided.avsc");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }


    @Test
    public void testObtainSchemaOnWriteWhenUserProvidedSchemaOnClasspath() throws MalformedURLException {
        AvroUtilities.UrlProvider provider = mock(AvroUtilities.UrlProvider.class);
        when(provider.getUrlFromPath(any(), any())).thenReturn(new URL("file://" + avroDirectory + "user-provided.avro"));
        AvroUtilities.setUrlProvider(provider);

        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avro");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }

    @Test
    public void testObtainSchemaOnWriteWhenUserProvidedJsonSchemaOnClasspath() throws MalformedURLException {
        AvroUtilities.UrlProvider provider = mock(AvroUtilities.UrlProvider.class);
        when(provider.getUrlFromPath(any(), any())).thenReturn(new URL("file://" + avroDirectory + "user-provided.avsc"));
        AvroUtilities.setUrlProvider(provider);

        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avsc");

        schema = AvroUtilities.obtainSchema(context, configuration);

        verifySchema(schema, "user_provided_schema");
    }
    @Test(expected = DataSchemaException.class)
    public void testObtainSchemaOnWriteWhenUserProvidedSchemaNotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avro");

        schema = AvroUtilities.obtainSchema(context, configuration);
    }

    @Test(expected = DataSchemaException.class)
    public void testObtainSchemaOnWriteWhenUserProvidedSchemaJsonNotOnClasspath() {
        context.setRequestType(RequestContext.RequestType.WRITE_BRIDGE);

        context.addOption("SCHEMA", "user-provided.avsc");

        schema = AvroUtilities.obtainSchema(context, configuration);
    }

    @After
    public void tearDown() {
        AvroUtilities.setUrlProvider(null);
        AvroUtilities.setFsProvider(null);
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