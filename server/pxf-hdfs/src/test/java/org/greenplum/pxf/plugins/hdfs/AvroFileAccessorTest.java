package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AvroFileAccessorTest {
    AvroFileAccessor accessor;
    RequestContext context;
    String avroDirectory;
    @Mock
    ConfigurationFactory mockConfigurationFactory;

    @Before
    public void setup() {
        accessor = new AvroFileAccessor();
        context = new RequestContext();
        Configuration configuration = new Configuration();
        when(mockConfigurationFactory
                .initConfiguration("fakeConfig", "fakeServerName", "fakeUser", null))
                .thenReturn(configuration);

        context.setConfig("fakeConfig");
        context.setServerName("fakeServerName");
        context.setUser("fakeUser");
        context.setDataSource(avroDirectory + "test.avro");
        context.setSegmentId(0);
        context.setTransactionId("testID");
        context.setProfileScheme("localfile");
        context.setRequestType(RequestContext.RequestType.READ_BRIDGE);
        context.setDataSource(this.getClass().getClassLoader().getResource("avro/").getPath() + "test.avro");
    }

    @Test
    public void testInitialize() {
        accessor.initialize(context);
        Schema schema = (Schema) context.getMetadata();
        assertNotNull(schema);
        verifySchema(schema, "example_schema");
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
}