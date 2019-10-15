package org.greenplum.pxf.plugins.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.greenplum.pxf.api.model.ConfigurationFactory;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        mockConfigurationFactory = mock(ConfigurationFactory.class);
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
        assertNotNull(context.getMetadata());
    }

}