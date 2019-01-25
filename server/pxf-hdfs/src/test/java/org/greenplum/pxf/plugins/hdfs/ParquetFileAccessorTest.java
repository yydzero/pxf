package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.model.RequestContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

@RunWith(MockitoJUnitRunner.class)
public class ParquetFileAccessorTest {
    ParquetFileAccessor accessor;
    RequestContext context;
    MessageType schema;

    @Before
    public void setup() {
        accessor = new ParquetFileAccessor();
        context = new RequestContext();
        schema = new MessageType("hive_schema");
    }

    @Test
    public void TestInitializeWithNullSchema() {
        accessor.initialize(context);
        assertEquals(schema, context.getMetadata());
    }

    @Test
    public void TestInitializeWithSchema() {
        Type field = new PrimitiveType(
                Type.Repetition.OPTIONAL,
                PrimitiveTypeName.BINARY,
                "s1",
                OriginalType.UTF8
        );
        schema = new MessageType("hive_schema", field);
        byte[] bytes = schema.toString().getBytes();
        context.setFragmentUserData(bytes);
        accessor.initialize(context);
        assertEquals(schema, context.getMetadata());
    }
}
