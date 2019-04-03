package org.greenplum.pxf.plugins.hdfs;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.*;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class ParquetFileAccessorTest {
    ParquetFileAccessor accessor;
    RequestContext context;
    MessageType schema;

    @Before
    public void setup() {
        accessor = new ParquetFileAccessor();
        context = new RequestContext();
        schema = new MessageType("test");
        context.setMetadata(schema);
    }

    @Test
    public void testInitialize() {
        accessor.initialize(context);
    }

    @Test
    public void testGetFields_Count() throws IOException {
        schema = getParquetSchemaForPrimitiveTypes(Type.Repetition.OPTIONAL, true);
        // schema has changed, set metadata again
        context.setMetadata(schema);
        context.setTupleDescription(getColumnDescriptorsFromSchema(schema));
        String filename = "parquet/primitive_types.parquet";
        String parquetFile = Objects.requireNonNull(getClass().getClassLoader().getResource(filename)).getPath();
        context.setDataSource(parquetFile);
        accessor.initialize(context);
        accessor.openForRead();
        int rowCount = 0;
        while(accessor.readNextObject() != null) {
            rowCount++;
        }
        assertEquals(25, rowCount);
    }

    private MessageType getParquetSchemaForPrimitiveTypes(Type.Repetition repetition, boolean readCase) {
        List<Type> fields = new ArrayList<>();

        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, "s1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, "s2", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, "n1", null));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.DOUBLE, "d1", null));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16, "dc1", OriginalType.DECIMAL, new DecimalMetadata(38, 18), null));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT96, "tm", null));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.FLOAT, "f", null));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, "bg", null));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BOOLEAN, "b", null));

        // GPDB only has int16 and not int8 type, so for write tiny numbers int8 are still treated as shorts in16
        OriginalType tinyType = readCase ? OriginalType.INT_8 : OriginalType.INT_16;
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, "tn", tinyType));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT32, "sml", OriginalType.INT_16));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, "vc1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, "c1", OriginalType.UTF8));
        fields.add(new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BINARY, "bin", null));

        return new MessageType("hive_schema", fields);
    }

    private List<ColumnDescriptor> getColumnDescriptorsFromSchema(MessageType schema) {
        return schema.getFields()
                .stream()
                .map(f -> {
                    ParquetTypeConverter converter = ParquetTypeConverter.from(f.asPrimitiveType());
                    return new ColumnDescriptor(f.getName(), converter.getDataType(f).getOID(), 1, "", new Integer[]{});
                })
                .collect(Collectors.toList());
    }

}
