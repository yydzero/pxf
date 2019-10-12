package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;

import java.util.List;
import java.util.stream.Collectors;

public enum AvroTypeConverter {
    BOOLEAN {
        @Override
        public DataType getDataType() {
            return DataType.BOOLEAN;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    BYTES {
        @Override
        public DataType getDataType() {
            return DataType.BYTEA;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    DOUBLE {
        @Override
        public DataType getDataType() {
            return DataType.FLOAT8;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    FLOAT {
        @Override
        public DataType getDataType() {
            return DataType.REAL;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    INT {
        @Override
        public DataType getDataType() {
            return DataType.INTEGER;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    LONG {
        @Override
        public DataType getDataType() {
            return DataType.BIGINT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    STRING {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    ARRAY {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    MAP {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    ENUM {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    FIXED {
        @Override
        public DataType getDataType() {
            return DataType.BYTEA;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    RECORD {
        @Override
        public DataType getDataType() {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    UNION {
        @Override
        public DataType getDataType() {
            // this should not be called if using AvroTypeConverter#from()
            return null;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    };

    public static AvroTypeConverter from(Schema schema) {
        Schema.Type type = schema.getType();

        if (type == Schema.Type.UNION) {
            Schema.Type nestedType = schema.getTypes().get(0).getType();
            // make sure to get the non-null type
            type = (nestedType == Schema.Type.NULL) ?
                    schema.getTypes().get(1).getType() :
                    nestedType;
        }

        return valueOf(type.name());
    }

    // ********** PUBLIC INTERFACE **********
    public abstract DataType getDataType();

    public abstract Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type);

    public static List<ColumnDescriptor> getColumnDescriptorsFromSchema(Schema schema) {
        return schema
                .getFields()
                .stream()
                .map(f -> new ColumnDescriptor(
                        f.name(),
                        from(f.schema()).getDataType().getOID(),
                        1,
                        "",
                        new Integer[]{}
                )).collect(Collectors.toList());
    }
}
