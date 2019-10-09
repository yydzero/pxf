package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.greenplum.pxf.api.io.DataType;

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
    UNION {
        @Override
        public DataType getDataType() {
            // use the from method to get the correct converter for UNION
            return null;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    };

    public static AvroTypeConverter from(Schema schema) {
        Schema.Type type = schema.getType();
        // we want the underlying type of UNION
        if (valueOf(type.name()) == UNION) {
            return valueOf(schema.getTypes().get(1).getType().name());
        }
        return valueOf(type.name());
    }

    // ********** PUBLIC INTERFACE **********
    public abstract DataType getDataType();
    public abstract Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type);
}
