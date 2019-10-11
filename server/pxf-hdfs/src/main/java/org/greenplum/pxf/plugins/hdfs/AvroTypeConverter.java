package org.greenplum.pxf.plugins.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.greenplum.pxf.api.io.DataType;

public enum AvroTypeConverter {
    BOOLEAN {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.BOOLEAN;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    BYTES {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.BYTEA;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    DOUBLE {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.FLOAT8;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    FLOAT {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.REAL;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    INT {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.INTEGER;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    LONG {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.BIGINT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    STRING {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    ARRAY {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    MAP {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    ENUM {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    FIXED {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.BYTEA;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    RECORD {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            return DataType.TEXT;
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    },
    UNION {
        @Override
        public DataType getDataType(Schema schema, Object val) {
            /**
             * When an Avro field is actually a union, we resolve the type
             * of the union element, and delegate the record update via
             * recursion
             */
            int unionIndex = GenericData.get().resolveUnion(schema, val);
            /**
             * Retrieve index of the non null data type from the type array
             * if value is null, to match the column type in Greenplum
             */
            if (val == null) {
                unionIndex ^= 1; // exclusive or assignment
            }
            Schema nestedSchema = schema.getTypes().get(unionIndex);
            AvroTypeConverter converter = AvroTypeConverter.valueOf(
                    nestedSchema
                            .getType()
                            .name()
            );
            return converter.getDataType(nestedSchema, val);
        }

        @Override
        public Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type) {
            return null;
        }
    };

    public static AvroTypeConverter from(Schema schema) {
        Schema.Type type = schema.getType();

        return valueOf(type.name());
    }

    // ********** PUBLIC INTERFACE **********
    public abstract DataType getDataType(Schema schema, Object val);

    public abstract Object getValue(GenericRecord genericRecord, int columnIndex, Schema.Type type);
}
