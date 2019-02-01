package org.greenplum.pxf.plugins.hdfs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.io.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;

public enum ParquetTypeConverter {

    BINARY {
        @Override
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
            if (originalType == null) {
                byte[] value = repetitionCount == 0 ? null : group.getBinary(columnIndex, repeatIndex).getBytes();
                if (jsonArrayNode != null)
                    jsonArrayNode.add(value);
                else
                    field.populate(DataType.BYTEA, value);
            } else if (originalType == OriginalType.DATE) { // DATE type
                field.type = DataType.DATE.getOID();
                field.val = repetitionCount == 0 ? null : group.getString(columnIndex, repeatIndex);
            } else if (originalType == OriginalType.TIMESTAMP_MILLIS) { // TIMESTAMP type
                field.type = DataType.TIMESTAMP.getOID();
                field.val = repetitionCount == 0 ? null : group.getString(columnIndex, repeatIndex);
            } else {
                field.type = DataType.TEXT.getOID();
                field.val = repetitionCount == 0 ? null : group.getString(columnIndex, repeatIndex);
            }
        }

        @Override
        public DataType getDataType(OriginalType originalType) {
            if (originalType == null) {
                return DataType.BYTEA;
            }
            switch (originalType) {
                case DATE: return DataType.DATE;
                case TIMESTAMP_MILLIS: return DataType.TIMESTAMP;
                default: return DataType.TEXT;
            }
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, int repetitionCount, OriginalType originalType, ArrayNode jsonNode) {
            if (getDataType(originalType) == DataType.BYTEA) {
                jsonNode.add(getByteArray(group, columnIndex, repeatIndex, repetitionCount);
            } else {
                jsonNode.add(getString(group, columnIndex, repeatIndex, repetitionCount);
            }
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, int repetitionCount, OriginalType originalType) {
            if (getDataType(originalType) == DataType.BYTEA) {
                return getByteArray(group, columnIndex, repeatIndex, repetitionCount);
            } else {
                return getString(group, columnIndex, repeatIndex, repetitionCount);
            }
        }



    },
    INT32 {
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
            if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
                field.type = DataType.SMALLINT.getOID();
                field.val = repetitionCount == 0 ? null : (short) group.getInteger(columnIndex, repeatIndex);
            } else {
                field.type = DataType.INTEGER.getOID();
                field.val = repetitionCount == 0 ? null : group.getInteger(columnIndex, repeatIndex);
            }
        }

        @Override
        public DataType getDataType(OriginalType originalType) {
            if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
                return DataType.SMALLINT;
            } else {
                return DataType.INTEGER;
            }
        }

        @Override
        public Object getValue(Group group, int columnIndex, int repeatIndex, int repetitionCount, OriginalType originalType) {
            Integer result = getInteger(group, columnIndex, repeatIndex, repetitionCount);
            return (getDataType(originalType) == DataType.SMALLINT) ? result.shortValue() : result;
        }

        @Override
        public void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, int repetitionCount, OriginalType originalType, ArrayNode jsonNode) {

        }
    },
    INT64 {
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
        field.type = DataType.BIGINT.getOID();
        field.val = repetitionCount == 0 ? null : group.getLong(columnIndex, repeatIndex);
        }
    },
    DOUBLE {
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
            field.type = DataType.FLOAT8.getOID();
        field.val = repetitionCount == 0 ? null : group.getDouble(columnIndex, repeatIndex);
        }
    },
    INT96 {
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
            field.type = DataType.TIMESTAMP.getOID();
            field.val = repetitionCount == 0 ? null : ParquetResolver.bytesToTimestamp(group.getInt96(columnIndex, repeatIndex).getBytes());
        }
    },
    FLOAT {
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
        field.type = DataType.REAL.getOID();
        field.val = repetitionCount == 0 ? null : group.getFloat(columnIndex, repeatIndex);
        }
    },
    FIXED_LEN_BYTE_ARRAY {
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
            field.type = DataType.NUMERIC.getOID();
            if (repetitionCount > 0) {
                int scale = primitiveType.getDecimalMetadata().getScale();
                field.val = new BigDecimal(new BigInteger(group.getBinary(columnIndex, repeatIndex).getBytes()), scale);
            }
        }
    },
    BOOLEAN {
        public void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode) {
            field.type = DataType.BOOLEAN.getOID();
            field.val = repetitionCount == 0 ? null : group.getBoolean(columnIndex, repeatIndex);
        }
    };

    public static ParquetTypeConverter from(PrimitiveType primitiveType) {
        return valueOf(primitiveType.getPrimitiveTypeName().name());
    }

    public abstract void resolve(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount, OneField field, ArrayNode jsonArrayNode);

    // ********** PUBLIC INTERFACE **********
    public abstract DataType getDataType(OriginalType originalType);
    public abstract Object getValue(Group group, int columnIndex, int repeatIndex, int repetitionCount, OriginalType originalType);
    public abstract void addValueToJsonArray(Group group, int columnIndex, int repeatIndex, int repetitionCount, OriginalType originalType, ArrayNode jsonNode);

    // ********** PRIVATE TYPED METHODS **********

    private static String getString(Group group, int columnIndex, int repeatIndex, int repetitionCount) {
        return (repetitionCount == 0) ? null : group.getString(columnIndex, repeatIndex);
    }

    private static byte[] getByteArray(Group group, int columnIndex, int repeatIndex, int repetitionCount) {
        return (repetitionCount == 0) ? null : group.getBinary(columnIndex, repeatIndex).getBytes();
    }

    private static Integer getInteger(Group group, int columnIndex, int repeatIndex, int repetitionCount) {
        return (repetitionCount == 0) ? null : group.getInteger(columnIndex, repeatIndex);
    }


}
