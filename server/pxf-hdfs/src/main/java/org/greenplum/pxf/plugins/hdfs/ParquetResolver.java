package org.greenplum.pxf.plugins.hdfs;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.parquet.schema.Type.Repetition.REPEATED;

public class ParquetResolver extends BasePlugin implements Resolver {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final int SECOND_IN_MILLIS = 1000;
    private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;
    private static final String COLLECTION_DELIM = ",";
    private static final String MAPKEY_DELIM = ":";
    private String collectionDelim;
    private String mapkeyDelim;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private MessageType schema;
    private SimpleGroupFactory groupFactory;
    private ObjectMapper mapper = new ObjectMapper();
//    private List<OneField> output;

    @Override
    public void initialize(RequestContext requestContext) {
        super.initialize(requestContext);

        schema = (MessageType) requestContext.getMetadata();
        groupFactory = new SimpleGroupFactory(schema);
        collectionDelim = context.getOption("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : context.getOption("COLLECTION_DELIM");
        mapkeyDelim = context.getOption("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : context.getOption("MAPKEY_DELIM");
//        output = new ArrayList<>(schema.getFieldCount());
    }

//    private void setType() {
//        for (int fieldIndex = 0; fieldIndex < schema.getFieldCount(); fieldIndex++) {
//
//            Type type = schema.getType(fieldIndex);
//            OriginalType originalType = type.getOriginalType();
//            PrimitiveType primitiveType = type.asPrimitiveType();
//            OneField field = new OneField();
//
//            switch (schema.getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
//                case BINARY:
//                    if (originalType == null) {
//                        field.type = DataType.BYTEA.getOID();
//                    } else if (originalType == OriginalType.DATE) { // DATE type
//                        field.type = DataType.DATE.getOID();
//                    } else if (originalType == OriginalType.TIMESTAMP_MILLIS) { // TIMESTAMP type
//                        field.type = DataType.TIMESTAMP.getOID();
//                    } else {
//                        field.type = DataType.TEXT.getOID();
//                    break;
//                }
//                case INT32:
//                    if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
//                        field.type = DataType.SMALLINT.getOID();
//                    } else {
//                        field.type = DataType.INTEGER.getOID();
//                    }
//                    break;
//                case INT64:
//                    field.type = DataType.BIGINT.getOID();
//                    break;
//                case DOUBLE:
//                    field.type = DataType.FLOAT8.getOID();
//                    break;
//                case INT96:
//                    field.type = DataType.TIMESTAMP.getOID();
//                    break;
//                case FLOAT:
//                    field.type = DataType.REAL.getOID();
//                    break;
//                case FIXED_LEN_BYTE_ARRAY:
//                    field.type = DataType.NUMERIC.getOID();
//                    break;
//                case BOOLEAN:
//                    field.type = DataType.BOOLEAN.getOID();
//                    break;
//                default:
//                    throw new UnsupportedTypeException("Type " + primitiveType.getPrimitiveTypeName()
//                            + "is not supported");
//            }
//            output.add(field);
//        }
//    }
//
//    private Object getPrimitiveScalarValue(Group group, int columnIndex, int repeatIndex, PrimitiveType primitiveType, OriginalType originalType, int repetitionCount) {
//        if (repetitionCount == 0)
//            return null;
//        switch (primitiveType.getPrimitiveTypeName()) {
//            case BINARY:
//                if (originalType == null) {
//                    return group.getBinary(columnIndex, repeatIndex).getBytes();
//                }
//                return group.getString(columnIndex, repeatIndex);
//            case INT32:
//                if (originalType == OriginalType.INT_8 || originalType == OriginalType.INT_16) {
//                    return (short) group.getInteger(columnIndex, repeatIndex);
//                }
//                return group.getInteger(columnIndex, repeatIndex);
//            case INT64:
//                return group.getLong(columnIndex, repeatIndex);
//            case DOUBLE:
//                return group.getDouble(columnIndex, repeatIndex);
//            case INT96:
//                return bytesToTimestamp(group.getInt96(columnIndex, repeatIndex).getBytes());
//            case FLOAT:
//                return group.getFloat(columnIndex, repeatIndex);
//            case FIXED_LEN_BYTE_ARRAY:
//                int scale = primitiveType.getDecimalMetadata().getScale();
//                return new BigDecimal(new BigInteger(group.getBinary(columnIndex, repeatIndex).getBytes()), scale);
//            case BOOLEAN:
//                return group.getBoolean(columnIndex, repeatIndex);
//            default:
//                throw new UnsupportedTypeException("Type " + primitiveType.getPrimitiveTypeName()
//                        + "is not supported");
//        }
//    }


    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws IOException if constructing a row from the fields failed
     */
    @Override
    public OneRow setFields(List<OneField> record) throws IOException {
        Group group = groupFactory.newGroup();
        for (int i = 0; i < record.size(); i++) {
            fillGroup(i, record.get(i), group, schema.getType(i));
        }
        return new OneRow(null, group);
    }

    private void fillGroup(int index, OneField field, Group group, Type type) throws IOException {
        if (field.val == null)
            return;
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
            case BINARY:
                if (type.getOriginalType() == OriginalType.UTF8)
                    group.add(index, (String) field.val);
                else
                    group.add(index, Binary.fromReusedByteArray((byte[]) field.val));
                break;
            case INT32:
                if (type.getOriginalType() == OriginalType.INT_16)
                    group.add(index, (Short) field.val);
                else
                    group.add(index, (Integer) field.val);
                break;
            case INT64:
                group.add(index, (Long) field.val);
                break;
            case DOUBLE:
                group.add(index, (Double) field.val);
                break;
            case FLOAT:
                group.add(index, (Float) field.val);
                break;
            case FIXED_LEN_BYTE_ARRAY:
                BigDecimal value = new BigDecimal((String) field.val);
                byte fillByte = (byte) (value.signum() < 0 ? 0xFF : 0x00);
                byte[] unscaled = value.unscaledValue().toByteArray();
                byte[] bytes = new byte[16];
                int offset = bytes.length - unscaled.length;
                for (int i = 0; i < bytes.length; i += 1) {
                    if (i < offset) {
                        bytes[i] = fillByte;
                    } else {
                        bytes[i] = unscaled[i - offset];
                    }
                }
                group.add(index, Binary.fromReusedByteArray(bytes));
                break;
            case INT96:
                LocalDateTime date = LocalDateTime.parse((String) field.val, dateFormatter);
                long millisSinceEpoch = date.toEpochSecond(ZoneOffset.UTC) * SECOND_IN_MILLIS;
                group.add(index, ParquetTypeConverter.getBinary(millisSinceEpoch));
                break;
            case BOOLEAN:
                group.add(index, (Boolean) field.val);
                break;
            default:
                throw new IOException("Not supported type " + type.asPrimitiveType().getPrimitiveTypeName());
        }
    }

    @Override
    public List<OneField> getFields(OneRow row) {
        Group group = (Group) row.getData();
        List<OneField> output = new LinkedList<>();

        for (int columnIndex = 0; columnIndex < schema.getFieldCount(); columnIndex++) {

//            Type parquetType = schema.getType(fieldIndex);
//            if (parquetType.getRepetition() == REPEATED) {
//                // process repeated types (primitive or complex)
//            } else {
//                // process scalar types (primitive or complex)
//            }


            Type type = schema.getType(columnIndex);
            if (schema.getType(columnIndex).isPrimitive()) {
                output.add(resolvePrimitive(group, columnIndex, type, 0));
            } else {
//                int repeatCount = group.getFieldRepetitionCount(fieldIndex);
//                for (int repeatIndex = 0; repeatIndex < repeatCount; repeatIndex++) {
//                    output.add(resolveComplex(group.getGroup(fieldIndex, repeatIndex), type.asGroupType()));
//                }
                output.add(resolveComplex(group.getGroup(columnIndex, 0), type.asGroupType(), 0));
            }

        }
        return output;
    }

    private OneField resolveComplex(Group g, GroupType groupType, int level) {

        level++;
        ObjectNode node = mapper.createObjectNode();



        List<Type> types = groupType.getFields();
        OneField result = new OneField();
        result.type = DataType.TEXT.getOID();

        List<OneField> fieldList = new ArrayList<>();
        int fieldCount = types.size();

        for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            Type type = types.get(fieldIndex);
            //OneField field = new OneField();
            //field.type = DataType.TEXT.getOID();
            int repeatCount = g.getFieldRepetitionCount(fieldIndex);

            // primitive ? --> cal our method, get either value or json back, hook to key
            //"foo" : 123
            //"froo": [123, 567]
            Object value = resolvePrimitive(g, fieldIndex, type, level).val;
            //node.put(type.getName(), )


            for (int repeatIndex = 0; repeatIndex < repeatCount; repeatIndex++) {
                if (type.isPrimitive()) {
                    if(type.getOriginalType() != null) {
                        // we have a primitive that is part of key, value pair
                        // TODO: create JsonNode
                        //field.val = type.getName() + mapkeyDelim + g.getValueToString(fieldIndex, repeatIndex);
                    } else {
                        // we have a primitive in an array
                        //field.val = g.getValueToString(fieldIndex, repeatIndex);
                    }
                    //fieldList.add(field);
                } else {
                    fieldList.add(resolveComplex(g.getGroup(fieldIndex, repeatIndex), type.asGroupType(), level));
                }
            }
        }

        if(fieldCount == 1 && types.get(0).isPrimitive()) {
            // Primitive type within List
            result.val = fieldList.get(0).val;
        } else if (OriginalType.LIST == groupType.getOriginalType()) {
            // List type
            result.val = String.format("[%s]", HdfsUtilities.toString(fieldList, collectionDelim));
        }
        else {
            // Struct type
            result.val = String.format("{%s}", HdfsUtilities.toString(fieldList, collectionDelim));
        }

        return result;
    }

//    private OneField resolveArray(int index, Group g, Type type) {
//        List<OneField> fieldList = new LinkedList<>();
//        for (int i = 0; i < g.getFieldRepetitionCount(index); i++)
//            fieldList.add(resolvePrimitive(index, g, type));
//        OneField field = new OneField();
//        field.type = DataType.TEXT.getOID();
//        field.val = String.format("[%s]", HdfsUtilities.toString(fieldList, collectionDelim));
//        return field;
//    }

    private OneField resolvePrimitive(Group group, int columnIndex, Type type, int level) {

        OneField field = new OneField();
        // get type converter based on the primitive type
        ParquetTypeConverter converter = ParquetTypeConverter.from(type.asPrimitiveType());

        // determine how many values for the primitive are present in the column
        int repetitionCount = group.getFieldRepetitionCount(columnIndex);

        // at the top level (top field), non-repeated primitives will convert to typed OneField
        if (level == 0 && type.getRepetition() != REPEATED) {
            field.type = converter.getDataType(type).getOID();
            field.val = repetitionCount == 0 ? null : converter.getValue(group, columnIndex, 0, type);
        } else if (type.getRepetition() == REPEATED) {
            // repeated primitive at any level will convert into JSON
            ArrayNode jsonArray = mapper.createArrayNode();
            for (int repeatIndex = 0; repeatIndex < repetitionCount; repeatIndex++) {
                converter.addValueToJsonArray(group, columnIndex, repeatIndex, type, jsonArray);
            }
            // but will become a string only at top level
            if (level == 0) {
                field.type = DataType.TEXT.getOID();
                try {
                    field.val = mapper.writeValueAsString(jsonArray);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to serialize repeated parquet type " + type.asPrimitiveType().getName(), e);
                }
            } else {
                // just return the array node within OneField container
                field.val = jsonArray;
            }
        }

        return field;
    }

}
