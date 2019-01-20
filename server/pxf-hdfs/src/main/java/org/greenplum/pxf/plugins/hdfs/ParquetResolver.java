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

import org.apache.commons.lang.ObjectUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.GPDBWritable;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.Resolver;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class ParquetResolver extends BasePlugin implements Resolver {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final int SECOND_IN_MILLIS = 1000;
    private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private MessageType schema;
    private Type[] types;
    private PrimitiveType.PrimitiveTypeName[] primitiveTypeNames;
    private SimpleGroupFactory groupFactory;
    private GPDBWritable gpdbOutput;
    private int numFields;

    @Override
    public List<OneField> getFields(OneRow row) {
        return null;
    }

    @Override
    public OneRow setFields(List<OneField> record) {
        return null;
    }

    @Override
    public boolean supportsWritable() {
        return true;
    }

    @Override
    public Writable getWritable(OneRow row) {
        if (schema == null) {
            initSchema();
            gpdbOutput = makeGPDBWritableOutput();
        }
        Group group = (Group) row.getData();
        for (int i = 0; i < numFields; i++) {
            resolvePrimitive(i, gpdbOutput, group, types[i]);
        }
        return gpdbOutput;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param  writable of {@link GPDBWritable}
     * @return the constructed {@link OneRow}
     * @throws IOException if constructing a row from the fields failed
     */
    @Override
    public OneRow setWritable(Writable writable) throws IOException {
        if (groupFactory == null) {
            initSchema();
            groupFactory = new SimpleGroupFactory(schema);
        }
        Group group = groupFactory.newGroup();
        GPDBWritable gpdbWritable = (GPDBWritable)writable;
        for (int i = 0; i < numFields; i++) {
            fillGroup(i, gpdbWritable.getObject(i), group, types[i]);
        }
        return new OneRow(null, group);
    }

    /**
     * Creates the GPDBWritable object. The object is created one time and is
     * refilled from recFields for each record sent
     *
     * @return empty GPDBWritable object with set columns
     */
    private GPDBWritable makeGPDBWritableOutput() {
        int num_actual_fields = context.getColumns();
        int[] schema = new int[num_actual_fields];
        for (int i = 0; i < num_actual_fields; i++) {
            schema[i] = context.getColumn(i).columnTypeCode();
        }
        return new GPDBWritable(schema);
    }

    private void initSchema() {
        schema = (MessageType)context.getMetadata();
        numFields = schema.getFieldCount();
        types = new Type[numFields];
        primitiveTypeNames = new PrimitiveType.PrimitiveTypeName[numFields];
        for (int i = 0; i < numFields; i++) {
            types[i] = schema.getType(i);
            if (!types[i].isPrimitive())
                throw new UnsupportedTypeException("Only primitive types are supported.");
            primitiveTypeNames[i] = types[i].asPrimitiveType().getPrimitiveTypeName();
        }
    }

    private void fillGroup(int index, Object val, Group group, Type type) throws IOException {
        if (val == null)
            return;
        switch (primitiveTypeNames[index]) {
            case BINARY:
                if (type.getOriginalType() == OriginalType.UTF8)
                    group.add(index, (String) val);
                else
                    group.add(index, Binary.fromReusedByteArray((byte[]) val));
                break;
            case INT32:
                if (type.getOriginalType() == OriginalType.INT_16)
                    group.add(index, (Short) val);
                else
                    group.add(index, (Integer) val);
                break;
            case INT64:
                group.add(index, (Long) val);
                break;
            case DOUBLE:
                group.add(index, (Double) val);
                break;
            case FLOAT:
                group.add(index, (Float) val);
                break;
            case FIXED_LEN_BYTE_ARRAY:
                BigDecimal value = new BigDecimal((String) val);
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
                LocalDateTime date = LocalDateTime.parse((String) val, dateFormatter);
                long millisSinceEpoch = date.toEpochSecond(ZoneOffset.UTC) * SECOND_IN_MILLIS;
                group.add(index, getBinary(millisSinceEpoch));
                break;
            case BOOLEAN:
                group.add(index, (Boolean) val);
                break;
            default:
                throw new IOException("Not supported type " + type.asPrimitiveType().getPrimitiveTypeName());
        }
    }

    private void resolvePrimitive(int index, GPDBWritable gpdbOutput, Group group, Type type) {
        Object val = null;
        int repetitionCount = group.getFieldRepetitionCount(index);
        if (repetitionCount > 0) {
            switch (primitiveTypeNames[index]) {
                case BINARY:
                    val = (type.getOriginalType() == null) ? group.getBinary(index, 0).getBytes() :
                            ObjectUtils.toString(group.getString(index, 0));
                    break;
                case INT32:
                    if (type.getOriginalType() == OriginalType.INT_8 ||
                            type.getOriginalType() == OriginalType.INT_16) {
                        val = (short) group.getInteger(index, 0);
                    } else {
                        val = group.getInteger(index, 0);
                    }
                    break;
                case INT64:
                    val = group.getLong(index, 0);
                    break;
                case DOUBLE:
                    val = group.getDouble(index, 0);
                    break;
                case INT96:
                    val = ObjectUtils.toString(bytesToTimestamp(group.getInt96(index, 0).getBytes()), null);
                    break;
                case FLOAT:
                    val = group.getFloat(index, 0);
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    int scale = type.asPrimitiveType().getDecimalMetadata().getScale();
                    val = ObjectUtils.toString(new BigDecimal(new BigInteger(group.getBinary(index, 0).getBytes()), scale), null);
                    break;
                case BOOLEAN:
                    val = group.getBoolean(index, 0);
                    break;
                default: {
                    throw new UnsupportedTypeException("Type " + primitiveTypeNames[index] + "is not supported");
                }
            }
        }
        gpdbOutput.setObject(index, repetitionCount == 0 ? null : val);
    }

    // Convert parquet byte array to java timestamp
    private Timestamp bytesToTimestamp(byte[] bytes) {
        long timeOfDayNanos = ByteBuffer.wrap(new byte[]{
                bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]}).getLong();
        int julianDays = (ByteBuffer.wrap(new byte[]{bytes[11], bytes[10], bytes[9], bytes[8]})).getInt();
        long unixTimeMs = (julianDays - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY + timeOfDayNanos / 1000000;
        return new Timestamp(unixTimeMs);
    }

    // Convert epoch timestamp to byte array (INT96)
    // Inverse of the function above
    private Binary getBinary(long timeMillis) {
        long daysSinceEpoch = timeMillis / MILLIS_IN_DAY;
        int julianDays = JULIAN_EPOCH_OFFSET_DAYS + (int) daysSinceEpoch;
        long timeOfDayNanos = (timeMillis % MILLIS_IN_DAY) * 1000000;
        return new NanoTime(julianDays, timeOfDayNanos).toBinary();
    }
}
