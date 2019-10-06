package org.greenplum.pxf.api.io;

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


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;


/**
 * This class represents a GPDB record in the form of
 * a Java object.
 */
public class GPDBWritable implements Writable {
    /*
     * GPDBWritable is using the following serialization form:
	 * Total Length | Version | Error Flag | # of columns | Col type |...| Col type | Null Bit array            |   Col val...
     * 4 byte		| 2 byte  |	1 byte     |   2 byte     |  1 byte  |...|  1 byte  | ceil(# of columns/8) byte |   Fixed or Var length
     *
     * For fixed length type, we know the length.
     * In the col val, we align pad according to the alignment requirement of the type.
     * For var length type, the alignment is always 4 byte.
     * For var length type, col val is <4 byte length><payload val>
	 */

    private static final Log LOG = LogFactory.getLog(GPDBWritable.class);
    private static final int EOF = -1;

    /*
     * Enum of the Database type
     */
    private enum DBType {
        BIGINT(8, 8),
        BOOLEAN(1, 1),
        FLOAT8(8, 8),
        INTEGER(4, 4),
        REAL(4, 4),
        SMALLINT(2, 2),
        BYTEA(4, -1),
        TEXT(4, -1);

        private final int typelength; // -1 means var length
        private final int alignment;

        DBType(int align, int len) {
            this.typelength = len;
            this.alignment = align;
        }

        public int getTypeLength() {
            return typelength;
        }

        public boolean isVarLength() {
            return typelength == -1;
        }

        // return the alignment requirement of the type
        public int getAlignment() {
            return alignment;
        }
    }

    /*
     * Constants
     */
    private static final int PREV_VERSION = 1;
    private static final int VERSION = 2; /* for backward compatibility */
    private static final String CHARSET = "UTF-8";

    /*
     * Local variables
     */
    private int[] colType;
    private Object[] colValue;
    private int alignmentOfEightBytes = 8;
    private byte errorFlag = 0;
    private int pktlen = EOF;

    public int[] getColType() {
        return colType;
    }

    /**
     * An exception class for column type definition and
     * set/get value mismatch.
     */
    public class TypeMismatchException extends IOException {
        TypeMismatchException(String msg) {
            super(msg);
        }
    }

    /**
     * Empty Constructor
     */
    public GPDBWritable() {
        initializeEightByteAlignment();
    }

    /**
     * Constructor to build a db record. colType defines the schema
     *
     * @param columnType the table column types
     */
    public GPDBWritable(int[] columnType) {
        initializeEightByteAlignment();
        colType = columnType;
        colValue = new Object[columnType.length];
    }

    /**
     * Constructor to build a db record from a serialized form.
     *
     * @param data a record in the serialized form
     * @throws IOException if the data is malformatted.
     */
    public GPDBWritable(byte[] data) throws IOException {
        initializeEightByteAlignment();
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bis);

        readFields(dis);
    }

    /*
     * Read first 4 bytes, and verify it's a valid packet length.
     * Upon error returns EOF.
     */
    private int readPktLen(DataInput in) throws IOException {
        pktlen = EOF;

        try {
            pktlen = in.readInt();
        } catch (EOFException e) {
            LOG.debug("Reached end of stream (EOFException)");
            return EOF;
        }
        if (pktlen == EOF) {
            LOG.debug("Reached end of stream (returned -1)");
        }

        return pktlen;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        /*
         * extract pkt len.
		 *
		 * GPSQL-1107:
		 * The DataInput might already be empty (EOF), but we can't check it beforehand.
		 * If that's the case, pktlen is updated to -1, to mark that the object is still empty.
		 * (can be checked with isEmpty()).
		 */
        pktlen = readPktLen(in);
        if (isEmpty()) {
            return;
        }

		/* extract the version and col cnt */
        int version = in.readShort();
        int curOffset = 4 + 2;
        int colCnt;

		/* !!! Check VERSION !!! */
        if (version != GPDBWritable.VERSION && version != GPDBWritable.PREV_VERSION) {
            throw new IOException("Current GPDBWritable version(" +
                    GPDBWritable.VERSION + ") does not match input version(" +
                    version + ")");
        }

        if (version == GPDBWritable.VERSION) {
            errorFlag = in.readByte();
            curOffset += 1;
        }

        colCnt = in.readShort();
        curOffset += 2;

		/* Extract Column Type */
        colType = new int[colCnt];
        DBType[] coldbtype = new DBType[colCnt];
        for (int i = 0; i < colCnt; i++) {
            int enumType = (in.readByte());
            curOffset += 1;
            if (enumType == DBType.BIGINT.ordinal()) {
                colType[i] = DataType.BIGINT.getOID();
                coldbtype[i] = DBType.BIGINT;
            } else if (enumType == DBType.BOOLEAN.ordinal()) {
                colType[i] = DataType.BOOLEAN.getOID();
                coldbtype[i] = DBType.BOOLEAN;
            } else if (enumType == DBType.FLOAT8.ordinal()) {
                colType[i] = DataType.FLOAT8.getOID();
                coldbtype[i] = DBType.FLOAT8;
            } else if (enumType == DBType.INTEGER.ordinal()) {
                colType[i] = DataType.INTEGER.getOID();
                coldbtype[i] = DBType.INTEGER;
            } else if (enumType == DBType.REAL.ordinal()) {
                colType[i] = DataType.REAL.getOID();
                coldbtype[i] = DBType.REAL;
            } else if (enumType == DBType.SMALLINT.ordinal()) {
                colType[i] = DataType.SMALLINT.getOID();
                coldbtype[i] = DBType.SMALLINT;
            } else if (enumType == DBType.BYTEA.ordinal()) {
                colType[i] = DataType.BYTEA.getOID();
                coldbtype[i] = DBType.BYTEA;
            } else if (enumType == DBType.TEXT.ordinal()) {
                colType[i] = DataType.TEXT.getOID();
                coldbtype[i] = DBType.TEXT;
            } else {
                throw new IOException("Unknown GPDBWritable.DBType ordinal value");
            }
        }

		/* Extract null bit array */
        byte[] nullbytes = new byte[getNullByteArraySize(colCnt)];
        in.readFully(nullbytes);
        curOffset += nullbytes.length;
        boolean[] colIsNull = byteArrayToBooleanArray(nullbytes, colCnt);

		/* extract column value */
        colValue = new Object[colCnt];
        for (int i = 0; i < colCnt; i++) {
            if (!colIsNull[i]) {
                /* Skip the alignment padding */
                int skipbytes = roundUpAlignment(curOffset, coldbtype[i].getAlignment()) - curOffset;
                for (int j = 0; j < skipbytes; j++) {
                    in.readByte();
                }
                curOffset += skipbytes;

				/* For fixed length type, increment the offset according to type type length here.
                 * For var length type (BYTEA, TEXT), we'll read 4 byte length header and the
				 * actual payload.
				 */
                int varcollen = -1;
                if (coldbtype[i].isVarLength()) {
                    varcollen = in.readInt();
                    curOffset += 4 + varcollen;
                } else {
                    curOffset += coldbtype[i].getTypeLength();
                }

                switch (DataType.get(colType[i])) {
                    case BIGINT: {
                        colValue[i] = in.readLong();
                        break;
                    }
                    case BOOLEAN: {
                        colValue[i] = in.readBoolean();
                        break;
                    }
                    case FLOAT8: {
                        colValue[i] = in.readDouble();
                        break;
                    }
                    case INTEGER: {
                        colValue[i] = in.readInt();
                        break;
                    }
                    case REAL: {
                        colValue[i] = in.readFloat();
                        break;
                    }
                    case SMALLINT: {
                        colValue[i] = in.readShort();
                        break;
                    }

					/* For BYTEA column, it has a 4 byte var length header. */
                    case BYTEA: {
                        colValue[i] = new byte[varcollen];
                        in.readFully((byte[]) colValue[i]);
                        break;
                    }
                    /* For text formatted column, it has a 4 byte var length header
                     * and it's always null terminated string.
					 * So, we can remove the last "\0" when constructing the string.
					 */
                    case TEXT: {
                        byte[] data = new byte[varcollen];
                        in.readFully(data, 0, varcollen);
                        colValue[i] = new String(data, 0, varcollen - 1, CHARSET);
                        break;
                    }

                    default:
                        throw new IOException("Unknown GPDBWritable ColType");
                }
            }
        }

		/* Skip the ending alignment padding */
        int skipbytes = roundUpAlignment(curOffset, 8) - curOffset;
        for (int j = 0; j < skipbytes; j++) {
            in.readByte();
        }
        curOffset += skipbytes;

        if (errorFlag != 0) {
            throw new IOException("Received error value " + errorFlag + " from format");
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int numCol = colType.length;
        boolean[] nullBits = new boolean[numCol];
        int colLength = 0;
        int padLength;
        byte[] padbytes = new byte[8];
        ByteArrayOutputStream types = new ByteArrayOutputStream();
        ByteArrayOutputStream values = new ByteArrayOutputStream();

        /*
         * Compute the total payload and header length
         * header = total length (4 byte), Version (2 byte), Error (1 byte), #col (2 byte)
         * col type array = #col * 1 byte
         * null bit array = ceil(#col/8)
         */
        int datlen = 4 + 2 + 1 + 2;
        datlen += numCol;
        datlen += getNullByteArraySize(numCol);

        for (int i = 0; i < numCol; i++) {
            /* Get the enum type */
            DBType coldbtype;
            switch (DataType.get(colType[i])) {
                case BIGINT:
                    coldbtype = DBType.BIGINT;
                    break;
                case BOOLEAN:
                    coldbtype = DBType.BOOLEAN;
                    break;
                case FLOAT8:
                    coldbtype = DBType.FLOAT8;
                    break;
                case INTEGER:
                    coldbtype = DBType.INTEGER;
                    break;
                case REAL:
                    coldbtype = DBType.REAL;
                    break;
                case SMALLINT:
                    coldbtype = DBType.SMALLINT;
                    break;
                case BYTEA:
                    coldbtype = DBType.BYTEA;
                    break;
                default:
                    coldbtype = DBType.TEXT;
            }
            types.write((byte) (coldbtype.ordinal()));

            /* Get the actual value, and set the null bit */
            if (colValue[i] == null) {
                nullBits[i] = true;
                colLength = 0;
            } else {
                nullBits[i] = false;

				/*
                 * For fixed length type, we get the fixed length.
				 * For var len binary format, the length is in the col value.
				 * For text format, we must convert encoding first.
				 */
                if (!coldbtype.isVarLength()) {
                    colLength = coldbtype.getTypeLength();
                }

				/* calculate and add the type alignment padding */
                padLength = roundUpAlignment(datlen, coldbtype.getAlignment()) - datlen;
                datlen += padLength;
                values.write(padbytes, 0, padLength);

                /* Now, write the actual column value */
                switch (DataType.get(colType[i])) {
                    case BIGINT:
                        values.write(longToByteArray((long) colValue[i]));
                        break;
                    case BOOLEAN:
                        values.write(boolToByte((boolean) colValue[i]));
                        break;
                    case FLOAT8:
                        values.write(doubleToByteArray((double) colValue[i]));
                        break;
                    case INTEGER:
                        values.write(intToByteArray((int) colValue[i]));
                        break;
                    case REAL:
                        values.write(floatToByteArray((float) colValue[i]));
                        break;
                    case SMALLINT:
                        values.write(shortToByteArray((short) colValue[i]));
                        break;

                    /* For BYTEA format, add 4byte length header at the beginning  */
                    case BYTEA:
                        colLength = ((byte[]) colValue[i]).length;
                        datlen += 4; /* for variable length type, we add a 4 byte length header */
                        values.write(intToByteArray(colLength));
                        values.write((byte[]) colValue[i]);
                        break;

                    /* For text format, add 4byte length header. string is already '\0' terminated */
                    default: {
                        colLength = ((String) colValue[i]).getBytes(CHARSET).length;
                        datlen += 4; /* for variable length type, we add a 4 byte length header */
                        values.write(intToByteArray(colLength));
                        byte[] data = ((String) colValue[i]).getBytes(CHARSET);
                        values.write(data);
                        break;
                    }
            }
            datlen += colLength;
        }


        }
        /*
         * Add the final alignment padding for the next record
         */
        int endpadding = roundUpAlignment(datlen, 8) - datlen;
        datlen += endpadding;

        /* Construct the packet header */
        out.writeInt(datlen);
        out.writeShort(VERSION);
        out.writeByte(errorFlag);
        out.writeShort(numCol);

        types.writeTo((OutputStream) out);
        out.write(boolArrayToByteArray(nullBits));
        values.writeTo((OutputStream) out);
        /* End padding */
        out.write(padbytes, 0, endpadding);
    }

    private byte boolToByte(boolean b) {
        return (byte) (b ? 1 : 0);
    }

    private byte[] shortToByteArray(int s) {
        return new byte[]{(byte) (s >> 8), (byte) (s)};
    }

    private byte[] intToByteArray(int i) {
        return new byte[]{(byte) (i >> 24), (byte) (i >> 16), (byte) (i >> 8), (byte) (i)};
    }

    private byte[] floatToByteArray(float f) {
        int bits = Float.floatToIntBits(f);
        return new byte[]{(byte) (bits >> 24), (byte) (bits >> 16), (byte) (bits >> 8), (byte) (bits)};
    }

    private byte[] longToByteArray(long l) {
        return new byte[]{(byte) (l >> 56), (byte) (l >> 48), (byte) (l >> 40), (byte) (l >> 32), (byte) (l >> 24), (byte) (l >> 16), (byte) (l >> 8), (byte) (l)};
    }

    private byte[] doubleToByteArray(double d) {
        long bits = Double.doubleToLongBits(d);
        return new byte[]{(byte) (bits >> 56), (byte) (bits >> 48), (byte) (bits >> 40), (byte) (bits >> 32), (byte) (bits >> 24), (byte) (bits >> 16), (byte) (bits >> 8), (byte) (bits)};
    }

    /**
     * Private helper to convert boolean array to byte array
     */
    private static byte[] boolArrayToByteArray(boolean[] data) {
        int len = data.length;
        byte[] bytes = new byte[getNullByteArraySize(len)];

        for (int i = 0, j = 0, k = 7; i < data.length; i++) {
            bytes[j] |= (data[i] ? 1 : 0) << k--;
            if (k < 0) {
                j++;
                k = 7;
            }
        }
        return bytes;
    }

    /**
     * Private helper to determine the size of the null byte array
     */
    private static int getNullByteArraySize(int colCnt) {
        return (colCnt / 8) + (colCnt % 8 != 0 ? 1 : 0);
    }

    /**
     * Private helper to convert byte array to boolean array
     */
    private static boolean[] byteArrayToBooleanArray(byte[] data, int colCnt) {
        boolean[] bools = new boolean[colCnt];
        for (int i = 0, j = 0, k = 7; i < bools.length; i++) {
            bools[i] = ((data[j] >> k--) & 0x01) == 1;
            if (k < 0) {
                j++;
                k = 7;
            }
        }
        return bools;
    }

    /**
     * Private helper to round up alignment for the given length
     */
    private int roundUpAlignment(int len, int align) {
        int commonAlignment = align;
        if (commonAlignment == 8) {
            commonAlignment = alignmentOfEightBytes;
        }
        return (((len) + ((commonAlignment) - 1)) & ~((commonAlignment) - 1));
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setLong(int colIdx, Long val)
            throws TypeMismatchException {
        checkType(DataType.BIGINT, colIdx, true);
        colValue[colIdx] = val;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setBoolean(int colIdx, Boolean val)
            throws TypeMismatchException {
        checkType(DataType.BOOLEAN, colIdx, true);
        colValue[colIdx] = val;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setBytes(int colIdx, byte[] val)
            throws TypeMismatchException {
        checkType(DataType.BYTEA, colIdx, true);
        colValue[colIdx] = val;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setString(int colIdx, String val)
            throws TypeMismatchException {
        checkType(DataType.TEXT, colIdx, true);
        colValue[colIdx] = (val != null) ? val + "\0": null;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setFloat(int colIdx, Float val)
            throws TypeMismatchException {
        checkType(DataType.REAL, colIdx, true);
        colValue[colIdx] = val;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setDouble(int colIdx, Double val)
            throws TypeMismatchException {
        checkType(DataType.FLOAT8, colIdx, true);
        colValue[colIdx] = val;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setInt(int colIdx, Integer val)
            throws TypeMismatchException {
        checkType(DataType.INTEGER, colIdx, true);
        colValue[colIdx] = val;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     * @throws TypeMismatchException the column type does not match
     */
    public void setShort(int colIdx, Short val)
            throws TypeMismatchException {
        checkType(DataType.SMALLINT, colIdx, true);
        colValue[colIdx] = val;
    }

    /**
     * Sets the column value of the record.
     *
     * @param colIdx the column index
     * @param val    the value
     */
    public void setObject(int colIdx, Object val) {
        colValue[colIdx] = val;
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public Long getLong(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.BIGINT, colIdx, false);
        return (Long) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public Boolean getBoolean(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.BOOLEAN, colIdx, false);
        return (Boolean) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public byte[] getBytes(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.BYTEA, colIdx, false);
        return (byte[]) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public String getString(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.TEXT, colIdx, false);
        return (String) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public Float getFloat(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.REAL, colIdx, false);
        return (Float) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public Double getDouble(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.FLOAT8, colIdx, false);
        return (Double) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public Integer getInt(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.INTEGER, colIdx, false);
        return (Integer) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     * @throws TypeMismatchException the column type does not match
     */
    public Short getShort(int colIdx)
            throws TypeMismatchException {
        checkType(DataType.SMALLINT, colIdx, false);
        return (Short) colValue[colIdx];
    }

    /**
     * Gets the column value of the record.
     *
     * @param colIdx the column index
     * @return column value
     */
    public Object getObject(int colIdx) {
        return colValue[colIdx];
    }

    /**
     * Sets the error field.
     *
     * @param errorVal the error value
     */
    public void setError(boolean errorVal) {
        errorFlag = errorVal ? (byte) 1 : (byte) 0;
    }

    /**
     * Returns a string representation of the object.
     */
    @Override
    public String toString() {
        if (colType == null) {
            return null;
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < colType.length; i++) {
            result.append("Column ").append(i).append(":");
            if (colValue[i] != null) {
                result.append(colType[i] == DataType.BYTEA.getOID()
                        ? byteArrayInString((byte[]) colValue[i])
                        : colValue[i]);
            }
            result.append("\n");
        }
        return result.toString();
    }

    /**
     * Helper printing function
     */
    private static String byteArrayInString(byte[] data) {
        StringBuilder result = new StringBuilder();
        for (Byte b : data) {
            result.append(b.intValue()).append(" ");
        }
        return result.toString();
    }

    /**
     * Private Helper to check the type mismatch
     * If the expected type is stored as string, then it must be set
     * via setString.
     * Otherwise, the type must match.
     */
    private void checkType(DataType inTyp, int idx, boolean isSet)
            throws TypeMismatchException {
        if (idx < 0 || idx >= colType.length) {
            throw new TypeMismatchException("Column index is out of range");
        }

        int exTyp = colType[idx];

        if (isTextForm(exTyp)) {
            if (inTyp != DataType.TEXT) {
                throw new TypeMismatchException(formErrorMsg(inTyp.getOID(), DataType.TEXT.getOID(), isSet));
            }
        } else if (inTyp != DataType.get(exTyp)) {
            throw new TypeMismatchException(formErrorMsg(inTyp.getOID(), exTyp, isSet));
        }
    }

    private String formErrorMsg(int inTyp, int colTyp, boolean isSet) {
        return isSet
                ? "Cannot set " + getTypeName(inTyp) + " to a " + getTypeName(colTyp) + " column"
                : "Cannot get " + getTypeName(inTyp) + " from a " + getTypeName(colTyp) + " column";
    }

    /**
     * Private Helper routine to tell whether a type is Text form or not
     *
     * @param type the type OID that we want to check
     */
    private boolean isTextForm(int type) {
        return DataType.isTextForm(type);
    }

    /**
     * Helper to get the type name.
     * If a given oid is not in the commonly used list, we
     * would expect a TEXT for it (for the error message).
     *
     * @param oid type OID
     * @return type name
     */
    public static String getTypeName(int oid) {
        DataType type = DataType.get(oid);
        if (type == DataType.UNSUPPORTED_TYPE)
            return DataType.TEXT.name();
        return type.name();
    }

    /*
     * Get alignment from command line to match to the alignment
     * the C code uses (see gphdfs/src/protocol_formatter/common.c).
     */
    private void initializeEightByteAlignment() {
        String alignment = System.getProperty("greenplum.alignment");
        if (alignment == null) {
            return;
        }
        alignmentOfEightBytes = Integer.parseInt(alignment);
    }

    /**
     * Returns if the writable object is empty,
     * based on the pkt len as read from stream.
     * -1 means nothing was read (eof).
     *
     * @return whether the writable object is empty
     */
    public boolean isEmpty() {
        return pktlen == EOF;
    }
}
