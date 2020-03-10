package org.greenplum.pxf.plugins.hive;

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

import org.apache.commons.lang.CharUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.greenplum.pxf.api.BadRecordException;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.UnsupportedTypeException;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.Utilities;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hive.utilities.HiveUtilities;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Class HiveResolver handles deserialization of records that were serialized
 * using Hadoop's Hive serialization framework.
 */
public class HiveResolver extends HivePlugin implements Resolver {
    private static final Log LOG = LogFactory.getLog(HiveResolver.class);
    protected static final String MAPKEY_DELIM = ":";
    protected static final String COLLECTION_DELIM = ",";
    protected static final String nullChar = "\\N";
    protected char delimiter;
    protected String collectionDelim;
    protected String mapkeyDelim;
    protected Deserializer deserializer;
    protected String serdeClassName;
    protected String propsString;
    protected String partitionKeys;

    private int numberOfPartitions;
    private List<OneField> partitionFields;
    private String hiveDefaultPartName;

    /**
     * Initializes the HiveResolver by parsing the request context and
     * obtaining the serde class name, the serde properties string and the
     * partition keys.
     * @param context request context
     */
    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        hiveDefaultPartName = HiveConf.getVar(configuration, HiveConf.ConfVars.DEFAULTPARTITIONNAME);

        try {
            parseUserData(this.context);
            initPartitionFields();
            initSerde(this.context);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize HiveResolver", e);
        }
    }

    @Override
    public List<OneField> getFields(OneRow onerow) throws Exception {
        Object tuple = deserializer.deserialize((Writable) onerow.getData());
        // Each Hive record is a Struct
        StructObjectInspector soi = (StructObjectInspector) deserializer.getObjectInspector();
        List<OneField> record = traverseStruct(tuple, soi, false);
        /*
         * We follow Hive convention. Partition fields are always added at the
         * end of the record
         */
        record.addAll(partitionFields);

        return record;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     * @throws Exception if constructing a row from the fields failed
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        throw new UnsupportedOperationException();
    }

    public List<OneField> getPartitionFields() {
        return partitionFields;
    }

    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    /* Parses user data string (arrived from fragmenter). */
    void parseUserData(RequestContext input) throws Exception {
        HiveUserData hiveUserData = HiveUtilities.parseHiveUserData(input);

        serdeClassName = hiveUserData.getSerdeClassName();
        propsString = hiveUserData.getPropertiesString();
        partitionKeys = hiveUserData.getPartitionKeys();

        collectionDelim = input.getOption("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : input.getOption("COLLECTION_DELIM");
        mapkeyDelim = input.getOption("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : input.getOption("MAPKEY_DELIM");
    }

    /*
     * Gets and init the deserializer for the records of this Hive data
     * fragment.
     */
    void initSerde(RequestContext requestContext) throws Exception {
        Properties serdeProperties;

        Class<?> c = Class.forName(serdeClassName, true, JavaUtils.getClassLoader());
        deserializer = (Deserializer) c.getDeclaredConstructor().newInstance();
        serdeProperties = new Properties();
        if (propsString != null) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(propsString.getBytes());
            serdeProperties.load(inStream);
        } else {
            throw new IllegalArgumentException("propsString is mandatory to initialize serde.");
        }
        deserializer.initialize(new JobConf(configuration, HiveResolver.class), serdeProperties);
    }

    /*
     * The partition fields are initialized one time base on userData provided
     * by the fragmenter.
     */
    void initPartitionFields() {
        partitionFields = new LinkedList<>();
        if (partitionKeys.equals(HiveDataFragmenter.HIVE_NO_PART_TBL)) {
            return;
        }

        String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
        for (String partLevel : partitionLevels) {
            String[] levelKey = partLevel.split(HiveDataFragmenter.HIVE_1_PART_DELIM);
            String type = levelKey[1];
            String val = levelKey[2];
            DataType convertedType;
            Object convertedValue = null;
            boolean isDefaultPartition = false;

            // check if value is default partition
            isDefaultPartition = isDefaultPartition(type, val);
            // ignore the type's parameters
            String typeName = type.replaceAll("\\(.*\\)", "");

            switch (typeName) {
                case serdeConstants.STRING_TYPE_NAME:
                    convertedType = DataType.TEXT;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.BOOLEAN_TYPE_NAME:
                    convertedType = DataType.BOOLEAN;
                    convertedValue = isDefaultPartition ? null
                            : Boolean.valueOf(val);
                    break;
                case serdeConstants.TINYINT_TYPE_NAME:
                case serdeConstants.SMALLINT_TYPE_NAME:
                    convertedType = DataType.SMALLINT;
                    convertedValue = isDefaultPartition ? null
                            : Short.parseShort(val);
                    break;
                case serdeConstants.INT_TYPE_NAME:
                    convertedType = DataType.INTEGER;
                    convertedValue = isDefaultPartition ? null
                            : Integer.parseInt(val);
                    break;
                case serdeConstants.BIGINT_TYPE_NAME:
                    convertedType = DataType.BIGINT;
                    convertedValue = isDefaultPartition ? null
                            : Long.parseLong(val);
                    break;
                case serdeConstants.FLOAT_TYPE_NAME:
                    convertedType = DataType.REAL;
                    convertedValue = isDefaultPartition ? null
                            : Float.parseFloat(val);
                    break;
                case serdeConstants.DOUBLE_TYPE_NAME:
                    convertedType = DataType.FLOAT8;
                    convertedValue = isDefaultPartition ? null
                            : Double.parseDouble(val);
                    break;
                case serdeConstants.TIMESTAMP_TYPE_NAME:
                    convertedType = DataType.TIMESTAMP;
                    convertedValue = isDefaultPartition ? null
                            : Timestamp.valueOf(val);
                    break;
                case serdeConstants.DATE_TYPE_NAME:
                    convertedType = DataType.DATE;
                    convertedValue = isDefaultPartition ? null
                            : Date.valueOf(val);
                    break;
                case serdeConstants.DECIMAL_TYPE_NAME:
                    convertedType = DataType.NUMERIC;
                    convertedValue = isDefaultPartition ? null
                            : HiveDecimal.create(val).bigDecimalValue().toString();
                    break;
                case serdeConstants.VARCHAR_TYPE_NAME:
                    convertedType = DataType.VARCHAR;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.CHAR_TYPE_NAME:
                    convertedType = DataType.BPCHAR;
                    convertedValue = isDefaultPartition ? null : val;
                    break;
                case serdeConstants.BINARY_TYPE_NAME:
                    convertedType = DataType.BYTEA;
                    convertedValue = isDefaultPartition ? null : val.getBytes();
                    break;
                default:
                    throw new UnsupportedTypeException(
                            "Unsupported partition type: " + type);
            }
            addOneFieldToRecord(partitionFields, convertedType, convertedValue);
        }
        numberOfPartitions = partitionFields.size();
    }

    /*
     * The partition fields are initialized one time based on userData provided
     * by the fragmenter.
     */
    void initTextPartitionFields(StringBuilder parts) {
        if (partitionKeys.equals(HiveDataFragmenter.HIVE_NO_PART_TBL)) {
            return;
        }
        String[] partitionLevels = partitionKeys.split(HiveDataFragmenter.HIVE_PARTITIONS_DELIM);
        for (String partLevel : partitionLevels) {
            String[] levelKey = partLevel.split(HiveDataFragmenter.HIVE_1_PART_DELIM);
            String type = levelKey[1];
            String val = levelKey[2];
            parts.append(delimiter);
            if (isDefaultPartition(type, val)) {
                parts.append(nullChar);
            } else {
                // ignore the type's parameters
                String typeName = type.replaceAll("\\(.*\\)", "");
                switch (typeName) {
                    case serdeConstants.STRING_TYPE_NAME:
                    case serdeConstants.VARCHAR_TYPE_NAME:
                    case serdeConstants.CHAR_TYPE_NAME:
                        parts.append(val);
                        break;
                    case serdeConstants.BOOLEAN_TYPE_NAME:
                        parts.append(Boolean.parseBoolean(val));
                        break;
                    case serdeConstants.TINYINT_TYPE_NAME:
                    case serdeConstants.SMALLINT_TYPE_NAME:
                        parts.append(Short.parseShort(val));
                        break;
                    case serdeConstants.INT_TYPE_NAME:
                        parts.append(Integer.parseInt(val));
                        break;
                    case serdeConstants.BIGINT_TYPE_NAME:
                        parts.append(Long.parseLong(val));
                        break;
                    case serdeConstants.FLOAT_TYPE_NAME:
                        parts.append(Float.parseFloat(val));
                        break;
                    case serdeConstants.DOUBLE_TYPE_NAME:
                        parts.append(Double.parseDouble(val));
                        break;
                    case serdeConstants.TIMESTAMP_TYPE_NAME:
                        parts.append(Timestamp.valueOf(val));
                        break;
                    case serdeConstants.DATE_TYPE_NAME:
                        parts.append(Date.valueOf(val));
                        break;
                    case serdeConstants.DECIMAL_TYPE_NAME:
                        parts.append(HiveDecimal.create(val).bigDecimalValue());
                        break;
                    case serdeConstants.BINARY_TYPE_NAME:
                        Utilities.byteArrayToOctalString(val.getBytes(), parts);
                        break;
                    default:
                        throw new UnsupportedTypeException(
                                "Unsupported partition type: " + type);
                }
            }
        }
        this.numberOfPartitions = partitionLevels.length;
    }

    /**
     * Returns true if the partition value is Hive's default partition name
     * (defined in hive.exec.default.partition.name).
     *
     * @param partitionType  partition field type
     * @param partitionValue partition value
     * @return true if the partition value is Hive's default partition
     */
    private boolean isDefaultPartition(String partitionType,
                                       String partitionValue) {
        boolean isDefaultPartition = false;
        if (hiveDefaultPartName.equals(partitionValue)) {
            LOG.debug("partition " + partitionType
                    + " is hive default partition (value " + partitionValue
                    + "), converting field to NULL");
            isDefaultPartition = true;
        }
        return isDefaultPartition;
    }

    /*
     * If the object representing the whole record is null or if an object
     * representing a composite sub-object (map, list,..) is null - then
     * BadRecordException will be thrown. If a primitive field value is null,
     * then a null will appear for the field in the record in the query result.
     * flatten is true only when we are dealing with a non primitive field
     */
    private void traverseTuple(Object obj, ObjectInspector objInspector,
                               List<OneField> record, boolean toFlatten)
            throws IOException, BadRecordException {
        ObjectInspector.Category category = objInspector.getCategory();
        switch (category) {
            case PRIMITIVE:
                resolvePrimitive(obj, (PrimitiveObjectInspector) objInspector,
                        record, toFlatten);
                break;
            case LIST:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> listRecord = traverseList(obj,
                            (ListObjectInspector) objInspector);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("[%s]",
                            HdfsUtilities.toString(listRecord, collectionDelim)));
                }
                break;
            case MAP:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> mapRecord = traverseMap(obj,
                            (MapObjectInspector) objInspector);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}",
                            HdfsUtilities.toString(mapRecord, collectionDelim)));
                }
                break;
            case STRUCT:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> structRecord = traverseStruct(obj,
                            (StructObjectInspector) objInspector, true);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}",
                            HdfsUtilities.toString(structRecord, collectionDelim)));
                }
                break;
            case UNION:
                if (obj == null) {
                    addOneFieldToRecord(record, DataType.TEXT, null);
                } else {
                    List<OneField> unionRecord = traverseUnion(obj,
                            (UnionObjectInspector) objInspector);
                    addOneFieldToRecord(record, DataType.TEXT, String.format("[%s]",
                            HdfsUtilities.toString(unionRecord, collectionDelim)));
                }
                break;
            default:
                throw new UnsupportedTypeException("Unknown category type: "
                        + objInspector.getCategory());
        }
    }

    private List<OneField> traverseUnion(Object obj, UnionObjectInspector uoi)
            throws BadRecordException, IOException {
        List<OneField> unionRecord = new LinkedList<>();
        List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
        if (ois == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Union");
        }
        traverseTuple(uoi.getField(obj), ois.get(uoi.getTag(obj)), unionRecord,
                true);
        return unionRecord;
    }

    private List<OneField> traverseList(Object obj, ListObjectInspector loi)
            throws BadRecordException, IOException {
        List<OneField> listRecord = new LinkedList<>();
        List<?> list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type List");
        }
        for (Object object : list) {
            traverseTuple(object, eoi, listRecord, true);
        }
        return listRecord;
    }

    protected List<OneField> traverseStruct(Object struct,
                                            StructObjectInspector soi,
                                            boolean toFlatten)
            throws BadRecordException, IOException {
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        List<Object> structFields = soi.getStructFieldsDataAsList(struct);
        if (structFields == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Struct");
        }
        List<OneField> structRecord = new LinkedList<>();
        List<OneField> complexRecord = new LinkedList<>();
        List<ColumnDescriptor> colData = context.getTupleDescription();
        for (int i = 0; i < structFields.size(); i++) {
            if (toFlatten) {
                complexRecord.add(new OneField(DataType.TEXT.getOID(), String.format(
                        "\"%s\"", fields.get(i).getFieldName())));
            } else if (!colData.get(i).isProjected()) {
                // Non-projected fields will be sent as null values.
                // This case is invoked only in the top level of fields and
                // not when interpreting fields of type struct.
                traverseTuple(null, fields.get(i).getFieldObjectInspector(),
                        complexRecord, toFlatten);
                continue;
            }
            traverseTuple(structFields.get(i),
                    fields.get(i).getFieldObjectInspector(), complexRecord,
                    toFlatten);
            if (toFlatten) {
                addOneFieldToRecord(structRecord, DataType.TEXT,
                        HdfsUtilities.toString(complexRecord, mapkeyDelim));
                complexRecord.clear();
            }
        }
        return toFlatten ? structRecord : complexRecord;
    }

    private List<OneField> traverseMap(Object obj, MapObjectInspector moi)
            throws BadRecordException, IOException {
        List<OneField> complexRecord = new LinkedList<>();
        List<OneField> mapRecord = new LinkedList<>();
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();
        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
            throw new BadRecordException(
                    "Illegal value NULL for Hive data type Map");
        } else if (map.isEmpty()) {
            traverseTuple(null, koi, complexRecord, true);
            traverseTuple(null, voi, complexRecord, true);
            addOneFieldToRecord(mapRecord, DataType.TEXT,
                    HdfsUtilities.toString(complexRecord, mapkeyDelim));
        } else {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                traverseTuple(entry.getKey(), koi, complexRecord, true);
                traverseTuple(entry.getValue(), voi, complexRecord, true);
                addOneFieldToRecord(mapRecord, DataType.TEXT,
                        HdfsUtilities.toString(complexRecord, mapkeyDelim));
                complexRecord.clear();
            }
        }
        return mapRecord;
    }

    private void resolvePrimitive(Object o, PrimitiveObjectInspector oi,
                                  List<OneField> record, boolean toFlatten)
            throws IOException {
        Object val;
        switch (oi.getPrimitiveCategory()) {
            case BOOLEAN: {
                val = (o != null) ? ((BooleanObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.BOOLEAN, val);
                break;
            }
            case SHORT: {
                if (o == null) {
                    val = null;
                } else if (o.getClass().getSimpleName().equals("ByteWritable")) {
                    val = Short.valueOf(((ByteWritable) o).get());
                } else {
                    val = ((ShortObjectInspector) oi).get(o);
                }
                addOneFieldToRecord(record, DataType.SMALLINT, val);
                break;
            }
            case INT: {
                val = (o != null) ? ((IntObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.INTEGER, val);
                break;
            }
            case LONG: {
                val = (o != null) ? ((LongObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.BIGINT, val);
                break;
            }
            case FLOAT: {
                val = (o != null) ? ((FloatObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.REAL, val);
                break;
            }
            case DOUBLE: {
                val = (o != null) ? ((DoubleObjectInspector) oi).get(o) : null;
                addOneFieldToRecord(record, DataType.FLOAT8, val);
                break;
            }
            case DECIMAL: {
                String sVal = null;
                if (o != null) {
                    HiveDecimal hd = ((HiveDecimalObjectInspector) oi).getPrimitiveJavaObject(o);
                    if (hd != null) {
                        BigDecimal bd = hd.bigDecimalValue();
                        sVal = bd.toString();
                    }
                }
                addOneFieldToRecord(record, DataType.NUMERIC, sVal);
                break;
            }
            case STRING: {
                val = (o != null) ? ((StringObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.TEXT,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            }
            case VARCHAR:
                val = (o != null) ? ((HiveVarcharObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.VARCHAR,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            case CHAR:
                val = (o != null) ? ((HiveCharObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.BPCHAR,
                        toFlatten ? String.format("\"%s\"", val) : val);
                break;
            case BINARY: {
                byte[] toEncode = null;
                if (o != null) {
                    BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(o);
                    toEncode = new byte[bw.getLength()];
                    System.arraycopy(bw.getBytes(), 0, toEncode, 0,
                            bw.getLength());
                }
                addOneFieldToRecord(record, DataType.BYTEA, toEncode);
                break;
            }
            case TIMESTAMP: {
                val = (o != null) ? ((TimestampObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.TIMESTAMP, val);
                break;
            }
            case DATE:
                val = (o != null) ? ((DateObjectInspector) oi).getPrimitiveJavaObject(o)
                        : null;
                addOneFieldToRecord(record, DataType.DATE, val);
                break;
            case BYTE: { /* TINYINT */
                val = (o != null) ? Short.valueOf(((ByteObjectInspector) oi).get(o))
                        : null;
                addOneFieldToRecord(record, DataType.SMALLINT, val);
                break;
            }
            default: {
                throw new UnsupportedTypeException(oi.getTypeName()
                        + " conversion is not supported by "
                        + getClass().getSimpleName());
            }
        }
    }

    private void addOneFieldToRecord(List<OneField> record,
                                     DataType gpdbWritableType, Object val) {
        record.add(new OneField(gpdbWritableType.getOID(), val));
    }

    /*
     * Gets the delimiter character from the URL, verify and store it. Must be a
     * single ascii character (same restriction as Gpdb's). If a hex
     * representation was passed, convert it to its char.
     */
    void parseDelimiterChar(RequestContext input) {

        String userDelim = String.valueOf(input.getGreenplumCSV().getDelimiter());

        if (userDelim == null) {
            /* No DELIMITER in URL, try to get it from fragment's user data*/
            HiveUserData hiveUserData = HiveUtilities.parseHiveUserData(input);
            if (hiveUserData.getDelimiter() == null) {
                throw new IllegalArgumentException("DELIMITER is a required option");
            }
            delimiter = (char) Integer.valueOf(hiveUserData.getDelimiter()).intValue();
        } else {
            final int VALID_LENGTH = 1;
            final int VALID_LENGTH_HEX = 4;
            if (userDelim.startsWith("\\x")) { // hexadecimal sequence
                if (userDelim.length() != VALID_LENGTH_HEX) {
                    throw new IllegalArgumentException(
                            "Invalid hexdecimal value for delimiter (got"
                                    + userDelim + ")");
                }
                delimiter = (char) Integer.parseInt(
                        userDelim.substring(2, VALID_LENGTH_HEX), 16);
                if (!CharUtils.isAscii(delimiter)) {
                    throw new IllegalArgumentException(
                            "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII "
                                    + delimiter + ")");
                }
                return;
            }
            if (userDelim.length() != VALID_LENGTH) {
                throw new IllegalArgumentException(
                        "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got "
                                + userDelim + ")");
            }
            if (!CharUtils.isAscii(userDelim.charAt(0))) {
                throw new IllegalArgumentException(
                        "Invalid delimiter value. Must be a single ASCII character, or a hexadecimal sequence (got non ASCII "
                                + userDelim + ")");
            }
            delimiter = userDelim.charAt(0);
        }
    }
}
