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


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.io.BytesWritable;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BasePlugin;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Resolver;
import org.greenplum.pxf.plugins.hdfs.avro.AvroUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.HdfsUtilities;
import org.greenplum.pxf.plugins.hdfs.utilities.RecordkeyAdapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Class AvroResolver handles deserialization of records that were serialized
 * using the AVRO serialization framework.
 */
public class AvroResolver extends BasePlugin implements Resolver {
    private static final String MAPKEY_DELIM = ":";
    private static final String RECORDKEY_DELIM = ":";
    private static final String COLLECTION_DELIM = ",";
    private GenericRecord avroRecord = null;
    private DatumReader<GenericRecord> reader = null;
    // member kept to enable reuse, and thus avoid repeated allocation
    private BinaryDecoder decoder = null;
    private List<Schema.Field> fields = null;
    private RecordkeyAdapter recordkeyAdapter = new RecordkeyAdapter();
    private String collectionDelim;
    private String mapkeyDelim;
    private String recordkeyDelim;
    private HcfsType hcfsType;
    private AvroUtilities avroUtilities;

    /**
     * Constructs a new instance of the AvroFileAccessor
     */
    public AvroResolver() {
        avroUtilities = AvroUtilities.getInstance();
    }

    /*
     * Initializes an AvroResolver. Initializes Avro data structure: the Avro
     * record - fields information and the Avro record reader. All Avro data is
     * build from the Avro schema, which is based on the *.avsc file that was
     * passed by the user
     *
     * throws RuntimeException if Avro schema could not be retrieved or parsed
     */
    @Override
    public void initialize(RequestContext context) {
        super.initialize(context);

        hcfsType = HcfsType.getHcfsType(configuration, this.context);
        Schema schema = avroUtilities.obtainSchema(this.context, configuration, hcfsType);

        reader = new GenericDatumReader<>(schema);

        fields = schema.getFields();

        collectionDelim = this.context.getOption("COLLECTION_DELIM") == null ? COLLECTION_DELIM
                : this.context.getOption("COLLECTION_DELIM");
        mapkeyDelim = this.context.getOption("MAPKEY_DELIM") == null ? MAPKEY_DELIM
                : this.context.getOption("MAPKEY_DELIM");
        recordkeyDelim = this.context.getOption("RECORDKEY_DELIM") == null ? RECORDKEY_DELIM
                : this.context.getOption("RECORDKEY_DELIM");
    }

    /**
     * Returns a list of the fields of one record. Each record field is
     * represented by a OneField item. OneField item contains two fields: an
     * integer representing the field type and a Java Object representing the
     * field value.
     */
    @Override
    public List<OneField> getFields(OneRow row) throws Exception {
        avroRecord = makeAvroRecord(row.getData(), avroRecord);
        List<OneField> record = new LinkedList<>();

        int recordkeyIndex = (context.getRecordkeyColumn() == null) ? -1
                : context.getRecordkeyColumn().columnIndex();
        int currentIndex = 0;

        for (Schema.Field field : fields) {
            /*
             * Add the record key if exists
             */
            if (currentIndex == recordkeyIndex) {
                currentIndex += recordkeyAdapter.appendRecordkeyField(record,
                        context, row);
            }

            currentIndex += populateRecord(record,
                    avroRecord.get(field.name()), field.schema());
        }

        return record;
    }

    /**
     * Constructs and sets the fields of a {@link OneRow}.
     *
     * @param record list of {@link OneField}
     * @return the constructed {@link OneRow}
     */
    @Override
    public OneRow setFields(List<OneField> record) {
        GenericRecord genericRecord = new GenericData.Record((Schema) context.getMetadata());
        int cnt = 0;
        for (OneField field : record) {
            // Avro does not seem to understand regular byte arrays
            if (field.val instanceof byte[]) {
                field.val = ByteBuffer.wrap((byte[]) field.val);
            }
            genericRecord.put(cnt++, field.val);
        }
        return new OneRow(null, genericRecord);
    }

    /**
     * The record can arrive from one out of two different sources: a sequence
     * file or an AVRO file. If it comes from an AVRO file, then it was already
     * obtained as a {@link GenericRecord} when it was fetched from the
     * file so in this case a cast is enough.
     * On the other hand, if the source is a sequence file, then the input
     * parameter obj hides a bytes [] buffer which is in fact one Avro record
     * serialized. Here, we build the Avro record from the flat buffer, using
     * the AVRO API. Then (for both cases) in the remaining functions we build a
     * {@code List<OneField>} record from the Avro record.
     *
     * @param obj         object holding an Avro record
     * @param reuseRecord Avro record to be reused to create new record from obj
     * @return Avro record
     * @throws IOException if creating the Avro record from byte array failed
     */
    GenericRecord makeAvroRecord(Object obj, GenericRecord reuseRecord)
            throws IOException {
        if (obj instanceof GenericRecord) {
            return (GenericRecord) obj;
        } else {
            byte[] bytes = ((BytesWritable) obj).getBytes();
            decoder = DecoderFactory.get().binaryDecoder(bytes, decoder);
            return reader.read(reuseRecord, decoder);
        }
    }

    /**
     * For a given field in the Avro record we extract its value and insert it
     * into the output {@code List<OneField>} record. An Avro field can be a
     * primitive type or an array type.
     *
     * @param record      list of fields to be populated
     * @param fieldValue  field value
     * @param fieldSchema field schema
     * @return the number of populated fields
     */
    int populateRecord(List<OneField> record, Object fieldValue,
                       Schema fieldSchema) {

        Schema.Type fieldType = fieldSchema.getType();
        int ret = 0;

        switch (fieldType) {
            case ARRAY:
                if (fieldValue == null) {
                    return addOneFieldToRecord(record, DataType.TEXT, null);
                }
                List<OneField> listRecord = new LinkedList<>();
                ret = setArrayField(listRecord, fieldValue, fieldSchema);
                addOneFieldToRecord(record, DataType.TEXT, String.format("[%s]",
                        HdfsUtilities.toString(listRecord, collectionDelim)));
                break;
            case MAP:
                if (fieldValue == null) {
                    return addOneFieldToRecord(record, DataType.TEXT, null);
                }
                List<OneField> mapRecord = new LinkedList<>();
                ret = setMapField(mapRecord, fieldValue, fieldSchema);
                addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}",
                        HdfsUtilities.toString(mapRecord, collectionDelim)));
                break;
            case RECORD:
                if (fieldValue == null) {
                    return addOneFieldToRecord(record, DataType.TEXT, null);
                }
                List<OneField> recRecord = new LinkedList<>();
                ret = setRecordField(recRecord, fieldValue, fieldSchema);
                addOneFieldToRecord(record, DataType.TEXT, String.format("{%s}",
                        HdfsUtilities.toString(recRecord, collectionDelim)));
                break;
            case UNION:
                /*
                 * When an Avro field is actually a union, we resolve the type
                 * of the union element, and delegate the record update via
                 * recursion
                 */

                int unionIndex = GenericData.get().resolveUnion(fieldSchema, fieldValue);
                /*
                 * Retrieve index of the non null data type from the type array
                 * if value is null
                 */
                if (fieldValue == null) {
                    unionIndex ^= 1; // exclusive or assignment
                }
                ret = populateRecord(record, fieldValue, fieldSchema.getTypes().get(unionIndex));
                break;
            case ENUM:
                ret = addOneFieldToRecord(record, DataType.TEXT, fieldValue);
                break;
            case INT:
                ret = addOneFieldToRecord(record, DataType.INTEGER, fieldValue);
                break;
            case DOUBLE:
                ret = addOneFieldToRecord(record, DataType.FLOAT8, fieldValue);
                break;
            case STRING:
                fieldValue = (fieldValue != null) ? fieldValue.toString() : null;
                ret = addOneFieldToRecord(record, DataType.TEXT, fieldValue);
                break;
            case FLOAT:
                ret = addOneFieldToRecord(record, DataType.REAL, fieldValue);
                break;
            case LONG:
                ret = addOneFieldToRecord(record, DataType.BIGINT, fieldValue);
                break;
            case BYTES:
            case FIXED:
                ret = addOneFieldToRecord(record, DataType.BYTEA, fieldValue);
                break;
            case BOOLEAN:
                ret = addOneFieldToRecord(record, DataType.BOOLEAN, fieldValue);
                break;
            default:
                break;
        }
        return ret;
    }

    /**
     * When an Avro field is actually a record, we iterate through each field
     * for each entry, the field name and value are added to a local record
     * {@code List<OneField>} complexRecord with the necessary delimiter we
     * create an object of type OneField and insert it into the output
     * {@code List<OneField>} record.
     *
     * @param record    list of fields to be populated
     * @param value     field value
     * @param recSchema record schema
     * @return number of populated fields
     */
    int setRecordField(List<OneField> record, Object value, Schema recSchema) {

        GenericRecord rec = ((GenericData.Record) value);
        Schema fieldKeySchema = Schema.create(Schema.Type.STRING);
        int currentIndex = 0;
        for (Schema.Field field : recSchema.getFields()) {
            Schema fieldSchema = field.schema();
            Object fieldValue = rec.get(field.name());
            List<OneField> complexRecord = new LinkedList<>();
            populateRecord(complexRecord, field.name(), fieldKeySchema);
            populateRecord(complexRecord, fieldValue, fieldSchema);
            addOneFieldToRecord(record, DataType.TEXT,
                    HdfsUtilities.toString(complexRecord, recordkeyDelim));
            currentIndex++;
        }
        return currentIndex;
    }

    /**
     * When an Avro field is actually a map, we resolve the type of the map
     * value For each entry, the field name and value are added to a local
     * record we create an object of type OneField and insert it into the output
     * {@code List<OneField>} record.
     * <p>
     * Unchecked warning is suppressed to enable us to cast fieldValue to a Map.
     * (since the value schema has been identified to me of type map)
     *
     * @param record     list of fields to be populated
     * @param fieldValue field value
     * @param mapSchema  map schema
     * @return number of populated fields
     */
    @SuppressWarnings("unchecked")
    int setMapField(List<OneField> record, Object fieldValue, Schema mapSchema) {
        Schema keySchema = Schema.create(Schema.Type.STRING);
        Schema valueSchema = mapSchema.getValueType();
        Map<String, ?> avroMap = ((Map<String, ?>) fieldValue);
        for (Map.Entry<String, ?> entry : avroMap.entrySet()) {
            List<OneField> complexRecord = new LinkedList<>();
            populateRecord(complexRecord, entry.getKey(), keySchema);
            populateRecord(complexRecord, entry.getValue(), valueSchema);
            addOneFieldToRecord(record, DataType.TEXT,
                    HdfsUtilities.toString(complexRecord, mapkeyDelim));
        }
        return avroMap.size();
    }

    /**
     * When an Avro field is actually an array, we resolve the type of the array
     * element, and for each element in the Avro array, we recursively invoke
     * the population of {@code List<OneField>} record.
     *
     * @param record      list of fields to be populated
     * @param fieldValue  field value
     * @param arraySchema array schema
     * @return number of populated fields
     */
    int setArrayField(List<OneField> record, Object fieldValue,
                      Schema arraySchema) {
        Schema typeSchema = arraySchema.getElementType();
        GenericData.Array<?> array = (GenericData.Array<?>) fieldValue;
        int length = array.size();
        for (int i = 0; i < length; i++) {
            populateRecord(record, array.get(i), typeSchema);
        }
        return length;
    }

    /**
     * Creates the {@link OneField} object and adds it to the output {@code List<OneField>}
     * record. Strings and byte arrays are held inside special types in the Avro
     * record so we transfer them to standard types in order to enable their
     * insertion in the GPDBWritable instance.
     *
     * @param record           list of fields to be populated
     * @param gpdbWritableType field type
     * @param val              field value
     * @return 1 (number of populated fields)
     */
    int addOneFieldToRecord(List<OneField> record, DataType gpdbWritableType,
                            Object val) {
        OneField oneField = new OneField();
        oneField.type = gpdbWritableType.getOID();
        switch (gpdbWritableType) {
            case BYTEA:
                if (val == null) {
                    oneField.val = null;
                } else if (val instanceof ByteBuffer) {
                    oneField.val = ((ByteBuffer) val).array();
                } else {
                    /**
                     * Entry point when the underlying bytearray is from a Fixed
                     * data
                     */
                    oneField.val = ((GenericData.Fixed) val).bytes();
                }
                break;
            default:
                oneField.val = val;
                break;
        }

        record.add(oneField);
        return 1;
    }
}
