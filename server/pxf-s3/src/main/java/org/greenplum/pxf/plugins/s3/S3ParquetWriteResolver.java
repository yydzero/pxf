package org.greenplum.pxf.plugins.s3;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.OneField;
import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.WriteResolver;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.InputData;
import org.greenplum.pxf.api.utilities.Plugin;
import org.greenplum.pxf.plugins.s3.utils.AvroUtil;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unused")
public class S3ParquetWriteResolver extends Plugin implements WriteResolver {

	private static final Log LOG = LogFactory.getLog(S3ParquetWriteResolver.class);

	// Required for creating GenericRecord containing the row of data
	private Schema avroSchema;

	public S3ParquetWriteResolver(InputData input) {
		super(input);
		avroSchema = AvroUtil.schemaFromInputData(input);
	}

	/*
	 * Refer to:
	 * https://github.com/apache/parquet-mr/blob/master/parquet-avro/src/test/java/
	 * org/apache/parquet/avro/TestReadWrite.java
	 * https://avro.apache.org/docs/1.8.1/api/java/org/apache/avro/generic/
	 * GenericData.Record.html
	 */
	@Override
	public OneRow setFields(List<OneField> oneFieldList) throws Exception {
		GenericData.Record record = new GenericData.Record(avroSchema);
		List<Schema.Field> fieldList = avroSchema.getFields();
		// TODO: handle type conversion, from PostgreSQL to Avro
		for (int i = 0; i < oneFieldList.size(); i++) {
			OneField oneField = oneFieldList.get(i);
			int dbType = oneField.type;
			Object value = oneField.val;

			// What's the Avro type, that we'll have to convert to?
			Schema.Field avroField = fieldList.get(i);
			Schema fieldSchema = avroField.schema();
			Type avroType = fieldSchema.getType();
			List<String> typeList = new ArrayList<>();
			// TODO: move this date/time resolution code into AvroUtil
			if (avroType == Schema.Type.UNION) {
				for (Schema s : fieldSchema.getTypes()) {
					Type t = s.getType();
					LogicalType lt = s.getLogicalType();
					String ltName = "null";
					if (lt != null) {
						ltName = lt.getName();
					}
					/* Here, the value on the right is lt.getName():
					 *
					 *   DATE: (Type.INT, LogicalType = date)
					 *   TIME: (Type.INT, LogicalType = time-millis)
					 *   TIMESTAMP: (Type.LONG, LogicalType = timestamp-millis)
					 *
					 * Validated that these do get read back from Parquet with the specfied
					 * types (int or long).
					 */
					if (null != value) {
						if ("date".equals(ltName) && Type.STRING == t) {
							// DATE
							value = (String) value;
						} else if ("timestamp-millis".equals(ltName) && Type.STRING == t) {
							// TIMESTAMP
							value = (String) value;
						}
					}
					typeList.add(s.toString() + "(Type." + t + ", LogicalType = " + ltName + ")");
				}
			}
			LOG.debug("type: " + DataType.get(dbType) + ", value: " + (value == null ? "null" : value.toString())
					+ ", types: " + String.join(", ", typeList));
			record.put(fieldList.get(i).name(), value);
		}
		return new OneRow(null, record);
	}

}
