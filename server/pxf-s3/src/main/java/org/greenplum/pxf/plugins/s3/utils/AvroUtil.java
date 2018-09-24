package org.greenplum.pxf.plugins.s3.utils;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.api.utilities.InputData;

import java.util.ArrayList;
import java.util.List;

public class AvroUtil {

	public static final String NAMESPACE = "org.greenplum.avro";
	private static final Log LOG = LogFactory.getLog(AvroUtil.class);

	private AvroUtil() {
	}

	/**
	 * When writing Parquet or Avro, the Avro schema corresponding to the database rows selected
	 * in the query is required.  This method provides that schema.
	 * NOTE: the provided schema specifies that any of the fields can be null
	 *
	 * @param id the InputData
	 * @return the corresponding Avro Schema instance
	 */
	public static Schema schemaFromInputData(InputData id) {
		String schemaStr = "{\"namespace\": \"" + NAMESPACE + "\", \"type\": \"record\", ";
		// FIXME: is there a way to get the table name from InputData?
		schemaStr += "\"name\": \"mytable\", \"fields\": [";
		List<String> fieldList = new ArrayList<>();
		for (ColumnDescriptor cd : id.getTupleDescription()) {
			String fieldStr = "{\"name\": \"" + cd.columnName().toLowerCase() + "\", \"type\": ["
					+ asAvroType(DataType.get(cd.columnTypeCode())) + ", \"null\" ]}";
			fieldList.add(fieldStr);
		}
		schemaStr += String.join(", ", fieldList);
		schemaStr += "]}";
		LOG.info("Avro schema string: " + schemaStr);
		return new Schema.Parser().parse(schemaStr);
	}

	/**
	 * @param s the input string
	 * @return the input string, surrounded by double quotes
	 */
	public static String addQuotes(String s) {
		return "\"" + s + "\"";
	}

	/**
	 *
	 * @param gpType the DataType, from the database, to be resolved into an Avro type
	 * @return a String representation of the corresponding Avro data type, surrounded by double quotes
	 */
	public static String asAvroType(DataType gpType) {
		String rv = null;
		switch (gpType) {
		case BOOLEAN:
			rv = addQuotes("boolean");
			break;
		case BYTEA:
			rv = addQuotes("bytes");
			break;
		case BIGINT:
			rv = addQuotes("long");
			break;
		case SMALLINT:
		case INTEGER:
			rv = addQuotes("int");
			break;
		case TEXT:
		case BPCHAR:
		case VARCHAR:
			rv = addQuotes("string");
			break;
		case REAL:
			rv = addQuotes("float");
			break;
		case FLOAT8:
			rv = addQuotes("double");
			break;
		case NUMERIC: // FIXME: come up with a better approach for NUMERIC
			rv = addQuotes("string");
			break;
		/*
		 * Ref.
		 * https://avro.apache.org/docs/1.8.0/spec.html#Timestamp+%28millisecond+precision%29
		 * https://avro.apache.org/docs/1.8.1/api/java/index.html?org/apache/avro/SchemaBuilder.html
		 */
		case DATE:
			rv = "{ \"type\": \"string\", \"logicalType\": \"date\" }";
			break;
		case TIMESTAMP:
			rv = "{ \"type\": \"string\", \"logicalType\": \"timestamp-millis\" }";
			break;
		default:
			throw new RuntimeException("Unsupported type: " + gpType);
		}
		return rv;
	}
}
