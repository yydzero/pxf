package org.greenplum.pxf.plugins.hdfs.utilities;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.ColumnDescriptor;
import org.greenplum.pxf.plugins.hdfs.HcfsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

public final class AvroUtilities {
    private static String COMMON_NAMESPACE = "public.avro";

    private FileSearcher fileSearcher;
    private String schemaPath;
    private final static Logger LOG = LoggerFactory.getLogger(AvroUtilities.class);

    public interface FileSearcher {
        File searchForFile(String filename);
    }

    // default constructor
    private AvroUtilities() {
        fileSearcher = (file) -> {
            try {
                return searchForFile(file);
            } catch (UnsupportedEncodingException e) {
                LOG.info(e.toString());
                return null;
            }
        };
    }

    private static AvroUtilities instance = new AvroUtilities();

    // constructor for use in test
    AvroUtilities(FileSearcher fileSearcher) {
        this.fileSearcher = fileSearcher;
    }

    public static AvroUtilities getInstance() {
        return instance;
    }

    /**
     * All-purpose method for obtaining an Avro schema based on the request context and
     * HCFS config.
     *
     * @param context
     * @param configuration
     * @return
     */
    public Schema obtainSchema(RequestContext context, Configuration configuration, HcfsType hcfsType) {
        Schema schema = (Schema) context.getMetadata();

        if (schema != null) {
            return schema;
        }
        try {
            schemaPath = context.getDataSource();
            schema = readOrGenerateAvroSchema(context, configuration, hcfsType);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to obtain Avro schema from '%s'", schemaPath), e);
        }
        context.setMetadata(schema);
        return schema;
    }

    private Schema readOrGenerateAvroSchema(RequestContext context, Configuration configuration, HcfsType hcfsType) throws IOException, URISyntaxException {
        // user-provided schema trumps everything
        String userProvidedSchemaFile = context.getOption("SCHEMA");
        if (userProvidedSchemaFile != null) {
            schemaPath = userProvidedSchemaFile;
            if (userProvidedSchemaFile.matches("^.*\\.avsc$")) {
                return readSchemaFromUserProvidedJson(configuration, userProvidedSchemaFile, hcfsType);
            }
            return readSchemaFromUserProvidedAvroBinary(configuration, userProvidedSchemaFile, hcfsType);
        }

        // if we are writing we must generate the schema if there is none to read
        if (context.getRequestType() == RequestContext.RequestType.WRITE_BRIDGE) {
            return generateSchema(context.getTupleDescription());
        }

        // reading from external: get the schema from data source
        return readSchemaFromAvroDataSource(configuration, context.getDataSource());
    }

    private Schema readSchemaFromUserProvidedJson(Configuration configuration, String schemaName, HcfsType hcfsType) throws IOException, URISyntaxException {

        InputStream schemaStream = null;
        // try searching local disk (full path name and relative path in classpath) first.
        // by searching locally first we can avoid an extra call to external (fs.exists())
        try {
            File file = fileSearcher.searchForFile(schemaName);
            if (file == null) {
                Path path = new Path(hcfsType.getDataUri(configuration, schemaName));
                FileSystem fs = FileSystem.get(path.toUri(), configuration);
                schemaStream = new FSDataInputStream(fs.open(path));
            } else {
                schemaStream = new FileInputStream(file);
            }
            return (new Schema.Parser()).parse(schemaStream);
        } finally {
            if (schemaStream != null) {
                schemaStream.close();
            }
        }
    }

    // static Path schemaPathFromContext(RequestContext context, String schemaName) {
    //     if (context.getProfileScheme() == null) {
    //         return new Path(schemaName);
    //     }
    //     // grab the bucket name from dataSource
    //     // read/write are inconsistent about whether context.getDataSource()
    //     // includes the profile scheme or not
    //     String bucket = context
    //             .getDataSource()
    //             .replaceFirst("^(.*://)?/*([^:/]+)/.*$", "$2");
    //     return new Path(context.getProtocol() + "://" + bucket + schemaName);
    // }

    /**
     * Accessing the Avro file through the "unsplittable" API just to get the
     * schema. The splittable API (AvroInputFormat) which is the one we will be
     * using to fetch the records, does not support getting the Avro schema yet.
     *
     * @param configuration Hadoop configuration
     * @param avroFile      Avro file (i.e fileName.avro) path
     * @return the Avro schema
     * @throws IOException if I/O error occurred while accessing Avro schema file
     */
    private Schema readSchemaFromUserProvidedAvroBinary(Configuration configuration, String avroFile, HcfsType hcfsType) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> fileReader = null;

        try {
            File file = fileSearcher.searchForFile(avroFile);
            if (file == null) {
                final Path path = new Path(hcfsType.getDataUri(configuration, avroFile));
                FsInput inStream = new FsInput(path, configuration);
                fileReader = new DataFileReader<>(inStream, datumReader);
            } else {
                fileReader = new DataFileReader<>(file, datumReader);
            }
        } finally {
            if (fileReader != null) {
                fileReader.close();
            }
        }
        return fileReader.getSchema();
    }

    private static Schema readSchemaFromAvroDataSource(Configuration configuration, String dataSource) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        FsInput inStream = new FsInput(new Path(dataSource), configuration);

        try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(inStream, datumReader)) {
            return fileReader.getSchema();
        }
    }

    /*
     * if user provided a full path, use that.
     * otherwise we need to check classpath
     */
    private static File searchForFile(String schemaName) throws UnsupportedEncodingException {
        File file = new File(schemaName);
        if (!file.exists()) {
            URL url = AvroUtilities.class.getClassLoader().getResource(schemaName);

            // Testing that the schema resource exists
            if (url == null) {
                return null;
            }
            file = new File(URLDecoder.decode(url.getPath(), "UTF-8"));
        }
        return file;
    }

    private static Schema generateSchema(List<ColumnDescriptor> tupleDescription) throws IOException {
        Schema schema = Schema.createRecord("tableName", "", COMMON_NAMESPACE, false);
        List<Schema.Field> fields = new ArrayList<>();

        for (ColumnDescriptor cd : tupleDescription) {
            fields.add(new Schema.Field(
                    cd.columnName(),
                    getFieldSchema(DataType.get(cd.columnTypeCode()), cd.columnName()),
                    "",
                    null
            ));
        }

        schema.setFields(fields);

        return schema;
    }

    private static Schema getFieldSchema(DataType type, String colName) throws IOException {
        List<Schema> unionList = new ArrayList<>();
        // in this version of gpdb, external table should not set 'notnull' attribute
        // so we should use union between NULL and another type everywhere
        unionList.add(Schema.create(Schema.Type.NULL));

        switch (type) {
            case BOOLEAN:
                unionList.add(Schema.create(Schema.Type.BOOLEAN));
                break;
            case BYTEA:
                unionList.add(Schema.create(Schema.Type.BYTES));
                break;
            case BIGINT:
                unionList.add(Schema.create(Schema.Type.LONG));
                break;
            case SMALLINT:
            case INTEGER:
                unionList.add(Schema.create(Schema.Type.INT));
                break;
            case REAL:
                unionList.add(Schema.create(Schema.Type.FLOAT));
                break;
            case FLOAT8:
                unionList.add(Schema.create(Schema.Type.DOUBLE));
                break;
            default:
                unionList.add(Schema.create(Schema.Type.STRING));
                break;
        }

        return Schema.createUnion(unionList);
    }

}
