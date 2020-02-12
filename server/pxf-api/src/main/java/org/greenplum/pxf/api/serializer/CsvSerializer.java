package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BaseSerializer;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.serializer.csv.CsvValueHandlerProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

public class CsvSerializer extends BaseSerializer {

    private final GreenplumCSV greenplumCSV;
    private final ValueHandlerProvider valueHandlerProvider;
    private int currentField;
    private OutputStreamWriter writer;

    public CsvSerializer(GreenplumCSV greenplumCSV) {
        this.greenplumCSV = greenplumCSV;
        this.valueHandlerProvider = new CsvValueHandlerProvider();
    }

    @Override
    public void open(final OutputStream out) {
        writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
    }

    @Override
    public void startRow(int numColumns) {
        currentField = 0;
    }

    @Override
    public void startField() throws IOException {
        if (currentField > 0) {
            writer.write(greenplumCSV.getDelimiter());
        }
    }

    @Override
    public <T> void writeField(DataType dataType, T field) throws IOException {
        currentField++;
        if (field == null) {
            writer.write(greenplumCSV.getValueOfNull());
        } else {
            // TODO: best effort to resolve the field to the greenplum datatype
            if (DataType.isTextForm(dataType.getOID()) && !DataType.isArrayType(dataType.getOID()) && field instanceof String) {
                valueHandlerProvider.resolve(DataType.TEXT).handle(writer,
                        greenplumCSV.toCsvField((String) field, true, true, true));
            } else {
                valueHandlerProvider.resolve(dataType).handle(writer, field);
            }
        }
    }

    @Override
    public void endField() {
    }

    @Override
    public void endRow() throws IOException {
        writer.write(greenplumCSV.getNewline());
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}
