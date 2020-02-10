package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.model.BaseSerializer;
import org.greenplum.pxf.api.model.GreenplumCSV;
import org.greenplum.pxf.api.serializer.csv.CsvValueHandlerProvider;
import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;

public class CsvSerializer extends BaseSerializer {

    private final GreenplumCSV greenplumCSV;
    private final ValueHandlerProvider valueHandlerProvider;
    private int currentField;

    public CsvSerializer(GreenplumCSV greenplumCSV) {
        this(greenplumCSV, 256 * OneKB);
    }

    public CsvSerializer(GreenplumCSV greenplumCSV, int bufferSize) {
        super(bufferSize);
        this.greenplumCSV = greenplumCSV;
        this.valueHandlerProvider = new CsvValueHandlerProvider();
    }

    @Override
    public void startRow(int numColumns) {
        currentField = 0;
    }

    @Override
    public void startField() throws IOException {
        if (currentField > 0) {
            buffer.write(greenplumCSV.getDelimiter());
        }
    }

    @Override
    public <T> void writeField(DataType dataType, T field) throws IOException {
        currentField++;
        if (field == null) {
            buffer.write(Utilities.getUtf8Bytes(greenplumCSV.getValueOfNull()));
        } else {
            // TODO: best effort to resolve the field to the greenplum datatype
            if (DataType.isTextForm(dataType.getOID()) && !DataType.isArrayType(dataType.getOID()) && field instanceof String) {
                valueHandlerProvider.resolve(DataType.TEXT).handle(buffer,
                        greenplumCSV.toCsvField((String) field, true, true, true));
            } else {
                valueHandlerProvider.resolve(dataType).handle(buffer, field);
            }
        }
    }

    @Override
    public void endField() {
    }

    @Override
    public void endRow() throws IOException {
        buffer.write(Utilities.getUtf8Bytes(greenplumCSV.getNewline()));
    }
}
