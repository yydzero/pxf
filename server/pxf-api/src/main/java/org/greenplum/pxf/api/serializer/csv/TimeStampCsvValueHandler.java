package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.GreenplumDateTime;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Timestamp;

public class TimeStampCsvValueHandler extends BaseCsvValueHandler<Timestamp> {

    @Override
    protected void internalHandle(OutputStreamWriter writer, Timestamp value) throws IOException {
        writer.write(value.toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER));
    }
}