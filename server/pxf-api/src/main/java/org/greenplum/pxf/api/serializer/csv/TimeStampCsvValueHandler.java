package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.GreenplumDateTime;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;

public class TimeStampCsvValueHandler extends BaseCsvValueHandler<Timestamp> {

    @Override
    protected void internalHandle(OutputStream buffer, Timestamp value) throws IOException {
        buffer.write(value.toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER).getBytes(StandardCharsets.UTF_8));
    }
}