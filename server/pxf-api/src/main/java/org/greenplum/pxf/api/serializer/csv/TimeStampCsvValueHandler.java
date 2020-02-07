package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.GreenplumDateTime;
import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;

public class TimeStampCsvValueHandler extends BaseCsvValueHandler<Timestamp> {

    @Override
    protected void internalHandle(OutputStream buffer, Timestamp value) throws IOException {
        buffer.write(Utilities.getUtf8Bytes(value.toLocalDateTime().format(GreenplumDateTime.DATETIME_FORMATTER)));
    }
}