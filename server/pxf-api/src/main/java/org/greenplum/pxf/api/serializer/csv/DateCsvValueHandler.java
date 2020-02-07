package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.utilities.Utilities;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Date;

public class DateCsvValueHandler extends BaseCsvValueHandler<Date> {

    @Override
    protected void internalHandle(OutputStream buffer, Date value) throws IOException {
        buffer.write(Utilities.getUtf8Bytes(value.toString()));
    }
}
