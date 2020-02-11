package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Date;

public class DateCsvValueHandler extends BaseCsvValueHandler<Date> {

    @Override
    protected void internalHandle(OutputStream buffer, Date value) throws IOException {
        buffer.write(value.toString().getBytes(StandardCharsets.UTF_8));
    }
}
