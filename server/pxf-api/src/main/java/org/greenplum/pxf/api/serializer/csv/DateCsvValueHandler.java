package org.greenplum.pxf.api.serializer.csv;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;

public class DateCsvValueHandler extends BaseCsvValueHandler<Date> {

    @Override
    protected void internalHandle(DataOutputStream buffer, Date value) throws IOException {
        buffer.writeChars(value.toString());
    }
}
