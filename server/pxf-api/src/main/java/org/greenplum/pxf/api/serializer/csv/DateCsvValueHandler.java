package org.greenplum.pxf.api.serializer.csv;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Date;

public class DateCsvValueHandler extends BaseCsvValueHandler<Date> {

    @Override
    protected void internalHandle(OutputStreamWriter writer, Date value) throws IOException {
        writer.write(value.toString());
    }
}
