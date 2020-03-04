package org.greenplum.pxf.api.serializer;

import org.greenplum.pxf.api.io.DataType;

import java.io.IOException;
import java.io.OutputStream;

public class GpdbWritableSerializer extends BaseSerializer {

    @Override
    public void open(OutputStream out) {

    }

    @Override
    public void startRow(int numColumns) throws IOException {

    }

    @Override
    public void startField() throws IOException {

    }

    @Override
    public <T> void writeField(DataType valueHandler, T field) throws IOException {

    }

    @Override
    public void endField() throws IOException {

    }

    @Override
    public void endRow() throws IOException {

    }


    @Override
    public void close() {

    }
}
