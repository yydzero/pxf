package org.greenplum.pxf.api.utilities;

import org.greenplum.pxf.api.model.OutputFormat;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.model.Serializer;
import org.greenplum.pxf.api.serializer.BinarySerializer;
import org.greenplum.pxf.api.serializer.CsvSerializer;

public class SerializerFactory {

    private static final SerializerFactory instance = new SerializerFactory();

    /**
     * Returns a singleton instance of the factory.
     *
     * @return a singleton instance of the factory.
     */
    public static SerializerFactory getInstance() {
        return instance;
    }


    public Serializer getSerializer(RequestContext context) {

        if (context.getOutputFormat() == OutputFormat.TEXT) {
            return new CsvSerializer(context.getGreenplumCSV());
        } else if (context.getOutputFormat() == OutputFormat.Binary) {
            return new BinarySerializer();
        }

        throw new UnsupportedOperationException("The output format is not supported");
    }
}
