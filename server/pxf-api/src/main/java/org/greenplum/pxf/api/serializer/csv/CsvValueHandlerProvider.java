package org.greenplum.pxf.api.serializer.csv;

import org.greenplum.pxf.api.io.DataType;
import org.greenplum.pxf.api.serializer.ValueHandler;
import org.greenplum.pxf.api.serializer.ValueHandlerProvider;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvValueHandlerProvider implements ValueHandlerProvider {

    private final Map<DataType, ValueHandler> valueHandlers;

    public CsvValueHandlerProvider() {
        StringCsvValueHandler stringCsvValueHandler = new StringCsvValueHandler();

        valueHandlers = new EnumMap<>(DataType.class);
        valueHandlers.put(DataType.BPCHAR, stringCsvValueHandler);
        valueHandlers.put(DataType.BYTEA, new ByteArrayCsvValueHandler());
        valueHandlers.put(DataType.DATE, new DateCsvValueHandler());
        valueHandlers.put(DataType.INT4ARRAY, new IntArrayCsvValueHandler());
        valueHandlers.put(DataType.TEXT, stringCsvValueHandler);
        valueHandlers.put(DataType.TIMESTAMP, new TimeStampCsvValueHandler());
        valueHandlers.put(DataType.VARCHAR, stringCsvValueHandler);
    }

    @Override
    public <T> ValueHandler<T> resolve(DataType dataType) {
        @SuppressWarnings("unchecked")
        ValueHandler<T> handler = valueHandlers.get(dataType);
        if (handler == null) {
            throw new IllegalArgumentException(String.format("DataType '%s' has not been registered", dataType));
        }
        return handler;
    }

    @Override
    public String toString() {
        String valueHandlersString = valueHandlers.values()
                .stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));

        return "CsvValueHandlerProvider{" +
                "valueHandlers=" + valueHandlersString +
                '}';
    }
}
