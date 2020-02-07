package org.greenplum.pxf.api.utilities;

import org.greenplum.pxf.api.examples.DemoProcessor;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.RequestContext;

public class ProcessorFactory extends BasePluginFactory<Processor> {

    private static final ProcessorFactory instance = new ProcessorFactory();

    /**
     * Returns a singleton instance of the factory.
     *
     * @return a singleton instance of the factory.
     */
    public static ProcessorFactory getInstance() {
        return instance;
    }

    @Override
    protected String getPluginClassName(RequestContext context) {
        // TODO: return the processor
        return DemoProcessor.class.getName();
    }
}