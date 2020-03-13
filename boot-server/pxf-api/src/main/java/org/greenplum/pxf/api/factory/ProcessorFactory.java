package org.greenplum.pxf.api.factory;

import org.greenplum.pxf.api.examples.DemoProcessor;
import org.greenplum.pxf.api.model.Processor;
import org.greenplum.pxf.api.model.RequestContext;
import org.springframework.stereotype.Component;

@Component
public class ProcessorFactory extends BasePluginFactory<Processor> {

    @Override
    protected String getPluginClassName(RequestContext context) {
        // TODO: return the processor
        return DemoProcessor.class.getName();
    }
}