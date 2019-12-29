package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.service.BridgeOutputBuilder;

import java.io.DataInputStream;
import java.util.Iterator;

public class StreamingImageReadBridge extends BaseBridge {
    final BridgeOutputBuilder outputBuilder;
    Iterator<Writable> writableIterator;

    public StreamingImageReadBridge(RequestContext context) {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance());
    }

    StreamingImageReadBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
        super(context, accessorFactory, resolverFactory);
        outputBuilder = new BridgeOutputBuilder(context);
    }

    @Override
    public boolean beginIteration() throws Exception {
        return accessor.openForRead();
    }

    @Override
    public Writable getNext() throws Exception {
        if (writableIterator == null) {
            writableIterator = outputBuilder.makeStreamingOutput(resolver.getFields(accessor.readNextObject()));
        } else if (!writableIterator.hasNext()) {
            writableIterator = null;
            return null;
        }

        return writableIterator.next();
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void endIteration() throws Exception {
        try {
            accessor.closeForRead();
        } catch (Exception e) {
            LOG.error("Failed to close bridge resources: {}", e.getMessage());
            throw e;
        }
    }

    @Override
    public boolean setNext(DataInputStream inputStream) {
        throw new UnsupportedOperationException("setNext is not implemented");
    }
}
