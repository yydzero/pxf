package org.greenplum.pxf.service.bridge;

import org.greenplum.pxf.api.OneRow;
import org.greenplum.pxf.api.io.BufferWritable;
import org.greenplum.pxf.api.io.Writable;
import org.greenplum.pxf.api.model.BatchResolver;
import org.greenplum.pxf.api.model.RequestContext;
import org.greenplum.pxf.api.utilities.AccessorFactory;
import org.greenplum.pxf.api.utilities.ResolverFactory;
import org.greenplum.pxf.service.BridgeOutputBuilder;

import java.io.DataInputStream;
import java.util.Deque;
import java.util.LinkedList;

public class BatchedImageReadBridge extends BaseBridge {
    final BridgeOutputBuilder outputBuilder;
    private OneRow row;
    private boolean rowDepleted;

    Deque<Writable> outputQueue = new LinkedList<>();

    public BatchedImageReadBridge(RequestContext context) {
        this(context, AccessorFactory.getInstance(), ResolverFactory.getInstance());
    }

    BatchedImageReadBridge(RequestContext context, AccessorFactory accessorFactory, ResolverFactory resolverFactory) {
        super(context, accessorFactory, resolverFactory);
        rowDepleted = true;
        outputBuilder = new BridgeOutputBuilder(context);
    }

    @Override
    public boolean beginIteration() throws Exception {
        return accessor.openForRead();
    }

    @Override
    public Writable getNext() throws Exception {
        if (!outputQueue.isEmpty()) {
            return outputQueue.pop();
        }

        while (outputQueue.isEmpty()) {
            if (rowDepleted) {
                rowDepleted = false;
                row = accessor.readNextObject();
                outputQueue = outputBuilder.makeOutput(((BatchResolver) resolver).startBatch(row));
            }
            byte[] bytes = ((BatchResolver) resolver).getNextBatchedItem(row);

            if (bytes != null) {
                outputQueue.add(new BufferWritable(bytes));
            } else {
                rowDepleted = true;
                outputQueue.add(null);
            }
        }
        if (!outputQueue.isEmpty()) {
            return outputQueue.pop();
        }
        return null;
    }

    @Override
    public boolean isThreadSafe() {
        return false;
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
