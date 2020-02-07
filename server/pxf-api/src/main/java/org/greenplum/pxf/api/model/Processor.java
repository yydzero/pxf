package org.greenplum.pxf.api.model;

import org.greenplum.pxf.api.concurrent.TaskAwareBlockingQueue;

import javax.ws.rs.core.StreamingOutput;

public interface Processor<T> extends Plugin, StreamingOutput {

    /**
     * Register the output queue for processing the tuples
     *
     * @param outputQueue the task aware blocking output queue
     */
    void setOutputQueue(TaskAwareBlockingQueue<T> outputQueue);
}
