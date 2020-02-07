package org.greenplum.pxf.api.concurrent;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskAwareLinkedBlockingDeque<T> extends LinkedBlockingDeque<T> implements TaskAwareBlockingQueue<T> {

    private final AtomicInteger activeTasks;

    public TaskAwareLinkedBlockingDeque() {
        activeTasks = new AtomicInteger();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasPendingTasks() {
        return activeTasks.get() > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int activeTaskCount() {
        return activeTasks.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerTask() {
        activeTasks.incrementAndGet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deregisterTask() {
        activeTasks.decrementAndGet();
    }
}
