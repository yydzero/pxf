package org.greenplum.pxf.api.concurrent;

import java.util.Deque;
import java.util.concurrent.BlockingDeque;

/**
 * A {@link Deque} that is aware of the number of tasks being processed for the
 * queue. Additionally supports blocking operations that wait for the deque to
 * become non-empty when retrieving an element, and wait for space to become
 * available in the deque when storing an element.
 */
public interface TaskAwareBlockingQueue<T> extends BlockingDeque<T> {

    /**
     * Returns true if pending tasks are registered to the queue, false otherwise
     *
     * @return true if pending tasks are registered to the queue, false otherwise
     */
    boolean hasPendingTasks();

    /**
     * Returns the number of active tasks registered to this queue
     *
     * @return the number of active tasks registered to this queue
     */
    int activeTaskCount();

    /**
     * Registers a task to the queue
     */
    void registerTask();

    /**
     * De-registers a task from the queue
     */
    void deregisterTask();
}
