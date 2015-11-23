package com.devexed.dalwit.util;

import com.devexed.dalwit.DatabaseException;

/**
 * Base class to implement closeable objects.
 */
public class AbstractCloseable implements Closeable {

    private boolean closed = false;

    /**
     * @throws DatabaseException If this closable has been closed. Useful to ensure open state performing database
     *                           operations.
     */
    protected final void checkNotClosed() {
        if (isClosed()) throw new DatabaseException("Closed");
    }

    /**
     * @return True if this closable is closed. Can be overridden to provide additional closed criteria, enforced by the
     * {@link #checkNotClosed} method.
     */
    protected boolean isClosed() {
        return closed;
    }

    /**
     * Close this closable.
     */
    @Override
    public void close() {
        closed = true;
    }

}
