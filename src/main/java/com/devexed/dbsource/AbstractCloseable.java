package com.devexed.dbsource;

import java.io.Closeable;

/**
 * Base class to implement closeable objects.
 */
public abstract class AbstractCloseable implements Closeable {
	
	private boolean closed = false;

	/**
	 * @throws DatabaseException If this closable has been closed. Useful to ensure open state performing database
     * operations.
	 */
	protected final void checkNotClosed() {
		if (isClosed()) throw new DatabaseException("Closed");
	}

	/**
	 * @return True if this closable is closed. Can be overridden to provide additional closed criteria, enforced by the
	 * 		   {@link #checkNotClosed} method.
	 */
	protected boolean isClosed() {
		return closed;
	}

	/**
	 * Close this closable. A no-op if this closable has already been closed.
	 */
	@Override
	public void close() {
        if (isClosed()) return;
		closed = true;
	}
	
}
