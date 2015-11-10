package com.devexed.dbsource;

import com.devexed.dbsource.DatabaseException;

import java.io.Closeable;

/**
 * Base class to implement closeable objects.
 */
public abstract class AbstractCloseable implements Closeable {
	
	private boolean closed = false;
	
	protected void checkNotClosed() {
		if (closed) throw new DatabaseException("Closed");
	}

	@Override
	public void close() {
		checkNotClosed();
		closed = true;
	}
	
}
