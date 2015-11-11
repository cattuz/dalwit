package com.devexed.dbsource;

/**
 * An empty cursor. The cursor has no columns or rows.
 */
public final class EmptyCursor extends AbstractCloseable implements DatabaseCursor {

	private static final EmptyCursor instance = new EmptyCursor();

	public static DatabaseCursor of() {
		return instance;
	}

	private EmptyCursor() {}

	@Override
	public boolean next() {
		checkNotClosed();
		return false;
	}

	@Override
	public <T> T get(String column) {
		checkNotClosed();
		throw new DatabaseException("Access of empty cursor.");
	}
	
}
