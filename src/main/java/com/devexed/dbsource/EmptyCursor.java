package com.devexed.dbsource;

/**
 * An empty cursor. The cursor has no columns or rows.
 */
public final class EmptyCursor extends AbstractCloseable implements Cursor {

	private static final EmptyCursor instance = new EmptyCursor();

	public static Cursor of() {
		return instance;
	}

	private EmptyCursor() {}

    @Override
    public boolean seek(int rows) {
        checkNotClosed();
        close();
        return false;
    }

    @Override
    public boolean previous() {
        return seek(-1);
    }

    @Override
	public boolean next() {
		return seek(1);
	}

	@Override
	public <T> T get(String column) {
		checkNotClosed();
		throw new DatabaseException("Illegal cursor position.");
	}
	
}
