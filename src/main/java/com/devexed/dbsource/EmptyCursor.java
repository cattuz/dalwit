package com.devexed.dbsource;

import com.devexed.dbsource.util.AbstractCloseable;
import com.devexed.dbsource.util.CloseableCursor;

/**
 * An empty cursor. The cursor has no columns or rows.
 */
public final class EmptyCursor extends AbstractCloseable implements CloseableCursor {

    private static final EmptyCursor instance = new EmptyCursor();

    private EmptyCursor() {
    }

    public static CloseableCursor of() {
        return instance;
    }

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
