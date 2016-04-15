package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;

/**
 * Cursor helpers.
 */
public final class Cursors {

    /**
     * Create an empty cursor, with no rows or columns.
     *
     * @return The empty cursor.
     */
    public static Cursor empty() {
        return EmptyCursor.instance;
    }

    /**
     * Create a cursor with a single column and row.
     *
     * @param key The name of the column.
     * @param value The value for the cell.
     * @return A cursor with a single column and row.
     */
    public static Cursor singleton(String key, Object value) {
        return new SingletonCursor(key, value);
    }

    private static final class SingletonCursor extends AbstractCloseable implements Cursor {

        private final String key;
        private final Object value;
        private boolean first = false;

        @SuppressWarnings("unchecked")
        @Override
        public <T> T get(String column) {
            checkNotClosed();

            if (!first) throw new DatabaseException("Cursor points before first row");

            if (!column.equals(key)) throw new DatabaseException("Column name must be " + key);

            return (T) value;
        }

        public SingletonCursor(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean seek(int rows) {
            checkNotClosed();

            if (rows == 0) return true;

            if (!first && rows == 1) {
                first = true;
                return true;
            }

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

    }

    private static final class EmptyCursor extends AbstractCloseable implements Cursor {

        private static final EmptyCursor instance = new EmptyCursor();

        private EmptyCursor() {
        }

        @Override
        public boolean seek(int rows) {
            checkNotClosed();
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
            throw new DatabaseException("Illegal cursor position");
        }

    }

}
