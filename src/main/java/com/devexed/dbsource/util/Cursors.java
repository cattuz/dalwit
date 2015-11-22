package com.devexed.dbsource.util;

import com.devexed.dbsource.DatabaseException;

/**
 * Cursor helpers.
 */
public final class Cursors {

    /**
     * Create a cursor with a single row, its columns accessible through a getter function.
     *
     * @param columnFunction The function which gets the column values.
     * @return A cursor with a single row.
     */
    public static CloseableCursor singleton(ColumnFunction columnFunction) {
        return new SingletonCursor(columnFunction);
    }

    public interface ColumnFunction {

        /**
         * Get the value for a specific column.
         */
        <E> E get(String column);

    }

    private static final class SingletonCursor extends AbstractCloseable implements CloseableCursor {

        private final ColumnFunction columnFunction;
        private boolean first = false;

        public SingletonCursor(ColumnFunction columnFunction) {
            this.columnFunction = columnFunction;
        }

        @Override
        public <T> T get(String column) {
            checkNotClosed();

            if (!first) throw new DatabaseException("Cursor points before first row");

            return columnFunction.get(column);
        }

        @Override
        public boolean seek(int rows) {
            checkNotClosed();

            if (rows == 0) return true;

            if (!first && rows == 1) {
                first = true;
                return true;
            }

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

    }

}
