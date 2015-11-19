package com.devexed.dbsource;

import java.util.Map;

/**
 * Cursor helpers.
 */
public final class Cursors {

	public interface ColumnFunction {

        /** Get the value for a specific column. */
		<E> E get(String column);

	}

	private static final class SingletonCursor extends AbstractCloseable implements Cursor {

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

    public static Cursor singleton(ColumnFunction columnFunction) {
        return new SingletonCursor(columnFunction);
    }

    public static <E> Cursor singleton(final Map<String, E> values) {
        return new SingletonCursor(new ColumnFunction() {

            @Override
            @SuppressWarnings("unchecked")
            public <T> T get(String column) {
                if (!values.containsKey(column))
                    throw new DatabaseException("No such column " + column + " among " + values.keySet());

                return (T) values.get(column);
            }

        });
    }

}
