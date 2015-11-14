package com.devexed.dbsource;

/**
 * A cursor over an arbitrary data, provided by a getter interface. Useful for mocking database cursor data.
 */
public final class MockCursor extends AbstractCloseable implements Cursor {

	public interface Getter {

        /** Does a next row exists after index? */
		boolean next(int index);

        /** Get the value for a specific column at a specific row. */
		<E> E get(int index, String column);

	}

    private final Getter getter;
	private int row = -1;
    private boolean hasNext;

	public MockCursor(Getter getter) {
		this.getter = getter;
	}
	
	@Override
	public boolean next() {
		checkNotClosed();

        hasNext = getter.next(row);
		if (hasNext) row++;

		return hasNext;
	}

    @Override
	public <E> E get(String column) {
		checkNotClosed();

		if (row < 0 || !hasNext)
			throw new DatabaseException("Row index " + row + " out of bounds [0|" + (row + 1) + "]");
		
		return getter.get(row, column);
	}

}
