package com.devexed.dbsource;

import java.util.Arrays;
import java.util.List;

/**
 * A cursor over a list of values. The cursor has a single column.
 */
public final class ListCursor extends AbstractCloseable implements Cursor {

	public interface TypeFunction {

		Class<?> typeOf(String column);

	}

	/** Cursor over an array of objects. For empty cursors use {@link EmptyCursor#of()} */
	public static Cursor of(TypeFunction typeOfFunction, Object... objects) {
		return of(typeOfFunction, Arrays.asList(objects));
	}

	/** Cursor over a list of objects. For empty cursors use {@link EmptyCursor#of()} */
	public static Cursor of(TypeFunction typeOfFunction, List<?> list) {
		return new ListCursor(typeOfFunction, list);
	}

    private final TypeFunction typeOfFunction;
	private final List<?> results;
	private int row = -1;

	private ListCursor(TypeFunction typeOfFunction, List<?> results) {
		this.typeOfFunction = typeOfFunction;
		this.results = results;
	}
	
	@Override
	public boolean next() {
		checkNotClosed();

		row++;
		return row < results.size();
	}

    @Override
    @SuppressWarnings("unchecked")
	public <T> T get(String column) {
		checkNotClosed();
        Class<?> type = typeOfFunction.typeOf(column);

		if (type == null) throw new DatabaseException("No mapping to column " + column + ".");

		if (row < 0 || row > results.size())
			throw new DatabaseException("Row index " + row + " out of bounds [0|" + results.size() + "]");
		
		return (T) type.cast(results.get(row));
	}
	
}
