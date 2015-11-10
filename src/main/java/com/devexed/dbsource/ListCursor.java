package com.devexed.dbsource;

import com.devexed.dbsource.jdbc.JdbcAccessor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A cursor over a list of values. The cursor has a single column.
 */
public final class ListCursor extends AbstractCloseable implements Cursor {

	/** Cursor over an array of objects. For empty cursors use {@link EmptyCursor#of()} */
	public static Cursor of(Function<String, Class<?>> typeOfFunction, Object... objects) {
		return of(typeOfFunction, Arrays.asList(objects));
	}

	/** Cursor over a list of objects. For empty cursors use {@link EmptyCursor#of()} */
	public static Cursor of(Function<String, Class<?>> typeOfFunction, List<?> list) {
		return new ListCursor(typeOfFunction, list);
	}

    private final Function<String, Class<?>> typeOfFunction;
	private final List<?> results;
	private int row = -1;

	private ListCursor(Function<String, Class<?>> typeOfFunction, List<?> results) {
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
        Class<?> type = typeOfFunction.apply(column);

		if (type == null) throw new DatabaseException("No mapping to column " + column + ".");

		if (row < 0 || row > results.size())
			throw new DatabaseException("Row index " + row + " out of bounds [0|" + results.size() + "]");
		
		return (T) type.cast(results.get(row));
	}
	
}
