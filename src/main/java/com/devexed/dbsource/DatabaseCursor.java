package com.devexed.dbsource;

import java.io.Closeable;

/**
 * <p>A cursor with rows accessed sequentially through iteration. Cursor instances always start off pointing before the
 * first row, and as such require a call to {@link #next} before any column data can be read. Iterating over a cursor
 * typically follows the following pattern:</p>
 *
 * <pre><code>
 * Cursor cursor = ...;
 *
 * try {
 *   while (cursor.next()) {
 *     String a = cursor.get(0, String.class);
 *     String b = cursor.get(1, Integer.class);
 *     SomeComplexType c = cursor.get(2, SomeComplexType.class);
 *   }
 * } finally {
 *   cursor.close();
 * }
 * </code></pre>
 */
public interface DatabaseCursor extends Closeable {

	<T> T get(String column);

	/**
	 * Close the cursor, after which it is unusable.
	 */
	void close();

	/**
	 * @return True if the cursor has a next result.
	 */
	boolean next();
	
}
