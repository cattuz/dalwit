package com.devexed.dalwit;

/**
 * <p>A cursor to a sequence of rows. Cursor instances always start off pointing before the first row, and as such
 * require a call to {@link #seek} or equivalent before any column data can be read. Iterating over a cursor typically
 * follows the following pattern:</p>
 * <pre><code>
 * Cursor cursor = null;
 * <p/>
 * try {
 *   cursor = statement.query(database);
 * <p/>
 *   while (cursor.next()) {
 *     String name = cursor.&lt;String&gt;get("name");
 *     int age = cursor.&lt;Integer&gt;get("age");
 *     SomeComplexType relationshipStatus = cursor.&lt;SomeComplexType&gt;get("relationship_status");
 *   }
 * } finally {
 *   statement.close(cursor);
 * }
 * </code></pre>
 * <p>Note that random access through {@link #seek} is up to the implementation to define and not guaranteed to be
 * available.</p>
 */
public interface Cursor {

    /**
     * Get the value of a column on the current row.
     *
     * @param column The name of the column.
     * @param <T>    The class of the column data.
     * @return The value of the column at the current row.
     * @throws DatabaseException If the cursor is closed or the column doesn't exist.
     */
    <T> T get(String column);

    /**
     * Seek relative to the current position in the cursor. A seek beyond the bounds of the cursor will return false and
     * close the cursor.
     *
     * @param rows The amount of rows to move relative to the current position.
     * @return True if the seek succeeded and the cursor has been moved to the specified row.
     * @throws DatabaseException If the cursor is closed.
     */
    boolean seek(int rows);

    /**
     * Identical to <code>seek(-1)</code>.
     *
     * @see #seek
     */
    boolean previous();

    /**
     * Identical to <code>seek(1)</code>.
     *
     * @see #seek
     */
    boolean next();

}
