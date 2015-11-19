package com.devexed.dbsource;

import java.io.Closeable;

/**
 * <p>A cursor with rows accessed sequentially through iteration. Cursor instances always start off pointing before the
 * first row, and as such require a call to {@link #next} before any column data can be read. Iterating over a cursor
 * typically follows the following pattern:</p>
 * <p/>
 * <pre><code>
 * Cursor cursor = ...;
 * <p/>
 * try {
 *   while (cursor.next()) {
 *     String name = cursor.&lt;String&gt;get("name");
 *     int age = cursor.&lt;Integer&gt;get("age");
 *     SomeComplexType relationshipStatus = cursor.&lt;SomeComplexType&gt;get("relationship_status");
 *   }
 * } finally {
 *   cursor.close();
 * }
 * </code></pre>
 */
public interface Cursor extends Closeable {

    <T> T get(String column);

    /**
     * Close the cursor, after which it is unusable.
     */
    void close();

    /**
     * Seek relative to the current position in the cursor. A seek beyond the bound of the cursor will return false and
     * close the cursor, making it unusable.
     *
     * @param rows The amount of rows to move relative to the current position.
     * @return True if the seek succeeded and the cursor has been moved to the specified row.
     */
    boolean seek(int rows);

    /**
     * Identical to seek(-1);
     *
     * @see #seek
     */
    boolean previous();

    /**
     * Identical to seek(1);
     *
     * @see #seek
     */
    boolean next();

}
