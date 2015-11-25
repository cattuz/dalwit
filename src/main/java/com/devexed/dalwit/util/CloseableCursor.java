package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;

/**
 * A cursor with a close method that closes the cursor using its parent.
 */
public interface CloseableCursor extends Cursor, Closeable {

    @Override
    void close();

}
