package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;

/**
 * FIXME Document!
 */
public interface CloseableCursor extends Cursor, Closeable {

    @Override
    void close();

}
