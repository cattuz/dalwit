package com.devexed.dbsource.util;

import com.devexed.dbsource.Cursor;

/**
 * FIXME Document!
 */
public interface CloseableCursor extends Cursor, Closeable {

    @Override
    void close();

}
