package com.devexed.dalwit.util;

import com.devexed.dalwit.Closeable;
import com.devexed.dalwit.Cursor;

/**
 * FIXME Document!
 */
public class ClosingCursor implements Cursor {

    private final Closeable parent;
    private final Cursor cursor;

    ClosingCursor(Closeable parent, Cursor cursor) {
        this.parent = parent;
        this.cursor = cursor;
    }

    @Override
    public <T> T get(String column) {
        return cursor.get(column);
    }

    @Override
    public boolean seek(int rows) {
        return cursor.seek(rows);
    }

    @Override
    public boolean previous() {
        return cursor.previous();
    }

    @Override
    public boolean next() {
        return cursor.next();
    }

    @Override
    public void close() {
        cursor.close();
        parent.close();
    }

}
