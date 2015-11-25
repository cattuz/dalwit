package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;

public abstract class AbstractCloseableCursor implements CloseableCursor {

    private final Cursor cursor;

    public AbstractCloseableCursor(Cursor cursor) {
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

}
