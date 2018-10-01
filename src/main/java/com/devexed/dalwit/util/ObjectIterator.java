package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;

import java.util.Iterator;

public final class ObjectIterator<T> implements Iterator<T> {

    private final Cursor cursor;
    private final ObjectGetter<T> getter;
    private boolean hasNext;

    ObjectIterator(Cursor cursor, ObjectGetter<T> getter) {
        this.cursor = cursor;
        this.getter = getter;
        hasNext = cursor.next();
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public T next() {
        T instance = getter.get();
        hasNext = cursor.next();

        return instance;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
