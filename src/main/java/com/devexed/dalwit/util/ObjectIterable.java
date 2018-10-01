package com.devexed.dalwit.util;

import com.devexed.dalwit.Closeable;
import com.devexed.dalwit.Cursor;

import java.util.Iterator;

public final class ObjectIterable<T> implements Iterable<T>, Closeable {

    private final Cursor cursor;
    private final ObjectGetter<T> getter;

    ObjectIterable(Cursor cursor, ObjectGetter<T> getter) {
        this.cursor = cursor;
        this.getter = getter;
    }

    @Override
    public void close() {
        cursor.close();
    }

    @Override
    public Iterator<T> iterator() {
        return new ObjectIterator<>(cursor, getter);
    }

}
