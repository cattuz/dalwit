package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public final class ObjectGetter<T> {

    private final Constructor<T> constructor;
    private final Accessor[] accessors;

    ObjectGetter(Cursor cursor, Constructor<T> constructor, HashMap<String, ObjectDescriptor.Property> properties) {
        this.constructor = constructor;
        accessors = new Accessor[properties.size()];
        int i = 0;

        for (Map.Entry<String, ObjectDescriptor.Property> property : properties.entrySet()) {
            String column = property.getKey();
            accessors[i] = new Accessor(property.getValue(), cursor.getter(column));
            i++;
        }
    }

    public T get(T instance) {
        try {
            for (Accessor accessor : accessors) {
                accessor.property.set(instance, accessor.getter.get());
            }

            return instance;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DatabaseException(e);
        }
    }

    public T get() {
        T instance;

        try {
            instance = constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new DatabaseException(e);
        }

        return get(instance);
    }

    private static class Accessor {

        private final ObjectDescriptor.Property property;
        private final Cursor.Getter<Object> getter;

        private Accessor(ObjectDescriptor.Property property, Cursor.Getter<Object> getter) {
            this.property = property;
            this.getter = getter;
        }

    }

}
