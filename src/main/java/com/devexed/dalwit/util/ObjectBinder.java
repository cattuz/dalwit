package com.devexed.dalwit.util;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Statement;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public final class ObjectBinder<T> {

    private final Constructor<T> constructor;
    private final Accessor[] accessors;

    ObjectBinder(Statement statement, Constructor<T> constructor, HashMap<String, ObjectDescriptor.Property> properties) {
        this.constructor = constructor;
        accessors = new Accessor[properties.size()];
        int i = 0;

        for (Map.Entry<String, ObjectDescriptor.Property> property : properties.entrySet()) {
            String column = property.getKey();
            accessors[i] = new Accessor(property.getValue(), statement.binder(column));
            i++;
        }
    }

    public void bind(T instance) {
        try {
            for (Accessor accessor : accessors) {
                accessor.binder.bind(accessor.property.get(instance));
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DatabaseException(e);
        }
    }

    private static class Accessor {

        private final ObjectDescriptor.Property property;
        private final Statement.Binder<Object> binder;

        private Accessor(ObjectDescriptor.Property property, Statement.Binder<Object> binder) {
            this.property = property;
            this.binder = binder;
        }

    }

}
