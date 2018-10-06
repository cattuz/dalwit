package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ObjectGetter<T> {

    private final Constructor<T> constructor;
    private final Cursor.Getter<?>[] constructorGetters;
    private final Accessor[] accessors;

    ObjectGetter(Cursor cursor, Constructor<T> constructor, ArrayList<String> constructorParameters, LinkedHashMap<String, ObjectDescriptor.Setter> properties) {
        this.constructor = constructor;
        constructorGetters = new Cursor.Getter[constructorParameters.size()];
        int parameterIndex = 0;

        for (String column : constructorParameters) {
            constructorGetters[parameterIndex] = cursor.getter(column);
            parameterIndex++;
        }

        accessors = new Accessor[properties.size()];
        int accessorIndex = 0;

        for (Map.Entry<String, ObjectDescriptor.Setter> property : properties.entrySet()) {
            String column = property.getKey();
            accessors[accessorIndex] = new Accessor(property.getValue(), cursor.getter(column));
            accessorIndex++;
        }
    }

    public T get() {
        T instance;
        Object[] constructorParameters = new Object[constructorGetters.length];

        for (int i = 0; i < constructorGetters.length; i++) {
            constructorParameters[i] = constructorGetters[i].get();
        }

        try {
            instance = constructor.newInstance(constructorParameters);

            for (Accessor accessor : accessors) {
                accessor.property.set(instance, accessor.getter.get());
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new DatabaseException(e);
        }

        return instance;
    }

    private static class Accessor {

        private final ObjectDescriptor.Setter property;
        private final Cursor.Getter<Object> getter;

        private Accessor(ObjectDescriptor.Setter property, Cursor.Getter<Object> getter) {
            this.property = property;
            this.getter = getter;
        }

    }

}
