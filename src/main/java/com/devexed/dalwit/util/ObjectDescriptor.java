package com.devexed.dalwit.util;

import com.devexed.dalwit.*;

import java.lang.reflect.*;
import java.util.*;

public final class ObjectDescriptor<T> {

    public static <T> ObjectDescriptor<T> of(Class<T> type) {
        return new ObjectDescriptor<>(type);
    }

    private final Constructor<T> constructor;
    private final HashMap<String, Class<?>> types;
    private final HashMap<String, Property> properties;

    private ObjectDescriptor(Class<T> type) {
        try {
            constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new DatabaseException(e);
        }

        types = new HashMap<>();
        properties = new HashMap<>();

        // Find public fields
        for (Field field : type.getDeclaredFields()) {
            int modifiers = field.getModifiers();

            if (!Modifier.isPublic(modifiers) || Modifier.isFinal(modifiers) || Modifier.isStatic(modifiers)) continue;

            types.put(field.getName(), field.getType());
            properties.put(field.getName(), new Property() {
                @Override
                public void set(Object instance, Object value) throws IllegalAccessException {
                    field.set(instance, value);
                }

                @Override
                public Object get(Object instance) throws IllegalAccessException {
                    return field.get(instance);
                }
            });
        }

        // Find public setters and getters
        for (Method setter : type.getDeclaredMethods()) {
            int modifiers = setter.getModifiers();

            if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) continue;

            String property = setter.getName().substring(4);

            if (property.startsWith("set") &&
                    property.length() > 3 &&
                    setter.getReturnType().equals(Void.TYPE) &&
                    setter.getParameterCount() == 1) {
                property = setter.getName().substring(4);
                property = property.substring(0, 1).toUpperCase() + property.substring(1);
                Method getter;

                try {
                    getter = type.getMethod("get" + property);
                } catch (NoSuchMethodException e) {
                    continue;
                }

                Class<?> propertyType = setter.getParameters()[0].getType();
                int getterModifiers = setter.getModifiers();

                if (!Modifier.isPublic(getterModifiers) || Modifier.isStatic(getterModifiers)) continue;

                if (getter.getReturnType().equals(propertyType)) {
                    setter.setAccessible(true);
                    types.put(property.toLowerCase(), propertyType);
                    properties.put(property.toLowerCase(), new Property() {
                        @Override
                        public void set(Object instance, Object value) throws InvocationTargetException, IllegalAccessException {
                            setter.invoke(instance, value);
                        }

                        @Override
                        public Object get(Object instance) throws InvocationTargetException, IllegalAccessException {
                            return getter.invoke(instance);
                        }
                    });
                }
            }
        }
    }

    public ObjectBinder<T> binder(ReadonlyStatement statement) {
        return new ObjectBinder<>(statement, properties);
    }

    public void bindAll(Statement statement, Iterable<T> objects) {
        ObjectBinder<T> binder = binder(statement);

        for (T object : objects) {
            binder.bind(object);
            statement.execute();
        }
    }

    public <E> ArrayList<E> bindAll(Statement statement, ObjectDescriptor<E> resultDescriptor, Iterable<T> objects) {
        ArrayList<E> result = new ArrayList<>();
        ObjectBinder<T> binder = binder(statement);

        for (T object : objects) {
            binder.bind(object);

            try (Cursor cursor = statement.insert()) {
                result.add(resultDescriptor.getter(cursor).get());
            }
        }

        return result;
    }

    public ObjectGetter<T> getter(Cursor cursor) {
        return new ObjectGetter<>(cursor, constructor, properties);
    }

    public ObjectIterable<T> iterate(Cursor cursor) {
        return new ObjectIterable<>(cursor, getter(cursor));
    }

    public Map<String, Class<?>> types() {
        return types;
    }

    interface Property {

        void set(Object instance, Object value) throws InvocationTargetException, IllegalAccessException;

        Object get(Object instance) throws InvocationTargetException, IllegalAccessException;

    }

}
