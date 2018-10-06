package com.devexed.dalwit.util;

import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.ReadonlyStatement;
import com.devexed.dalwit.Statement;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public final class ObjectDescriptor<T> {

    public static <T> ObjectDescriptor<T> of(Class<T> type) {
        return new ObjectDescriptor<>(type);
    }

    private final Constructor<T> constructor;
    private final ArrayList<String> constructorParameters;
    private final LinkedHashMap<String, Class<?>> types;
    private final LinkedHashMap<String, Setter> setters;
    private final LinkedHashMap<String, Getter> getters;

    @SuppressWarnings("unchecked")
    private ObjectDescriptor(Class<T> type) {
        types = new LinkedHashMap<>();
        setters = new LinkedHashMap<>();
        getters = new LinkedHashMap<>();

        // Find public fields
        for (Field field : type.getDeclaredFields()) {
            int modifiers = field.getModifiers();

            if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) continue;

            types.put(field.getName().toLowerCase(), field.getType());
            getters.put(field.getName().toLowerCase(), field::get);
            if (!Modifier.isFinal(modifiers)) setters.put(field.getName().toLowerCase(), field::set);
        }

        // Find public setters and getters
        for (Method method : type.getDeclaredMethods()) {
            int modifiers = method.getModifiers();
            String methodName = method.getName();

            if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers) || methodName.length() <= 3) continue;

            String property = methodName.substring(3).toLowerCase();
            Class<?> propertyType;

            if (methodName.startsWith("set") && method.getReturnType().equals(Void.TYPE) && method.getParameterCount() == 1) {
                propertyType = method.getParameters()[0].getType();
                setters.put(property, method::invoke);
                method.setAccessible(true);
            } else if (methodName.startsWith("get") && !method.getReturnType().equals(Void.TYPE) && method.getParameterCount() == 0) {
                propertyType = method.getReturnType();
                getters.put(property, method::invoke);
                method.setAccessible(true);
            } else {
                continue;
            }

            Class<?> presentType = types.get(property);

            if (presentType == null) {
                types.put(property, propertyType);
            } else if (!presentType.equals(propertyType)) {
                throw new DatabaseException("Mismatched types for property " + property + ". Type " + propertyType + " does not match " + presentType);
            }
        }

        // Find constructor with greatest number of parameters
        Constructor<?> constructor = null;
        ArrayList<String> constructorParameters = null;

        for (Constructor<?> c : type.getDeclaredConstructors()) {
            Parameter[] parameters = c.getParameters();

            if (parameters.length <= types.size()) {
                int parameterIndex = 0;
                boolean typesMatch = true;
                ArrayList<String> cps = new ArrayList<>();

                for (Map.Entry<String, Class<?>> e : types.entrySet()) {
                    String parameterName = e.getKey();
                    Class<?> parameterType = e.getValue();

                    if (!parameterType.isAssignableFrom(parameters[parameterIndex].getType())) {
                        typesMatch = false;
                        break;
                    }

                    cps.add(parameterName);
                    parameterIndex++;

                    if (parameterIndex >= parameters.length) break;
                }

                if (typesMatch) {
                    c.setAccessible(true);
                    constructor = c;
                    constructorParameters = cps;
                }
            }
        }

        if (constructor == null) {
            throw new DatabaseException("No object constructor could be found for type " + type);
        }

        this.constructor = (Constructor<T>) constructor;
        this.constructorParameters = constructorParameters;
    }

    public ObjectBinder<T> binder(ReadonlyStatement statement) {
        return new ObjectBinder<>(statement, getters);
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
        return new ObjectGetter<>(cursor, constructor, constructorParameters, setters);
    }

    public ObjectIterable<T> iterate(Cursor cursor) {
        return new ObjectIterable<>(cursor, getter(cursor));
    }

    public Map<String, Class<?>> types() {
        return types;
    }

    interface Setter {

        void set(Object instance, Object value) throws InvocationTargetException, IllegalAccessException;

    }

    interface Getter {

        Object get(Object instance) throws InvocationTargetException, IllegalAccessException;

    }

}
