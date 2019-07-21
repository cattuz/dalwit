package com.devexed.dalwit.util;

import com.devexed.dalwit.*;

import java.lang.reflect.*;
import java.util.*;

public final class ObjectDescriptor<T> {

    public static <T> ObjectDescriptor<T> of(Class<T> type, String table, ObjectColumnMapper mapper) {
        return new ObjectDescriptor<>(type, table, mapper);
    }

    public static <T> ObjectDescriptor<T> of(Class<T> type, String table) {
        return of(type, table, new DefaultObjectColumnMapper());
    }

    public static <T> ObjectDescriptor<T> of(Class<T> type) {
        return of(type, type.getSimpleName().toLowerCase());
    }

    private final String table;
    private final Constructor<T> constructor;
    private final ArrayList<String> constructorParameters;
    private final LinkedHashMap<String, Class<?>> columns;
    private final LinkedHashMap<String, Class<?>> parameters;
    private final LinkedHashMap<String, Setter> setters;
    private final LinkedHashMap<String, Getter> getters;

    @SuppressWarnings("unchecked")
    private ObjectDescriptor(Class<T> type, String table, ObjectColumnMapper mapper) {
        this.table = table;
        columns = new LinkedHashMap<>();
        parameters = new LinkedHashMap<>();
        setters = new LinkedHashMap<>();
        getters = new LinkedHashMap<>();

        // Find public fields
        for (Field field : type.getDeclaredFields()) {
            int modifiers = field.getModifiers();

            if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers)) continue;

            columns.put(mapper.apply(field.getName()).toLowerCase(), field.getType());
            parameters.put(mapper.apply(field.getName()).toLowerCase(), field.getType());
            getters.put(mapper.apply(field.getName()).toLowerCase(), field::get);
            if (!Modifier.isFinal(modifiers)) setters.put(mapper.apply(field.getName()).toLowerCase(), field::set);
        }

        // Find public setters and getters
        for (Method method : type.getDeclaredMethods()) {
            int modifiers = method.getModifiers();
            String methodName = method.getName();

            if (!Modifier.isPublic(modifiers) || Modifier.isStatic(modifiers) || methodName.length() <= 3) continue;

            String property = methodName.substring(3).toLowerCase();
            Class<?> propertyType;

            if (methodName.startsWith("set") && method.getReturnType().equals(Void.TYPE) && method.getParameterTypes().length == 1) {
                propertyType = method.getParameterTypes()[0];
                setters.put(property, method::invoke);
                method.setAccessible(true);
            } else if (methodName.startsWith("get") && !method.getReturnType().equals(Void.TYPE) && method.getParameterTypes().length == 0) {
                propertyType = method.getReturnType();
                getters.put(property, method::invoke);
                method.setAccessible(true);
            } else {
                continue;
            }

            Class<?> presentType = parameters.get(property);

            if (presentType == null) {
                columns.put(property, propertyType);
                parameters.put(property, propertyType);
            } else if (!presentType.equals(propertyType)) {
                throw new DatabaseException("Mismatched types for property " + property + ". Type " + propertyType + " does not match " + presentType);
            }
        }

        // Find constructor with greatest number of parameters
        Constructor<?> constructor = null;
        ArrayList<String> constructorParameters = null;

        for (Constructor<?> c : type.getDeclaredConstructors()) {
            Class<?>[] parameters = c.getParameterTypes();

            if (parameters.length <= this.parameters.size()) {
                int parameterIndex = 0;
                boolean typesMatch = true;
                ArrayList<String> cps = new ArrayList<>();

                for (Map.Entry<String, Class<?>> e : this.parameters.entrySet()) {
                    if (parameterIndex >= parameters.length) break;

                    String parameterName = e.getKey();
                    Class<?> parameterType = e.getValue();

                    if (!parameterType.isAssignableFrom(parameters[parameterIndex])) {
                        typesMatch = false;
                        break;
                    }

                    cps.add(parameterName);
                    parameterIndex++;
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

    public ObjectBinder<T> binder(ReadonlyStatement statement, Collection<String> columns) {
        LinkedHashMap<String, Getter> columnGetters = new LinkedHashMap<>(getters);
        columnGetters.keySet().retainAll(columns);

        return new ObjectBinder<>(statement, columnGetters);
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

    /**
     * Start building a select query with the column part pre-filled with the object properties.
     * @param sqlPart SQL after the FROM part. E.g. "WHERE id=0"
     * @return A query builder
     */
    public Query.QueryBuilder select(String sqlPart, Set<String> columns) {
        StringBuilder sqlBuilder = new StringBuilder();
        Iterator<String> typeIterator = columns.iterator();
        sqlBuilder.append("SELECT \"").append(typeIterator.next()).append("\"");

        while (typeIterator.hasNext()) sqlBuilder.append(",\"").append(typeIterator.next()).append("\"");

        sqlBuilder.append(" FROM \"").append(table).append("\" ").append(sqlPart);

        return Query.builder(sqlBuilder.toString()).columns(this.columns);
    }

    /**
     * @see #select(String, Set)
     */
    public Query.QueryBuilder select(String sqlPart) {
        return select(sqlPart, this.columns.keySet());
    }

    /**
     * @see #select(String)
     */
    public Query.QueryBuilder select() {
        return select("");
    }

    /**
     * Build an insert query like "INSERT x INTO t VALUES (:x) for the object properties.
     * @return A query builder
     */
    public Query.QueryBuilder insert(Set<String> parameters) {
        StringBuilder sqlBuilder = new StringBuilder();
        Iterator<String> columnIterator = parameters.iterator();
        sqlBuilder.append("INSERT INTO \"").append(table).append("\" (\"").append(columnIterator.next()).append("\"");

        while (columnIterator.hasNext()) sqlBuilder.append(",\"").append(columnIterator.next()).append("\"");

        sqlBuilder.append(") VALUES (");
        Iterator<String> parameterIterator = this.parameters.keySet().iterator();
        sqlBuilder.append(":").append(parameterIterator.next());

        while (parameterIterator.hasNext()) sqlBuilder.append(",:").append(parameterIterator.next());

        sqlBuilder.append(")");

        return Query.builder(sqlBuilder.toString()).parameters(this.parameters);
    }

    /**
     * @see #insert(Set)
     */
    public Query.QueryBuilder insert() {
        return insert(parameters.keySet());
    }

    /**
     * Build an update query like "UPDATE t SET x = :x".
     * @param sqlPart The part after the SET list. E.g. "WHERE id = 0"
     * @return A query builder
     */
    public Query.QueryBuilder update(String sqlPart, Set<String> parameters) {
        StringBuilder sqlBuilder = new StringBuilder();
        Iterator<String> parameterIterator = parameters.iterator();
        String firstParameter = parameterIterator.next();
        sqlBuilder.append("UPDATE \"").append(table)
                .append("\" SET \"")
                .append(firstParameter).append("\"")
                .append(" = :").append(firstParameter);

        while (parameterIterator.hasNext()) {
            String parameter = parameterIterator.next();
            sqlBuilder
                    .append(",\"").append(parameter).append("\"")
                    .append(" = :").append(parameter);
        }

        sqlBuilder.append(sqlPart);

        return Query.builder(sqlBuilder.toString()).parameters(this.parameters);
    }

    /**
     * @see #update(String, Set)
     */
    public Query.QueryBuilder update(String sqlPart) {
        return update(sqlPart, parameters.keySet());
    }

    public Map<String, Class<?>> columns() {
        return columns;
    }

    public Map<String, Class<?>> parameters() {
        return parameters;
    }

    interface Setter {

        void set(Object instance, Object value) throws InvocationTargetException, IllegalAccessException;

    }

    interface Getter {

        Object get(Object instance) throws InvocationTargetException, IllegalAccessException;

    }

}
