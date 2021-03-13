package com.devexed.dalwit;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Utility class for making implementation of list parameters easier.
 *
 * @param <T>
 */
public final class ListBinder<T> implements ReadonlyStatement.Binder<T> {

    private static final String countMismatchError = "List value must have exactly %d elements to match the declared parameter type";

    private final ArrayList<ReadonlyStatement.Binder<Object>> binders;

    public ListBinder(ArrayList<ReadonlyStatement.Binder<Object>> binders) {
        this.binders = binders;
    }

    @Override
    public void bind(T value) {
        if (value == null) return;

        Class<?> valueType = value.getClass();

        if (Collection.class.isAssignableFrom(valueType)) {
            Collection values = (Collection) value;

            if (values.size() != binders.size()) {
                throw new DatabaseException(String.format(countMismatchError, values.size()));
            }

            int i = 0;

            for (Object object : values) {
                binders.get(i).bind(object);
                i++;
            }
        } else if (valueType.isArray()) {
            int length = Array.getLength(value);

            if (length != binders.size()) {
                throw new DatabaseException(String.format(countMismatchError, length));
            }

            for (int i = 0; i < length; i++) {
                binders.get(i).bind(Array.get(value, i));
            }
        } else {
            throw new DatabaseException("Expected iterable type, was " + valueType);
        }
    }

}
