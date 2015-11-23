package com.devexed.dalwit;

/**
 * A factory which creates accessors of some type. This interface is used to abstract code that needs special handling
 * for different object classes. If a {@link Connection} driver requires special accessor handling and is provided an
 * interface extending this class, it can be wrapped by generic utility classes to extend its functionality.
 *
 *
 * @param <S>  The settable type.
 * @param <SK> The settable key type.
 * @param <G>  The gettable type.
 * @param <GK> The gettable key type.
 * @param <E>  The exception thrown.
 *
 * @see Accessor
 */
public interface AccessorFactory<S, SK, G, GK, E extends Exception> {

    /**
     * Create an accessor for a specific type, or return null if no accessor is available for the specified type.
     * @param type The type for which to create the accessor.
     * @return The created accessor or null if no accessor could be created.
     */
    Accessor<S, SK, G, GK, E> create(Class<?> type);

}
