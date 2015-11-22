package com.devexed.dbsource;

/**
 * A factory which creates accessors of some type.
 */
public interface AccessorFactory<S, G, E extends Exception> {

    Accessor<S, G, E> create(Class<?> type);

}
