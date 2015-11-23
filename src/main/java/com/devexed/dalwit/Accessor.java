package com.devexed.dalwit;

/**
 * Defines a generic accessor which takes objects and maps appropriately to some settable target, for example an SQL
 * statement, and some gettable source, for example a query result set object.
 *
 * @see AccessorFactory
 */
public interface Accessor<S, SK, G, GK, E extends Exception> {

    /**
     * Set a value by key on the settable.
     *
     * @param settable The settable receiving the value.
     * @param key The key which to associate with the value.
     * @param value The value to set.
     * @throws E The exception thrown in case the value could not be set.
     */
    void set(S settable, SK key, Object value) throws E;

    /**
     * Get a value by key from a gettable.
     *
     * @param gettable The gettable from which to receive the value.
     * @param key The key of the received value.
     * @return The received value.
     * @throws E The exception thrown in case the value could not be gotten.
     */
    Object get(G gettable, GK key) throws E;

}
