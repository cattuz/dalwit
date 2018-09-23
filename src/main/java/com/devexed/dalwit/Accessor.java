package com.devexed.dalwit;

/**
 * Defines a generic accessor which takes objects and maps appropriately to some settable target, for example an SQL
 * statement, and some gettable source, for example a query result set object.
 *
 * @see AccessorFactory
 */
public interface Accessor<S, G, E extends Exception> {

    /**
     * Set a value by key on the settable.
     *
     * @param settable The settable receiving the value.
     * @param index The index of the value.
     * @param value The value to set.
     * @throws E The exception thrown in case the value could not be set.
     */
    void set(S settable, int index, Object value) throws E;

    /**
     * Get a value by key from a gettable.
     *
     * @param gettable The gettable from which to receive the value.
     * @param index The index of the value.
     * @return The received value.
     * @throws E The exception thrown in case the value could not be gotten.
     */
    Object get(G gettable, int index) throws E;
    
}
