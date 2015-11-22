package com.devexed.dbsource;

/**
 * Defines a generic accessor which takes objects and maps appropriately to some settable target, for example an SQL
 * statement, and some gettable source, for example a query result set object.
 */
public interface Accessor<S, G, E extends Exception> {

    void set(S settable, int index, Object value) throws E;

    Object get(G gettable, int index) throws E;

}
