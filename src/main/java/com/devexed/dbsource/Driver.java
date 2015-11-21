package com.devexed.dbsource;

/**
 * Container of database driver information. Used to provide different queries and implementations for different
 * database types and type versions.
 */
public interface Driver {

    /**
     * @return The type of the underlying driver providing the database access. For example "SQLite" or "H2".
     */
    String getType();

    /**
     * @return The version of the of the underlying driver providing the database. Typically a string of integers
     * separated by dots (e.g. <code>"1.0.2"</code>), but it's not guaranteed to follow that format.
     */
    String getVersion();

}
