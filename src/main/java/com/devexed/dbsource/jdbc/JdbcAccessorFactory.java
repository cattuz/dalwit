package com.devexed.dbsource.jdbc;

/**
 * A factory which creates JDBC column and parameter accessors for types.
 */
public interface JdbcAccessorFactory {

    JdbcAccessor create(Class<?> type);

}
