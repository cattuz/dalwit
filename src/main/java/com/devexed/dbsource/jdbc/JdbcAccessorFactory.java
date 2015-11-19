package com.devexed.dbsource.jdbc;

public interface JdbcAccessorFactory {

    JdbcAccessor create(Class<?> type);

}
