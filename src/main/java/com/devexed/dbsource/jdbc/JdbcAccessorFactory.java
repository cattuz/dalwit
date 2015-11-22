package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Accessor;
import com.devexed.dbsource.AccessorFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A factory which creates JDBC column and parameter accessors for types.
 */
public interface JdbcAccessorFactory extends AccessorFactory<PreparedStatement, ResultSet, SQLException> {

    @Override
    Accessor<PreparedStatement, ResultSet, SQLException> create(Class<?> type);

}
