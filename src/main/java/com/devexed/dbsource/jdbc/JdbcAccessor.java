package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Accessor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Accessor to bind values to and retrieve values from JDBC interfaces.
 */
public interface JdbcAccessor extends Accessor<PreparedStatement, ResultSet, SQLException> {

    @Override
    void set(PreparedStatement statement, int index, Object value) throws SQLException;

    @Override
    Object get(ResultSet resultSet, int index) throws SQLException;

}
