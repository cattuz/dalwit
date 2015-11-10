package com.devexed.dbsource.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Accessor to bind values to and retrieve values from JDBC interfaces.
 */
public interface JdbcAccessor {

    void set(PreparedStatement statement, int index, Object value) throws SQLException;

    Object get(ResultSet resultSet, int index) throws SQLException;

}
