package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.AccessorFactory;
import com.devexed.dalwit.Cursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Selects generated keys generated after an insert.
 */
public interface JdbcGeneratedKeysSelector {

    /**
     * Prepares the insert statement.
     *
     * @param connection The JDBC connection with which to prepare the statement.
     * @param sql        The SQL insert query to prepare.
     * @param keys       The generated keys requested to be returned after executing.
     * @return A prepared insert statement.
     * @throws SQLException If the statement could not be prepared for any reason.
     */
    PreparedStatement prepareInsertStatement(Connection connection, String sql, Map<String, Class<?>> keys)
            throws SQLException;

    /**
     * Retrieve the generated keys from the database. Always called after the insert statement is executed with no
     * intermediate statements executed.
     *
     * @param connection      The JDBC connection.
     * @param statement       The recently executed insertion statement prepared with {@link #prepareInsertStatement}.
     * @param accessorFactory The active JDBC accessors of the database.
     * @param keys            The generated keys requested to be returned after executing.
     * @return A cursor over the generated keys.
     * @throws SQLException If the generated keys could not be queried for any reason.
     */
    Cursor selectGeneratedKeys(Connection connection, PreparedStatement statement,
                               AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                               Map<String, Class<?>> keys) throws SQLException;

}
