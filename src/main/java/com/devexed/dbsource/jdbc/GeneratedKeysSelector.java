package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.ReadonlyDatabase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Selects primary keys generated after an insert.
 */
public interface GeneratedKeysSelector {

    /**
     * Prepares the insert statement.
     *
     * @param database   The database requiring the prepared statement.
     * @param connection The JDBC connection with which to prepare the statement.
     * @param sql        The SQL insert query to prepare.
     * @param keys       The generated keys requested to be returned after executing.
     * @return A prepared insert statement.
     * @throws SQLException If the statement could not be prepared for any reason.
     */
    PreparedStatement prepareInsertStatement(ReadonlyDatabase database, Connection connection, String sql,
                                             Map<String, Class<?>> keys) throws SQLException;

    /**
     * Retrieve the generated keys from the database. Always called after the insert statement is executed with no
     * intermediate statements executed.
     *
     * @param database        The database requiring the prepared statement.
     * @param statement       The recently executed insertion statement prepared with {@link #prepareInsertStatement}.
     * @param accessorFactory The active JDBC accessors of the database.
     * @param keys            The generated keys requested to be returned after executing.
     * @return A cursor over the generated keys.
     * @throws SQLException If the generated keys could not be queried for any reason.
     */
    Cursor selectGeneratedKeys(ReadonlyDatabase database, PreparedStatement statement, JdbcAccessorFactory accessorFactory,
                               Map<String, Class<?>> keys)
            throws SQLException;

}
