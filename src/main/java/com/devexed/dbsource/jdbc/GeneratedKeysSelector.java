package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseCursor;
import com.devexed.dbsource.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Selects primary keys generated after an insert.
 */
public interface GeneratedKeysSelector {

    PreparedStatement prepareInsertStatement(Database database, Connection connection, String sql,
                                             Map<String, Class<?>> keys) throws SQLException;

    DatabaseCursor selectGeneratedKeys(Database database, PreparedStatement statement, Map<Class<?>,
            JdbcAccessor> accessors, Map<String, Class<?>> keys) throws SQLException;

}
