package com.devexed.dbsource;

import com.devexed.dbsource.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dbsource.jdbc.FunctionJdbcGeneratedKeysSelector;
import com.devexed.dbsource.jdbc.JdbcDatabase;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLiteJdbcDatabaseTest extends FileDatabaseTestCase {

    @Override
    public File createDatabaseFile() throws Exception {
        return File.createTempFile("test.sqlite.", ".db");
    }

    @Override
    public Database openDatabase() {
        Connection connection;

        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + getDatabasePath());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return JdbcDatabase.open(connection,
                new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("last_insert_rowid()", Long.TYPE));
    }

}
