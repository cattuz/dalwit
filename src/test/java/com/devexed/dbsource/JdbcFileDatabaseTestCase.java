package com.devexed.dbsource;

import com.devexed.dbsource.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dbsource.jdbc.JdbcDatabase;
import com.devexed.dbsource.jdbc.JdbcGeneratedKeysSelector;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class JdbcFileDatabaseTestCase extends FileDatabaseTestCase {

    private final String name;
    private final String driver;
    private final String prefix;
    private final DefaultJdbcAccessorFactory accessorFactory;
    private final JdbcGeneratedKeysSelector selector;

    protected JdbcFileDatabaseTestCase(String name, String driver, String prefix,
                                       DefaultJdbcAccessorFactory accessorFactory,
                                       JdbcGeneratedKeysSelector selector) {
        this.name = name;
        this.driver = driver;
        this.prefix = prefix;
        this.accessorFactory = accessorFactory;
        this.selector = selector;
    }

    @Override
    public final File createDatabaseFile() throws Exception {
        return File.createTempFile("test." + name, ".db");
    }

    @Override
    public final Database openDatabase() {
        Connection connection;

        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(prefix + getDatabasePath());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return JdbcDatabase.open(connection, accessorFactory, selector);
    }

}
