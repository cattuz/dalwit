package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;
import com.devexed.dbsource.util.CloseableManager;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A driver which opens JDBC databases.
 */
public final class JdbcConnection implements Connection {

    private final String driverClass;
    private final String url;
    private final Properties properties;
    private final AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory;
    private final JdbcGeneratedKeysSelector generatedKeysSelector;
    private final CloseableManager<JdbcDatabase> databaseManager = new CloseableManager<JdbcDatabase>(Connection.class,
            Database.class, Collections.newSetFromMap(new ConcurrentHashMap<JdbcDatabase, Boolean>()));

    public JdbcConnection(String driverClass, String url, Properties properties,
                          AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                          JdbcGeneratedKeysSelector generatedKeysSelector) {
        this.driverClass = driverClass;
        this.url = url;
        this.properties = properties;
        this.accessorFactory = accessorFactory;
        this.generatedKeysSelector = generatedKeysSelector;
    }

    public JdbcConnection(String driverClass, String url, Properties properties) {
        this(driverClass, url, properties, new DefaultJdbcAccessorFactory(), new DefaultJdbcGeneratedKeysSelector());
    }

    public JdbcConnection(String driverClass, String url) {
        this(driverClass, url, new Properties());
    }

    private JdbcDatabase open() {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new DatabaseException(e);
        }

        java.sql.Connection connection;

        try {
            connection = DriverManager.getConnection(url, properties);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        JdbcDatabase database = new JdbcDatabase(connection, accessorFactory, generatedKeysSelector);
        databaseManager.open(database);
        return database;
    }

    @Override
    public Database write() {
        JdbcDatabase database = open();

        try {
            database.connection.setReadOnly(false);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return database;
    }

    @Override
    public ReadonlyDatabase read() {
        JdbcDatabase database = open();

        try {
            database.connection.setReadOnly(true);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return write();
    }

    @Override
    public void close(ReadonlyDatabase database) {
        databaseManager.close(database);
    }

}
