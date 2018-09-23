package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A connection to a JDBC database.
 */
public final class JdbcConnection implements Connection {

    private final String driverClass;
    private final String url;
    private final Properties properties;
    private final AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory;
    private final JdbcGeneratedKeysSelector generatedKeysSelector;

    /**
     * Creates a connection object which can open databases for reading or writing using a JDBC driver.
     * Creating the {@link JdbcConnection} itself is does not perform any connective action.
     *
     * @param driverClass           The JDBC driver class.
     * @param url                   The JDBC connection url.
     * @param properties            The JDBC connection properties.
     * @param accessorFactory       The accessor factory creating accessors
     * @param generatedKeysSelector The selector of generated keys after inserts.
     */
    public JdbcConnection(String driverClass, String url, Properties properties,
                          AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                          JdbcGeneratedKeysSelector generatedKeysSelector) {
        this.driverClass = driverClass;
        this.url = url;
        this.properties = properties;
        this.accessorFactory = accessorFactory;
        this.generatedKeysSelector = generatedKeysSelector;
    }

    /**
     * Creates a default JDBC connection, with the default accessor factory and generated key selector.
     *
     * @see #JdbcConnection(String, String, Properties, AccessorFactory, JdbcGeneratedKeysSelector)
     */
    private JdbcConnection(String driverClass, String url, Properties properties) {
        this(driverClass, url, properties, new DefaultJdbcAccessorFactory(), new DefaultJdbcGeneratedKeysSelector());
    }

    /**
     * Open a {@link JdbcDatabase}.
     * @param readonly The readonly flag of the JDBC connection.
     * @return The opened database.
     */
    private JdbcDatabase open(boolean readonly) {
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new DatabaseException(e);
        }

        java.sql.Connection connection;

        try {
            connection = DriverManager.getConnection(url, properties);
            connection.setReadOnly(readonly);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return new JdbcDatabase(connection, accessorFactory, generatedKeysSelector);
    }

    /**
     * Open a database for writing. This method is thread safe.
     *
     * @see Connection#write()
     */
    @Override
    public Database write() {
        return open(false);
    }

    /**
     * Open a database for reading. This method is thread safe.
     *
     * @see Connection#write()
     */
    @Override
    public ReadonlyDatabase read() {
        return open(true);
    }

}
