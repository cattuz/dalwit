package com.devexed.dalwit;

import com.devexed.dalwit.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dalwit.jdbc.JdbcConnection;
import com.devexed.dalwit.jdbc.JdbcGeneratedKeysSelector;

import java.io.File;
import java.util.Properties;

public abstract class JdbcFileDatabaseTestCase extends DatabaseTestCase {

    private final String name;
    private final String driver;
    private final String prefix;
    private final DefaultJdbcAccessorFactory accessorFactory;
    private final JdbcGeneratedKeysSelector selector;

    private File file;

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
    public Connection createConnection() {
        try {
            file = File.createTempFile("test." + name, ".db");
            return new JdbcConnection(driver, prefix + file.getAbsolutePath(), new Properties(), accessorFactory, selector, new DefaultColumnNameMapper());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroyConnection() {
        if (!file.delete()) throw new RuntimeException("Failed to delete database file " + file.getAbsolutePath());
    }

}
