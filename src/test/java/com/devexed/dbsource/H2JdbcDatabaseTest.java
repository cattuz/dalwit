package com.devexed.dbsource;

import com.devexed.dbsource.TransactionDatabase;
import com.devexed.dbsource.jdbc.GeneratedKeysFunctionSelector;
import com.devexed.dbsource.jdbc.JdbcDatabase;
import com.devexed.dbsource.jdbc.GeneratedKeysJdbcSelector;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;

public final class H2JdbcDatabaseTest extends DatabaseTest {

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Override
    TransactionDatabase openTransactionDatabase() {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String dbPath = new File(dbFolder.getRoot(), "test.h2.db").getAbsolutePath();

        return JdbcDatabase.openWritable("jdbc:h2:" + dbPath, new Properties(), JdbcDatabase.accessors,
                new GeneratedKeysFunctionSelector("SCOPE_IDENTITY()"));
    }

}
