package com.devexed.dbsource;

import com.devexed.dbsource.TransactionDatabase;
import com.devexed.dbsource.jdbc.JdbcDatabase;
import com.devexed.dbsource.jdbc.GeneratedKeysFunctionSelector;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.fail;

public final class SQLiteJdbcDatabaseTest extends DatabaseTest {

    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Override
    public TransactionDatabase openDatabase() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String dbPath = new File(dbFolder.getRoot(), "test.sqlite.db").getAbsolutePath();

        return JdbcDatabase.openWritable("jdbc:sqlite:" + dbPath, new Properties(), JdbcDatabase.accessors,
                new GeneratedKeysFunctionSelector("LAST_INSERT_ROWID()"));
    }

}
