package com.devexed.dbsource;

import com.devexed.dbsource.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dbsource.jdbc.FunctionJdbcGeneratedKeysSelector;
import com.devexed.dbsource.jdbc.JdbcDatabase;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;

import static org.junit.Assert.fail;

public class SQLiteJdbcDatabaseTest extends FileDatabaseTestCase {

    @Override
    public File createDatabaseFile() throws Exception {
        return File.createTempFile("test.sqlite.", ".db");
    }

    @Override
    public TransactionDatabase openDatabase() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return JdbcDatabase.openWritable("jdbc:sqlite:" + getDatabasePath(), new Properties(),
                new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("LAST_INSERT_ROWID()", Long.TYPE));
    }

}
