package com.devexed.dbsource;

import com.devexed.dbsource.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dbsource.jdbc.FunctionJdbcGeneratedKeysSelector;
import com.devexed.dbsource.jdbc.JdbcDatabase;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Properties;

public class H2JdbcDatabaseTest extends FileDatabaseTestCase {

    @Override
    public File createDatabaseFile() throws Exception {
        return File.createTempFile("test.h2.", ".db");
    }

    @Override
    public TransactionDatabase openDatabase() {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        return JdbcDatabase.openWritable("jdbc:h2:" + getDatabasePath(), new Properties(),
                new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("SCOPE_IDENTITY()", Long.TYPE));
    }

}
