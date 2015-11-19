package com.devexed.dbsource;

import com.devexed.dbsource.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dbsource.jdbc.FunctionJdbcGeneratedKeysSelector;

public class SQLiteJdbcDatabaseTest extends JdbcFileDatabaseTestCase {

    public SQLiteJdbcDatabaseTest() {
        super("sqlite", "org.sqlite.JDBC", "jdbc:sqlite:", new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("last_insert_rowid()", Long.TYPE));
    }

}
