package com.devexed.dalwit;

import com.devexed.dalwit.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dalwit.jdbc.FunctionJdbcGeneratedKeysSelector;

public class SQLiteJdbcDatabaseTest extends JdbcFileDatabaseTestCase {

    public SQLiteJdbcDatabaseTest() {
        super("sqlite", "org.sqlite.JDBC", "jdbc:sqlite:", new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("last_insert_rowid()", Long.TYPE));
    }

}
