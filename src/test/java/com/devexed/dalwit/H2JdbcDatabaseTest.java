package com.devexed.dalwit;

import com.devexed.dalwit.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dalwit.jdbc.FunctionJdbcGeneratedKeysSelector;

public class H2JdbcDatabaseTest extends JdbcFileDatabaseTestCase {

    public H2JdbcDatabaseTest() {
        super("h2", "org.h2.Driver", "jdbc:h2:", new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("SCOPE_IDENTITY()", Long.TYPE));
    }

}
