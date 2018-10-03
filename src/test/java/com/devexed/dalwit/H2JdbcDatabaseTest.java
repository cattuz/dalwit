package com.devexed.dalwit;

import com.devexed.dalwit.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dalwit.jdbc.FunctionJdbcGeneratedKeysSelector;

public final class H2JdbcDatabaseTest extends JdbcFileDatabaseTestCase {

    public H2JdbcDatabaseTest() {
        super("h2", "org.h2.Driver", "jdbc:h2:", new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("scope_identity()", Long.TYPE));
    }

    public void testInsertWithGeneratedKeys() {
        Query createTable = Query.of("CREATE TABLE t4 (id BIGINT PRIMARY KEY AUTO_INCREMENT, a VARCHAR(50) NOT NULL)");
        Query insertQuery = Query.builder("INSERT INTO t4 (a) VALUES (:a)")
                .parameter("a", String.class)
                .key("id", Long.TYPE)
                .build();
        Query selectQuery = Query.builder("SELECT id FROM t4")
                .column("id", Long.TYPE)
                .build();

        // Create table and insert a row.
        Transaction transaction = db.transact();
        transaction.prepare(createTable).execute();
        Statement insertStatement = transaction.prepare(insertQuery);
        insertStatement.bind("a", "more text");
        Cursor keyCursor = insertStatement.insert();
        assertTrue(keyCursor.next());
        long insertedKey = keyCursor.<Long>get("id");
        assertFalse(keyCursor.next());
        keyCursor.close();
        transaction.commit();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm only the inserted keys exist in the table.
        ReadonlyStatement Statement = db.prepare(selectQuery);
        Cursor cursor = Statement.query();
        assertTrue(cursor.next());
        assertEquals((long) cursor.get("id"), insertedKey);
        assertFalse(cursor.next());
        cursor.close();
    }

}
