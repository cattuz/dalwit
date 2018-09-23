package com.devexed.dalwit;

import com.devexed.dalwit.jdbc.DefaultJdbcAccessorFactory;
import com.devexed.dalwit.jdbc.FunctionJdbcGeneratedKeysSelector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class SQLiteJdbcDatabaseTest extends JdbcFileDatabaseTestCase {

    public SQLiteJdbcDatabaseTest() {
        super("sqlite", "org.sqlite.JDBC", "jdbc:sqlite:", new DefaultJdbcAccessorFactory(),
                new FunctionJdbcGeneratedKeysSelector("last_insert_rowid()", Long.TYPE));
    }

    public void testInsertWithGeneratedKeys() {
        Map<String, Class<?>> keys = Collections.singletonMap("id", Long.TYPE);
        HashMap<String, Class<?>> columnTypes = new HashMap<>(keys);
        columnTypes.put("a", String.class);
        Query createTable = Query.of("CREATE TABLE t4 (id INTEGER PRIMARY KEY, a VARCHAR(50) NOT NULL)");
        Query insertQuery = Query.of("INSERT INTO t4 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Query.of("SELECT id FROM t4", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        InsertStatement insertStatement = transaction.createInsert(insertQuery, keys);
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
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query();
        assertTrue(cursor.next());
        assertEquals((long) cursor.get("id"), insertedKey);
        assertFalse(cursor.next());
        cursor.close();
    }

}
