package com.devexed.dbsource;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public abstract class DatabaseTest {

    TransactionDatabase db;

    public abstract TransactionDatabase openTransactionDatabase();

    private void reopenDatabase() {
        db.close();
        db = openTransactionDatabase();
    }

    @Before
    public void setUp() {
        db = openTransactionDatabase();
    }

    @Test
    public void emptyTransaction() {
        db.transact().close();
    }

    @Test
    public void transactionCommits() {
        HashMap<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("a", Long.TYPE);
            put("b", String.class);
        }};

        Query createTable = Queries.of("CREATE TABLE t1 (a INTEGER NOT NULL, b VARCHAR(50) NULL)");
        Query insertQuery = Queries.of("INSERT INTO t1 (a, b) VALUES (:a, :b)", columnTypes);
        Query selectQuery = Queries.of("SELECT a, b FROM t1", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        db.prepareExecution(createTable).execute(transaction);
        UpdateStatement updateStatement = db.prepareUpdate(insertQuery);
        updateStatement.bind("a", (long) 123);
        updateStatement.bind("b", "text");
        assertEquals(updateStatement.update(transaction), 1);
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm committal.
        DatabaseCursor cursor = db.createQuery(selectQuery).query();
        assertTrue(cursor.next());
        assertEquals((long) cursor.<Long>get("a"), 123);
        assertEquals(cursor.get("b"), "text");
        assertFalse(cursor.next());
        cursor.close();
    }

    @Test
    public void insertWithGeneratedKeys() {
        final Map<String, Class<?>> keys = Collections.<String, Class<?>>singletonMap("id", Long.TYPE);
        final HashMap<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            putAll(keys);
            put("a", String.class);
        }};

        Query createTable = Queries.builder()
                .forType("H2",      "CREATE TABLE t2 (id BIGINT PRIMARY KEY AUTO_INCREMENT, a VARCHAR(50) NOT NULL)")
                .forType("SQLite",  "CREATE TABLE t2 (id INTEGER PRIMARY KEY,               a VARCHAR(50) NOT NULL)")
                .build();

        Query insertQuery = Queries.of("INSERT INTO t2 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT id FROM t2", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        db.prepareExecution(createTable).execute(transaction);
        InsertStatement insertStatement = db.prepareInsert(insertQuery, keys);
        insertStatement.bind("a", "more text");
        DatabaseCursor keyCursor = insertStatement.insert(transaction);
        HashSet<Long> keyList = new HashSet<Long>();

        while (keyCursor.next()) keyList.add(keyCursor.<Long>get("id"));

        keyCursor.close();
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm only the inserted keys exist in the table.
        DatabaseCursor cursor = db.createQuery(selectQuery).query();
        int containedKeyCount = 0;

        while (cursor.next()) {
            if (keyList.contains(cursor.<Long>get("id"))) containedKeyCount++;
        }

        assertEquals(containedKeyCount, keyList.size());
        assertFalse(cursor.next());
        cursor.close();
    }

    @Test
    public void nestedTransactionsCommit() {
        HashMap<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("a", String.class);
        }};

        Query createTable = Queries.of("CREATE TABLE t3 (a VARCHAR(50) NULL)");
        Query insertQuery = Queries.of("INSERT INTO t3 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT a FROM t3", columnTypes);

        // Start parent transaction.
        Transaction transaction = db.transact();
        transaction.prepareExecution(createTable).execute(transaction);

        // Committed child transaction.
        {
            Transaction committedTransaction = transaction.transact();
            UpdateStatement updateStatement = db.prepareUpdate(insertQuery);
            updateStatement.bind("a", "should be committed");
            assertEquals(updateStatement.update(committedTransaction), 1);
            committedTransaction.commit();
            committedTransaction.close();
        }

        // Uncommitted child transaction.
        {
            Transaction uncommittedTransaction = transaction.transact();
            UpdateStatement updateStatement = db.prepareUpdate(insertQuery);
            updateStatement.bind("a", "should not be committed");
            assertEquals(updateStatement.update(uncommittedTransaction), 1);
            uncommittedTransaction.close();
        }

        // Commit parent transaction.
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Ensure only committed child transaction was stored.
        DatabaseCursor cursor = db.createQuery(selectQuery).query();
        assertTrue(cursor.next());
        assertEquals(cursor.get("a"), "should be committed");
        assertFalse(cursor.next());
        cursor.close();
    }

    @Test
    public void danglingTransactionThrows() {
        try {
            Transaction transaction = db.transact();
            transaction.transact();
            transaction.close();
            fail("Expected exception when closing parent transaction without closing all child transactions first.");
        } catch (DatabaseException e) {
            // Should succeed if an exception occurs.
        }
    }

}
