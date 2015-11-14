package com.devexed.dbsource;

import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.junit.Assert.*;

public abstract class DatabaseTest {

    TransactionDatabase db;

    public abstract TransactionDatabase openDatabase();

    private void reopenDatabase() {
        db.close();
        db = openDatabase();
    }

    @Before
    public void setUp() {
        db = openDatabase();
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
        db.createExecution(createTable).execute(transaction);
        UpdateStatement updateStatement = db.createUpdate(insertQuery);
        updateStatement.bind("a", (long) 123);
        updateStatement.bind("b", "text");
        assertEquals(1, updateStatement.update(transaction));
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm committal.
        Cursor cursor = db.createQuery(selectQuery).query();
        assertTrue(cursor.next());
        assertEquals(123, (long) cursor.<Long>get("a"));
        assertEquals("text", cursor.get("b"));
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
        db.createExecution(createTable).execute(transaction);
        InsertStatement insertStatement = db.createInsert(insertQuery, keys);
        insertStatement.bind("a", "more text");
        Cursor keyCursor = insertStatement.insert(transaction);
        HashSet<Long> keyList = new HashSet<Long>();

        while (keyCursor.next()) keyList.add(keyCursor.<Long>get("id"));

        keyCursor.close();
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm only the inserted keys exist in the table.
        Cursor cursor = db.createQuery(selectQuery).query();
        int containedKeyCount = 0;

        while (cursor.next()) {
            if (keyList.contains(cursor.<Long>get("id"))) containedKeyCount++;
        }

        assertEquals(keyList.size(), containedKeyCount);
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
        transaction.createExecution(createTable).execute(transaction);

        // Committed child transaction.
        {
            Transaction committedTransaction = transaction.transact();
            UpdateStatement updateStatement = db.createUpdate(insertQuery);
            updateStatement.bind("a", "should be committed");
            assertEquals(1, updateStatement.update(committedTransaction));
            committedTransaction.commit();
            committedTransaction.close();
        }

        // Uncommitted child transaction.
        {
            Transaction uncommittedTransaction = transaction.transact();
            UpdateStatement updateStatement = db.createUpdate(insertQuery);
            updateStatement.bind("a", "should not be committed");
            assertEquals(1, updateStatement.update(uncommittedTransaction));
            uncommittedTransaction.close();
        }

        // Commit parent transaction.
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Ensure only committed child transaction was stored.
        Cursor cursor = db.createQuery(selectQuery).query();
        assertTrue(cursor.next());
        assertEquals("should be committed", cursor.get("a"));
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

    @Test
    public void storeBigDecimal() {
        BigDecimal value = new BigDecimal("12112399213.2132991321321323324132132132112213213213");

        HashMap<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("n", BigDecimal.class);
        }};

        Query createTable = Queries.builder()
                .forType("H2",      "CREATE TABLE t4 (n TEXT)")
                .forType("SQLite",  "CREATE TABLE t4 (n TEXT)")
                .build();
        Query insertQuery = Queries.of("INSERT INTO t4 (n) VALUES (:n)", columnTypes);
        Query selectQuery = Queries.of("SELECT n FROM t4", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        db.createExecution(createTable).execute(transaction);
        UpdateStatement updateStatement = db.createUpdate(insertQuery);
        updateStatement.bind("n", value);
        assertEquals(1, updateStatement.update(transaction));
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm committal.
        Cursor cursor = db.createQuery(selectQuery).query();
        assertTrue(cursor.next());
        assertEquals(value, cursor.get("n"));
        assertFalse(cursor.next());
        cursor.close();
    }

}
