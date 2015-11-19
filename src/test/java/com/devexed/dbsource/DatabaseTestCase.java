package com.devexed.dbsource;

import junit.framework.TestCase;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Note: Tests written in JUNIT 3 style for Android compatibility.
 */
public abstract class DatabaseTestCase extends TestCase {

    Database db;

    public abstract void createDatabase() throws Exception;

    public abstract void destroyDatabase() throws Exception;

    public abstract Database openDatabase();

    private void reopenDatabase() {
        db.close();
        db = openDatabase();
    }

    @Override
    public void setUp() throws Exception {
        createDatabase();
        db = openDatabase();
    }

    @Override
    protected void tearDown() throws Exception {
        destroyDatabase();
        super.tearDown();
    }

    public void testEmptyTransaction() {
        db.transact().close();
    }

    public void testTransactionCommits() {
        Map<String, Class<?>> columnTypes = Collections.<String, Class<?>>singletonMap("a", String.class);
        Query createTable = Queries.of("CREATE TABLE t1 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO t1 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT a FROM t1", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        db.createExecution(createTable).execute(transaction);
        UpdateStatement updateStatement = db.createUpdate(insertQuery);
        updateStatement.bind("a", "committed");
        assertEquals(1, updateStatement.update(transaction));
        transaction.commit();
        transaction.close();

        // Re-open database to ensure persistence.
        reopenDatabase();

        // Query to confirm committal.
        Cursor cursor = db.createQuery(selectQuery).query();
        assertTrue(cursor.next());
        assertEquals("committed", cursor.get("a"));
        assertFalse(cursor.next());
        cursor.close();
    }

    public void testTransactionRollsBack() {
        Map<String, Class<?>> columnTypes = Collections.<String, Class<?>>singletonMap("a", String.class);
        Query createTable = Queries.of("CREATE TABLE t2 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO t2 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT a FROM t2", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        db.createExecution(createTable).execute(transaction);
        transaction.commit();
        transaction.close();

        Transaction insertTransaction = db.transact();
        UpdateStatement updateStatement = db.createUpdate(insertQuery);
        updateStatement.bind("a", "committed");
        assertEquals(1, updateStatement.update(insertTransaction));
        // Note: not committal
        transaction.close();

        // Re-open database to ensure persistence.
        reopenDatabase();

        // Query to confirm committal.
        Cursor cursor = db.createQuery(selectQuery).query();
        assertFalse(cursor.next());
        cursor.close();
    }

    public void testNestedTransactionsCommit() {
        Map<String, Class<?>> columnTypes = Collections.<String, Class<?>>singletonMap("a", String.class);
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

    public void testTransactionClosesDanglingTransaction() {
        Transaction transaction = db.transact();
        transaction.transact();
        transaction.close();
    }

    public void testDatabaseClosesDanglingTransaction() {
        Transaction transaction = db.transact();
        transaction.transact();
        reopenDatabase();
    }

    public void testInsertWithGeneratedKeys() {
        Map<String, Class<?>> keys = Collections.<String, Class<?>>singletonMap("id", Long.TYPE);
        HashMap<String, Class<?>> columnTypes = new HashMap<String, Class<?>>();
        columnTypes.putAll(keys);
        columnTypes.put("a", String.class);
        Query createTable = Queries.builder()
                .forType("H2", "CREATE TABLE t4 (id BIGINT PRIMARY KEY AUTO_INCREMENT, a VARCHAR(50) NOT NULL)")
                .forType("SQLite", "CREATE TABLE t4 (id INTEGER PRIMARY KEY,               a VARCHAR(50) NOT NULL)")
                .build();
        Query insertQuery = Queries.of("INSERT INTO t4 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT id FROM t4", columnTypes);

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
        cursor.close();
    }

    public void testStoreBigDecimal() {
        BigDecimal bigDecimal = new BigDecimal("12112399213.2132991321321323324132132132112213213213");

        HashMap<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("n", BigDecimal.class);
        }};

        // A text column type works when the underlying JDBC implementation supports coercing BigDecimal to text.
        Query createTable = Queries.of("CREATE TABLE t5 (n TEXT)");
        Query insertQuery = Queries.of("INSERT INTO t5 (n) VALUES (:n)", columnTypes);
        Query selectQuery = Queries.of("SELECT n FROM t5", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        db.createExecution(createTable).execute(transaction);
        UpdateStatement updateStatement = db.createUpdate(insertQuery);
        updateStatement.bind("n", bigDecimal);
        assertEquals(1, updateStatement.update(transaction));
        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm committal.
        Cursor cursor = db.createQuery(selectQuery).query();
        assertTrue(cursor.next());
        assertEquals(bigDecimal, cursor.get("n"));
        assertFalse(cursor.next());
        cursor.close();
    }

}
