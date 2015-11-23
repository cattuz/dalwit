package com.devexed.dalwit;

import com.devexed.dalwit.util.Queries;
import junit.framework.TestCase;

import java.math.BigDecimal;
import java.util.*;

/**
 * Note: Tests written in JUNIT 3 style for Android compatibility.
 */
public abstract class DatabaseTestCase extends TestCase {

    private Connection connection;
    private Database db;

    public abstract Connection createConnection();

    public abstract void destroyConnection();

    private void reopenDatabase() {
        connection.close(db);
        db = connection.write();
    }

    @Override
    public void setUp() throws Exception {
        connection = createConnection();
        db = connection.write();
    }

    @Override
    protected void tearDown() throws Exception {
        connection.close(db);
        destroyConnection();
        super.tearDown();
    }

    public void testIgnoresParameterInSQLGroups() {
        Query insertQuery = Queries.of("INSERT INTO q (a AS ':b', a AS [:c]) VALUES (:a, \":d\")");
        HashMap<String, ArrayList<Integer>> parameters = new HashMap<String, ArrayList<Integer>>();
        insertQuery.create(db, parameters);

        assertTrue(parameters.containsKey("a"));
        assertTrue(!parameters.containsKey("b"));
        assertTrue(!parameters.containsKey("c"));
        assertTrue(!parameters.containsKey("d"));
    }

    public void testBindsTypedQueryParameter() {
        Map<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("a", Integer.class);
        }};
        Query createTable = Queries.of("CREATE TABLE q2 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO q2 (a) VALUES (:a)", columnTypes);

        Transaction transaction = db.transact();
        db.createExecution(createTable).execute(transaction);
        UpdateStatement queryStatement = db.createUpdate(insertQuery);
        queryStatement.bind("a", 123);
        db.commit(transaction);
    }

    public void testBindWrongQueryParameterTypeThrows() {
        Map<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("a", Integer.class);
        }};
        Query createTable = Queries.of("CREATE TABLE q3 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO q3 (a) VALUES (:a)", columnTypes);

        Transaction transaction = db.transact();
        db.createExecution(createTable).execute(transaction);
        UpdateStatement queryStatement = db.createUpdate(insertQuery);

        try {
            queryStatement.bind("a", "test");
            fail("Succeed binding wrong type to parameter");
        } catch (Exception e) {
            // Success if reached.
        }

        db.commit(transaction);
    }

    public void testEmptyTransaction() {
        db.commit(db.transact());
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
        db.commit(transaction);

        // Re-open database to ensure persistence.
        reopenDatabase();

        // Query to confirm committal.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query(db);
        assertTrue(cursor.next());
        assertEquals("committed", cursor.get("a"));
        assertFalse(cursor.next());
        queryStatement.close(cursor);
    }

    public void testTransactionRollsBack() {
        Map<String, Class<?>> columnTypes = Collections.<String, Class<?>>singletonMap("a", String.class);
        Query createTable = Queries.of("CREATE TABLE t2 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO t2 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT a FROM t2", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        db.createExecution(createTable).execute(transaction);
        db.commit(transaction);

        Transaction insertTransaction = db.transact();
        UpdateStatement updateStatement = db.createUpdate(insertQuery);
        updateStatement.bind("a", "committed");
        assertEquals(1, updateStatement.update(insertTransaction));
        db.rollback(insertTransaction);

        // Re-open database to ensure persistence.
        reopenDatabase();

        // Query to confirm committal.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query(db);
        assertFalse(cursor.next());
        queryStatement.close(cursor);
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
            transaction.commit(committedTransaction);
        }

        // Uncommitted child transaction.
        {
            Transaction uncommittedTransaction = transaction.transact();
            UpdateStatement updateStatement = db.createUpdate(insertQuery);
            updateStatement.bind("a", "should not be committed");
            assertEquals(1, updateStatement.update(uncommittedTransaction));
            transaction.rollback(uncommittedTransaction);
        }

        // Commit parent transaction.
        db.commit(transaction);

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Ensure only committed child transaction was stored.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query(db);
        assertTrue(cursor.next());
        assertEquals("should be committed", cursor.get("a"));
        assertFalse(cursor.next());
        queryStatement.close(cursor);
    }

    public void testCommitWithDanglingTransactionThrows() {
        try {
            Transaction transaction = db.transact();
            transaction.transact();
            db.commit(transaction);
            fail("Should throw");
        } catch (DatabaseException e) {
            // Should throw
        }
    }

    public void testRollbackWithDanglingTransactionThrows() {
        try {
            Transaction transaction = db.transact();
            transaction.transact();
            db.rollback(transaction);
            fail("Should throw");
        } catch (DatabaseException e) {
            // Should throw
        }
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
                .type("H2", "CREATE TABLE t4 (id BIGINT PRIMARY KEY AUTO_INCREMENT, a VARCHAR(50) NOT NULL)")
                .type("SQLite", "CREATE TABLE t4 (id INTEGER PRIMARY KEY,               a VARCHAR(50) NOT NULL)")
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

        insertStatement.close(keyCursor);
        db.commit(transaction);

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm only the inserted keys exist in the table.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query(db);
        int containedKeyCount = 0;

        while (cursor.next()) {
            if (keyList.contains(cursor.<Long>get("id"))) containedKeyCount++;
        }

        assertEquals(keyList.size(), containedKeyCount);
        queryStatement.close(cursor);
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
        db.commit(transaction);

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm committal.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query(db);
        assertTrue(cursor.next());
        assertEquals(bigDecimal, cursor.get("n"));
        assertFalse(cursor.next());
        queryStatement.close(cursor);
    }

}
