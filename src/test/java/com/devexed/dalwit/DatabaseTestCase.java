package com.devexed.dalwit;

import junit.framework.TestCase;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/**
 * Note: Tests written in JUNIT 3 style for Android compatibility.
 */
public abstract class DatabaseTestCase extends TestCase {

    private Connection connection;
    Database db;

    protected abstract Connection createConnection();

    protected abstract void destroyConnection();

    void reopenDatabase() {
        db.close();
        db = connection.write();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
        db = connection.write();
    }

    @Override
    protected void tearDown() throws Exception {
        db.close();
        destroyConnection();
        db = null;
        connection = null;
        super.tearDown();
    }

    public void testIgnoresParameterInSQLGroups() {
        Query insertQuery = Query.builder("INSERT INTO q (a AS ':b', a AS [:c]) VALUES (:a, \":d\")")
                .declare("a", String.class)
                .build();
        insertQuery.typeOf("a"); // Should have been parsed as parameter

        try {
            insertQuery.typeOf("b"); // Should NOT have been parsed as parameter
            insertQuery.typeOf("c"); // -"-
            insertQuery.typeOf("d"); // -"-
            fail("Parameter in group should not be parsed");
        } catch (DatabaseException e) {
            // Should throw
        }
    }

    public void testBindsTypedQueryParameter() {
        Query createTable = Query.of("CREATE TABLE q2 (a TEXT NOT NULL)");
        Query insertQuery = Query.builder("INSERT INTO q2 (a) VALUES (:a)").declare("a", Integer.class).build();

        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        UpdateStatement queryStatement = transaction.createUpdate(insertQuery);
        queryStatement.bind("a", 123);
        transaction.commit();
        transaction.close();
    }

    public void testBindWrongQueryParameterTypeThrows() {
        Query createTable = Query.of("CREATE TABLE q3 (a TEXT NOT NULL)");
        Query insertQuery = Query.builder("INSERT INTO q3 (a) VALUES (:a)").declare("a", Integer.class).build();

        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        UpdateStatement queryStatement = transaction.createUpdate(insertQuery);

        try {
            queryStatement.bind("a", "test");
            fail("Bound wrong type to parameter");
        } catch (Exception e) {
            // Success if reached.
        }

        transaction.commit();
        transaction.close();
    }

    public void testEmptyTransaction() {
        Transaction transaction = db.transact();
        transaction.commit();
        transaction.close();
    }

    public void testTransactionCommits() {
        Query createTable = Query.of("CREATE TABLE t1 (a TEXT NOT NULL)");
        Query insertQuery = Query
                .builder("INSERT INTO t1 (a) VALUES (:a)")
                .declare("a", String.class)
                .build();
        Query selectQuery = Query
                .builder("SELECT a FROM t1")
                .declare("a", String.class)
                .build();

        // Create table and insert a row.
        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        UpdateStatement updateStatement = transaction.createUpdate(insertQuery);
        updateStatement.bind("a", "committed");
        assertEquals(1, updateStatement.update());
        transaction.commit();
        transaction.close();

        // Re-open database to ensure persistence.
        reopenDatabase();

        // Query to confirm committal.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query();
        assertTrue(cursor.next());
        assertEquals("committed", cursor.get("a"));
        assertFalse(cursor.next());
        cursor.close();
    }

    public void testTransactionRollsBack() {
        Map<String, Class<?>> columnTypes = Collections.singletonMap("a", String.class);
        Query createTable = Query.of("CREATE TABLE t2 (a TEXT NOT NULL)");
        Query insertQuery = Query.of("INSERT INTO t2 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Query.of("SELECT a FROM t2", columnTypes);

        // Create table and insert a row.
        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        transaction.commit();
        transaction.close();

        Transaction insertTransaction = db.transact();
        UpdateStatement updateStatement = insertTransaction.createUpdate(insertQuery);
        updateStatement.bind("a", "committed");
        assertEquals(1, updateStatement.update());
        insertTransaction.close();

        // Re-open database to ensure persistence.
        reopenDatabase();

        // Query to confirm committal.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query();
        assertFalse(cursor.next());
        cursor.close();
    }

    public void testNestedTransactionsCommit() {
        Map<String, Class<?>> columnTypes = Collections.singletonMap("a", String.class);
        Query createTable = Query.of("CREATE TABLE t3 (a VARCHAR(50) NULL)");
        Query insertQuery = Query.of("INSERT INTO t3 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Query.of("SELECT a FROM t3", columnTypes);

        // Start parent transaction.
        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();

        // Committed child transaction.
        {
            Transaction committedTransaction = transaction.transact();
            UpdateStatement updateStatement = committedTransaction.createUpdate(insertQuery);
            updateStatement.bind("a", "should be committed");
            assertEquals(1, updateStatement.update());
            committedTransaction.commit();
            committedTransaction.close();
        }

        // Uncommitted child transaction.
        {
            Transaction uncommittedTransaction = transaction.transact();
            UpdateStatement updateStatement = uncommittedTransaction.createUpdate(insertQuery);
            updateStatement.bind("a", "should not be committed");
            assertEquals(1, updateStatement.update());
            // Close without commit.
            uncommittedTransaction.close();
        }

        // Commit parent transaction.
        transaction.commit();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Ensure only committed child transaction was persisted.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query();
        assertTrue(cursor.next());
        assertEquals("should be committed", cursor.get("a"));
        assertFalse(cursor.next());
        cursor.close();
    }

    public void testCommitWithDanglingTransactionThrows() {
        try {
            Transaction transaction = db.transact();
            transaction.transact(); // Dangle transaction
            transaction.commit();
            transaction.close();
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

    public void testStoreBigDecimal() {
        BigDecimal bigDecimal = new BigDecimal("12112399213.2132991321321323324132132132112213213213");

        // A text column type works when the underlying JDBC implementation supports coercing BigDecimal to text.
        Query createTable = Query.of("CREATE TABLE t5 (n TEXT)");
        Query insertQuery = Query.builder("INSERT INTO t5 (n) VALUES (:n)").declare("n", BigDecimal.class).build();
        Query selectQuery = Query.builder("SELECT n FROM t5").declare("n", BigDecimal.class).build();

        // Create table and insert a row.
        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        UpdateStatement updateStatement = transaction.createUpdate(insertQuery);
        updateStatement.bind("n", bigDecimal);
        assertEquals(1, updateStatement.update());
        transaction.commit();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Query to confirm committal.
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query();
        assertTrue(cursor.next());
        assertEquals(bigDecimal, cursor.get("n"));
        assertFalse(cursor.next());
        cursor.close();
    }

    public void testDeepNestedTransactionsRollback() {
        Map<String, Class<?>> columnTypes = Collections.singletonMap("a", String.class);
        Query createTable = Query.of("CREATE TABLE t6 (a VARCHAR(50) NULL)");
        Query insertQuery = Query.of("INSERT INTO t6 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Query.of("SELECT a FROM t6", columnTypes);

        int transactionDepth = 15;
        ArrayList<Transaction> transactions = new ArrayList<>(transactionDepth);
        Transaction transaction = db.transact();
        transactions.add(transaction);
        transaction.createExecution(createTable).execute();

        for (int i = 0; i < transactionDepth - 1; i++) {
            transaction = transaction.transact();
            transactions.add(transaction);

            UpdateStatement updateStatement = transaction.createUpdate(insertQuery);
            updateStatement.bind("a", "should be committed");
            assertEquals(1, updateStatement.update());
        }

        for (int i = transactionDepth - 1; i >= 1; i--) {
            transaction.close();
            transaction = transactions.get(i - 1);
        }

        transaction.commit();
        transaction.close();

        // Close and reopen database to ensure data persistence.
        reopenDatabase();

        // Ensure table is empty
        QueryStatement queryStatement = db.createQuery(selectQuery);
        Cursor cursor = queryStatement.query();
        assertFalse(cursor.next());
    }

}
