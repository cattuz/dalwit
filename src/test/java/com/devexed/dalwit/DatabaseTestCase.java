package com.devexed.dalwit;

import com.devexed.dalwit.util.Queries;
import junit.framework.TestCase;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Note: Tests written in JUNIT 3 style for Android compatibility.
 */
public abstract class DatabaseTestCase extends TestCase {

    private Connection connection;
    private Database db;

    public abstract Connection createConnection();

    public abstract void destroyConnection();

    private void reopenDatabase() {
        db.close();
        db = connection.write();
    }

    @Override
    public void setUp() throws Exception {
        connection = createConnection();
        db = connection.write();
    }

    @Override
    protected void tearDown() throws Exception {
        db.close();
        destroyConnection();
        super.tearDown();
    }

    public void testQueryConcat() {
        Query a = Queries.of("SELECT :a FROM t ");
        Query b = Queries.of("WHERE :b");
        Query concat = Queries.concat(a, b);
        final HashMap<String, List<Integer>> parameters = new HashMap<String, List<Integer>>();
        final HashMap<Integer, String> indexes = new HashMap<Integer, String>();

        assertEquals(concat.create(db, parameters, indexes), "SELECT ? FROM t WHERE ?");
        assertEquals(parameters, new HashMap<String, List<Integer>>() {{
            for (int i = 0; i < indexes.size(); i++) put(indexes.get(i), Collections.singletonList(i));
        }});
    }

    public void testQueryConcatComplex() {
        String table = "t";
        String idColumn = "id";
        String nameColumn = "name";
        String typeColumn = "type";
        String selectPrefix = "SELECT " +
                idColumn + "," +
                nameColumn + "," +
                typeColumn;

        String joinSql = " INNER JOIN %s ON %s = " + idColumn; // %s = field id column
        String orderSql = " ORDER BY %s";
        String whereSql = " WHERE %s";


        Query a = Queries.of(selectPrefix + " FROM " + table + joinSql + whereSql + orderSql);
        Query concat = Queries.format(a,
                Queries.of(table),
                Queries.of(table + "." + "fieldId"),
                Queries.of("configId" + " = :" + "configId"),
                Queries.of("position"));

        final HashMap<String, List<Integer>> parameters = new HashMap<String, List<Integer>>();
        final HashMap<Integer, String> indexes = new HashMap<Integer, String>();

        concat.create(db, parameters, indexes);

        assertEquals(parameters, new HashMap<String, List<Integer>>() {{
            for (int i = 0; i < indexes.size(); i++) put(indexes.get(i), Collections.singletonList(i));
        }});
    }

    public void testQueryFormat() {
        Query a = Queries.of("SELECT :a FROM t %s");
        Query b = Queries.of("WHERE :b");
        Query format = Queries.format(a, b);
        final HashMap<String, List<Integer>> parameters = new HashMap<String, List<Integer>>();
        final HashMap<Integer, String> indexes = new HashMap<Integer, String>();

        assertEquals(format.create(db, parameters, indexes), "SELECT ? FROM t WHERE ?");
        assertEquals(parameters, new HashMap<String, List<Integer>>() {{
            put(indexes.get(0), Collections.singletonList(0));
            put(indexes.get(1), Collections.singletonList(1));
        }});
    }

    public void testIgnoresParameterInSQLGroups() {
        Query insertQuery = Queries.of("INSERT INTO q (a AS ':b', a AS [:c]) VALUES (:a, \":d\")");
        HashMap<String, List<Integer>> parameters = new HashMap<String, List<Integer>>();
        HashMap<Integer, String> indexes = new HashMap<Integer, String>();
        insertQuery.create(db, parameters, indexes);

        assertTrue(parameters.containsKey("a"));  // Should have been parsed as parameter
        assertTrue(!parameters.containsKey("b")); // Should NOT have been parsed as parameter
        assertTrue(!parameters.containsKey("c")); // -"-
        assertTrue(!parameters.containsKey("d")); // -"-
    }

    public void testBindsTypedQueryParameter() {
        Map<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("a", Integer.class);
        }};
        Query createTable = Queries.of("CREATE TABLE q2 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO q2 (a) VALUES (:a)", columnTypes);

        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        UpdateStatement queryStatement = transaction.createUpdate(insertQuery);
        queryStatement.bind("a", 123);
        transaction.commit();
        transaction.close();
    }

    public void testBindWrongQueryParameterTypeThrows() {
        Map<String, Class<?>> columnTypes = new HashMap<String, Class<?>>() {{
            put("a", Integer.class);
        }};
        Query createTable = Queries.of("CREATE TABLE q3 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO q3 (a) VALUES (:a)", columnTypes);

        Transaction transaction = db.transact();
        transaction.createExecution(createTable).execute();
        UpdateStatement queryStatement = transaction.createUpdate(insertQuery);

        try {
            queryStatement.bind("a", "test");
            fail("Succeed binding wrong type to parameter");
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
        Map<String, Class<?>> columnTypes = Collections.<String, Class<?>>singletonMap("a", String.class);
        Query createTable = Queries.of("CREATE TABLE t1 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO t1 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT a FROM t1", columnTypes);

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
        Map<String, Class<?>> columnTypes = Collections.<String, Class<?>>singletonMap("a", String.class);
        Query createTable = Queries.of("CREATE TABLE t2 (a TEXT NOT NULL)");
        Query insertQuery = Queries.of("INSERT INTO t2 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT a FROM t2", columnTypes);

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
        Map<String, Class<?>> columnTypes = Collections.<String, Class<?>>singletonMap("a", String.class);
        Query createTable = Queries.of("CREATE TABLE t3 (a VARCHAR(50) NULL)");
        Query insertQuery = Queries.of("INSERT INTO t3 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT a FROM t3", columnTypes);

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

        // Ensure only committed child transaction was stored.
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

    public void testRollbackWithDanglingTransactionThrows() {
        try {
            Transaction transaction = db.transact();
            transaction.transact(); // Dangle transaction
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

    public void testInsertWithGeneratedKeys() {
        Map<String, Class<?>> keys = Collections.<String, Class<?>>singletonMap("id", Long.TYPE);
        HashMap<String, Class<?>> columnTypes = new HashMap<String, Class<?>>();
        columnTypes.putAll(keys);
        columnTypes.put("a", String.class);
        Query createTable = Queries.builder()
                .type("H2", "CREATE TABLE t4 (id BIGINT PRIMARY KEY AUTO_INCREMENT, a VARCHAR(50) NOT NULL)")
                .type("SQLite", "CREATE TABLE t4 (id INTEGER PRIMARY KEY, a VARCHAR(50) NOT NULL)")
                .build();
        Query insertQuery = Queries.of("INSERT INTO t4 (a) VALUES (:a)", columnTypes);
        Query selectQuery = Queries.of("SELECT id FROM t4", columnTypes);

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
        assertEquals((long) cursor.<Long>get("id"), insertedKey);
        assertFalse(cursor.next());
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

}
