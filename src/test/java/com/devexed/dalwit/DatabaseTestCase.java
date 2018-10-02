package com.devexed.dalwit;

import com.devexed.dalwit.util.ObjectDescriptor;
import com.devexed.dalwit.util.ObjectIterable;
import com.devexed.dalwit.util.Statements;
import junit.framework.TestCase;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;

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
        Query createTable = Query.of("CREATE TABLE t2 (a TEXT NOT NULL)");
        Query insertQuery = Query.builder("INSERT INTO t2 (a) VALUES (:a)").declare("a", String.class).build();
        Query selectQuery = Query.builder("SELECT a FROM t2").declare("a", String.class).build();

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
        Query createTable = Query.of("CREATE TABLE t3 (a VARCHAR(50) NULL)");
        Query insertQuery = Query.builder("INSERT INTO t3 (a) VALUES (:a)").declare("a", String.class).build();
        Query selectQuery = Query.builder("SELECT a FROM t3").declare("a", String.class).build();

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
        Statements.execute(db, Query.of("CREATE TABLE t6 (a VARCHAR(50) NULL)"));
        Query insertQuery = Query.builder("INSERT INTO t6 (a) VALUES (:a)").declare("a", String.class).build();
        Query selectQuery = Query.builder("SELECT a FROM t6").declare("a", String.class).build();

        int transactionDepth = 15;
        ArrayList<Transaction> transactions = new ArrayList<>(transactionDepth);
        Transaction transaction = db.transact();
        transactions.add(transaction);

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

    public void testBinderAndGetter() {
        int count = 1000;
        Statements.execute(db, Query.of("CREATE TABLE t7 (a INTEGER)"));

        // Insert multiple values with the same binder
        Query insertQuery = Query
                .builder("INSERT INTO t7 (a) VALUES (:a)")
                .declare("a", Integer.TYPE)
                .build();

        try (Transaction transaction = db.transact();
             ExecutionStatement statement = transaction.createExecution(insertQuery)) {
            Statement.Binder<Integer> binder = statement.binder("a");

            for (int i = 0; i < count; i++) {
                binder.bind(i);
                statement.execute();
            }

            transaction.commit();
        }

        // Verify the values with same getter
        Query selectQuery = Query
                .builder("SELECT a FROM t7 ORDER BY a")
                .declare("a", Integer.class)
                .build();

        try (QueryStatement statement = db.createQuery(selectQuery);
             Cursor cursor = statement.query()) {
            Cursor.Getter<Integer> getter = cursor.getter("a");
            int i = 0;

            while (cursor.next()) {
                assertEquals((int) getter.get(), i);
                i++;
            }

            assertEquals(i, count);
        }
    }

    public static final class ObjectDescriptorTest {
        public int a;
        public String b;
        public byte[] c;

        public ObjectDescriptorTest() {
        }

        public ObjectDescriptorTest(int a, String b, byte[] c) {
            this.a = a;
            this.b = b;
            this.c = c;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ObjectDescriptorTest that = (ObjectDescriptorTest) o;
            return a == that.a &&
                    Objects.equals(b, that.b) &&
                    Arrays.equals(c, that.c);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(a, b);
            result = 31 * result + Arrays.hashCode(c);
            return result;
        }

        @Override
        public String toString() {
            return "ObjectDescriptorTest{" +
                    "a=" + a +
                    ", b='" + b + '\'' +
                    ", c=" + Arrays.toString(c) +
                    '}';
        }
    }

    public void testObjectMapper() {
        ObjectDescriptor<ObjectDescriptorTest> objectDescriptor = ObjectDescriptor.of(ObjectDescriptorTest.class);
        ArrayList<ObjectDescriptorTest> objects = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            objects.add(new ObjectDescriptorTest(i, "_" + i, new byte[] {(byte) i, (byte) (i << 1)}));
        }

        // Insert all objects then select and make sure they are identical
        Statements.execute(db, Query.of("CREATE TABLE t8 (a INTEGER, b TEXT, c BLOB)"));
        Query insertQuery = objectDescriptor.sql("INSERT INTO t8 (a, b, c) VALUES (:a, :b, :c)");
        Query selectQuery = objectDescriptor.sql("SELECT * FROM t8 ORDER BY a");

        try (Transaction transaction = db.transact();
             ExecutionStatement statement = transaction.createExecution(insertQuery)) {
            objectDescriptor.bindAll(statement, objects);
            transaction.commit();
        }

        try (ObjectIterable<ObjectDescriptorTest> selectedObjects = objectDescriptor.iterate(Statements.query(db, selectQuery))) {
            int i = 0;

            for (ObjectDescriptorTest object : selectedObjects) {
                assertEquals(object, objects.get(i));
                i++;
            }

            assertEquals(objects.size(), i);
        }
    }

    public void testListParameter() {
        // Insert all objects then select and make sure they are identical
        Statements.execute(db, Query.of("CREATE TABLE t9 (a INTEGER)"));
        Statements.execute(db, Query.of("INSERT INTO t9 (a) VALUES (1), (2), (3), (4), (5)"));

        try (QueryStatement selectStatement = db.createQuery(Query
                .builder("SELECT * FROM t9 WHERE a in :as")
                .declare("a", Integer.TYPE)
                .declare("as", Integer.TYPE, 3)
                .build())) {
            int[] checks = new int[] {1, 3, 5};
            selectStatement.bind("as", checks);

            try (Cursor cursor = selectStatement.query()) {
                HashSet<Integer> results = new HashSet<>();

                while (cursor.next()) {
                    results.add(cursor.get("a"));
                }

                for (int check : checks) assertTrue(results.contains(check));
            }
        }
    }

    public void testSnakeCaseColumn() {
        // Insert all objects then select and make sure they are identical
        Statements.execute(db, Query.of("CREATE TABLE t10 (a_a INTEGER)"));
        Statements.execute(db, Query.of("INSERT INTO t10 (a_a) VALUES (1)"));

        try (QueryStatement selectStatement = db.createQuery(Query
                .builder("SELECT * FROM t10")
                .declare("aA", Integer.TYPE)
                .build())) {
            try (Cursor cursor = selectStatement.query()) {
                assertTrue(cursor.next());
                assertEquals((int) cursor.get("aA"), 1);
            }
        }
    }

}
