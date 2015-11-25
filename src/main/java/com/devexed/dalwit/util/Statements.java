package com.devexed.dalwit.util;

import com.devexed.dalwit.*;

import java.util.Iterator;
import java.util.Map;

/**
 * Helper class for statements.
 */
public final class Statements {

    private Statements() {
    }

    public static CloseableCursor query(ReadonlyDatabase database, QueryStatement statement) {
        Cursor cursor = null;

        try {
            cursor = statement.query(database);
            return Cursors.closeable(statement, cursor);
        } catch (DatabaseException e){
            statement.close(cursor);
            throw e;
        }
    }

    public static CloseableCursor query(final ReadonlyDatabase database, Query query) {
        QueryStatement statement = null;

        try {
            statement = database.createQuery(query);
            final QueryStatement finalStatement = statement;
            return new AbstractCloseableCursor(query(database, statement)) {
                @Override
                public void close() {
                    database.close(finalStatement);
                }
            };
        } catch (DatabaseException e) {
            database.close(statement);
            throw e;
        }
    }

    public static CloseableCursor insert(final Database database, final InsertStatement statement) {
        Transaction transaction = null;
        Cursor cursor = null;

        try {
            transaction = database.transact();
            cursor = statement.insert(transaction);
            final Transaction finalTransaction = transaction;
            final Cursor finalCursor = cursor;

            return new AbstractCloseableCursor(cursor) {

                @Override
                public void close() {
                    statement.close(finalCursor);
                    database.commit(finalTransaction);
                }

            };
        } catch (DatabaseException e) {
            statement.close(cursor);
            database.rollback(transaction);
            throw e;
        }
    }

    public static CloseableCursor insert(final Database database, Query insert, Map<String, Class<?>> keys) {
        InsertStatement statement = null;

        try {
            statement = database.createInsert(insert, keys);
            final InsertStatement finalStatement = statement;
            return new AbstractCloseableCursor(insert(database, statement)) {
                @Override
                public void close() {
                    database.close(finalStatement);
                }
            };
        } catch (DatabaseException e) {
            database.close(statement);
            throw e;
        }
    }

    public static long update(Database database, UpdateStatement statement) {
        ClosableTransaction transaction = null;

        try {
            transaction = Databases.transact(database);
            return statement.update(transaction);
        } finally {
            if (transaction != null) transaction.close();
        }
    }

    public static long update(Database database, Query update) {
        UpdateStatement statement = null;

        try {
            statement = database.createUpdate(update);
            return update(database, statement);
        } finally {
            database.close(statement);
        }
    }

    public static void execute(Database database, final ExecutionStatement statement) {
        ClosableTransaction transaction = null;

        try {
            transaction = Databases.transact(database);
            statement.execute(transaction);
        } finally {
            if (transaction != null) transaction.close();
        }
    }

    public static void execute(Database database, Query execution) {
        ExecutionStatement statement = null;

        try {
            statement = database.createExecution(execution);
            execute(database, statement);
        } finally {
            database.close(statement);
        }
    }

    /**
     * Bind all parameters in a map to a {@link Statement}.
     *
     * @param statement  The statement to which to bind the parameter.
     * @param parameters A map of the parameters and their values.
     */
    public static <E> void bindAll(Statement statement, Map<String, E> parameters) {
        for (Map.Entry<String, E> e : parameters.entrySet())
            statement.bind(e.getKey(), e.getValue());
    }

    /**
     * <p>Build an list expression from any iterable collection of values. Given a collection of values this function
     * will build a string of a list of comma separated parameters and collects those bound parameters in a map that can
     * be bound to a statement (e.g. using the {@link #bindAll} function.</p>
     * <p/>
     * <p>For example, after...</p>
     * <pre><code>
     * Map&lt;String, Integer&gt; mappedItems = new HashMap&lt;&gt;();
     * StringBuilder queryExpression = new StringBuilder();
     * queryExpression.append("SELECT name, index FROM items WHERE index IN (");
     * buildListExpression(new Integer[] {123, 456, 789}, "item_", mappedItems);
     * queryExpression.append(")");
     * </code></pre>
     * <p>... <code>mappedItems</code> will contain...</p>
     * <pre><code>
     * {"item_1": 123, "item_2": 456, "item_3": 789}
     * </code></pre>
     * <p>... and <code>queryExpression</code> will contain...</p>
     * <pre><code>
     * SELECT name, index FROM items WHERE index IN (:item_1,:item_2,:item_3)
     * </code></pre>
     *
     * @param values            The iterable collection of values.
     * @param parameterPrefix   The prefix of the parameters in the list expresssion.
     * @param parametersBuilder The parameters builder which will contain the mapping of parameters to values.
     * @param stringBuilder     The builder of the string which will contain the joined list of parameters.
     */
    public static <E> void buildListExpression(Iterable<E> values, String parameterPrefix,
                                               Map<String, E> parametersBuilder, StringBuilder stringBuilder) {
        Iterator<E> it = values.iterator();

        if (it.hasNext()) {
            int index = 0;
            String p0 = parameterPrefix + index;
            index++;
            parametersBuilder.put(p0, it.next());
            stringBuilder.append(":").append(p0);

            while (it.hasNext()) {
                String p = parameterPrefix + index;
                index++;
                parametersBuilder.put(p, it.next());
                stringBuilder.append(",:").append(p);
            }
        }
    }

    /**
     * Convenience method which builds and returns the joined parameter list string for you.
     *
     * @return The string build by {@link #buildListExpression(Iterable, String, Map, StringBuilder)}
     * @see #buildListExpression(Iterable, String, Map, StringBuilder)
     */
    public static <E> String buildListExpression(Iterable<E> values, String parameterPrefix,
                                                 Map<String, E> parametersBuilder) {
        StringBuilder builder = new StringBuilder();
        buildListExpression(values, parameterPrefix, parametersBuilder, builder);
        return builder.toString();
    }

}
