package com.devexed.dalwit.util;

import com.devexed.dalwit.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Helper class for statements.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public final class Statements {

    private Statements() {
    }

    public static Cursor query(ReadonlyDatabase database, Query query) {
        return query(database, query, Collections.emptyMap());
    }

    public static Cursor query(ReadonlyDatabase database, Query query, Map<String, ?> bindings) {
        QueryStatement statement = null;

        try {
            statement = database.createQuery(query);
            bindAll(statement, bindings);
            return new ClosingCursor(statement, statement.query());
        } catch (RuntimeException e) {
            if (statement != null) statement.close();
            throw e;
        }
    }

    public static Cursor insert(Database database, Query insert, Map<String, Class<?>> keys) {
        return insert(database, insert, keys, Collections.emptyMap());
    }

    public static Cursor insert(Database database, Query insert, Map<String, Class<?>> keys, Map<String, ?> bindings) {
        Transaction transaction = null;
        InsertStatement statement = null;

        try {
            transaction = database.transact();
            statement = transaction.createInsert(insert, keys);
            bindAll(statement, bindings);
            return new CommittingCursor(transaction, statement, statement.insert());
        } catch (RuntimeException e) {
            if (statement != null) statement.close();
            if (transaction != null) transaction.close();
            throw e;
        }
    }

    public static long update(Database database, Query update) {
        return update(database, update, Collections.emptyMap());
    }

    public static long update(Database database, Query update, Map<String, ?> bindings) {
        try (Transaction transaction = database.transact(); UpdateStatement statement = transaction.createUpdate(update)) {
            bindAll(statement, bindings);
            long count = statement.update();
            transaction.commit();
            return count;
        }
    }

    public static void execute(Database database, Query execution) {
        execute(database, execution, Collections.emptyMap());
    }

    public static void execute(Database database, Query execution, Map<String, ?> bindings) {
        try (Transaction transaction = database.transact(); ExecutionStatement statement = transaction.createExecution(execution)) {
            bindAll(statement, bindings);
            statement.execute();
            transaction.commit();
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
     * @param parameterPrefix   The prefix of the parameters in the list expression.
     * @param parametersBuilder The parameters builder which will contain the mapping of parameters to values.
     * @param stringBuilder     The builder of the string which will contain the joined list of parameters.
     */
    public static <E> void buildListExpression(Iterable<E> values,
                                               String parameterPrefix,
                                               Map<String, E> parametersBuilder,
                                               StringBuilder stringBuilder) {
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
