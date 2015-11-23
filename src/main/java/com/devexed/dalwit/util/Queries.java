package com.devexed.dalwit.util;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Driver;
import com.devexed.dalwit.DriverMatcher;
import com.devexed.dalwit.Query;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Helpers for building database engine independent queries.
 */
public final class Queries {

    // Hidden constructor
    private Queries() {}

    /**
     * Builds a potentially complex query to handle multiple database types and versions.
     *
     * @return A query builder.
     */
    public static QueryBuilder builder() {
        return new QueryBuilder();
    }

    /**
     * Creates a simple query with the same SQL for all database types and versions.
     *
     * @param sql   The SQL of the query.
     * @param types The types of the selected columns and parameters in the query.
     * @return A simple query.
     */
    public static Query of(final String sql, final Map<String, ? extends Class<?>> types) {
        final Map<String, ArrayList<Integer>> queryParameterIndexes = new HashMap<String, ArrayList<Integer>>();
        final String querySql = parseParameterQuery(sql, queryParameterIndexes);

        return new Query() {

            @Override
            public String create(Driver driver, Map<String, ArrayList<Integer>> parameterIndexes) {
                parameterIndexes.putAll(queryParameterIndexes);
                return querySql;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Class<T> typeOf(String name) {
                return (Class<T>) types.get(name);
            }

        };
    }

    /**
     * Creates a simple query with the same SQL for all database types and versions and with no parameters or columns.
     *
     * @param sql The SQL of the query.
     * @return A simple query.
     */
    public static Query of(final String sql) {
        return of(sql, Collections.<String, Class<?>>emptyMap());
    }

    /**
     * Concatenate multiple queries into one single query, including the types and parameters of all the concatenated.
     *
     * @param queries The queries to concatenate.
     * @return The concatenated query.
     */
    public static Query concat(final Iterable<Query> queries) {
        return new ConcatenatedQuery(queries) {

            @Override
            public String create(Driver driver, Map<String, ArrayList<Integer>> parameterIndexes) {
                StringBuilder queryBuilder = new StringBuilder();

                for (Query query : queries) queryBuilder.append(query.create(driver, parameterIndexes));

                return queryBuilder.toString();
            }

        };
    }

    public static Query concat(Query... queries) {
        return concat(new ArrayIterable<Query>(queries));
    }

    /**
     * Includes the created SQL from the argument queries in the specified query at %s in the query using
     * {@link String#format}.
     *
     * @param query The query to format.
     * @param args  The queries to include.
     * @return The formatted query.
     */
    public static Query format(final Query query, final Iterable<Query> args) {
        return new ConcatenatedQuery(new ArrayList<Query>() {{
            add(query);
            for (Query arg : args) add(arg);
        }}) {

            @Override
            public String create(Driver driver, Map<String, ArrayList<Integer>> parameterIndexes) {
                ArrayList<String> stringArgList = new ArrayList<String>();

                for (Query arg : args) stringArgList.add(arg.create(driver, parameterIndexes));

                String[] stringArgs = new String[stringArgList.size()];
                stringArgList.toArray(stringArgs);

                return String.format(query.create(driver, parameterIndexes), (Object[]) stringArgs);
            }

        };
    }

    public static Query format(Query query, Query... args) {
        return format(query, new ArrayIterable<Query>(args));
    }

    /**
     * <p>Parse a query for named parameter named in the form of a colon (:) followed by
     * a java identifier and insert a ? at these occurrences.
     * Additionally map the replaced occurrences to unique sequential indexes starting
     * at zero and store the result in the provided parameter map.</p>
     * <p/>
     * <p>For example <code>SELECT name FROM person WHERE name = :name AND (mother_surname = :surname OR father_surname
     * = :surname)</code> becomes <code>SELECT name FROM person WHERE name = ? AND (mother_surname = ? OR father_surname
     * = ?)</code> and the parameter index map will contain the values <code>{"name": [0], "surname": [1, 2]}</code></p>
     *
     * @param query            The query to parse.
     * @param parameterIndexes The map which to fill with parameter indexes.
     * @return The query with the named parameters replaced with ?.
     */
    public static String parseParameterQuery(String query, Map<String, ArrayList<Integer>> parameterIndexes) {
        StringBuilder queryBuilder = new StringBuilder();
        StringBuilder parameterBuilder = new StringBuilder();
        int parameterIndex = 0;

        /* Various ranges where parameters aren't parsed. Handling escaped characters inside the ranges is unnecessary
           because SQL handles escaping by doubling the character. The parser will simply immediately begin a new range
           again after closing the previous when a doubled range end is encountered. For example 'abc''def' will be
           understood as two separate strings. */
        EscapedRange[] escapedRanges = new EscapedRange[]{
                new EscapedRange("--", "\n"),
                new EscapedRange("'"),
                new EscapedRange("\""),
                new EscapedRange("[", "]"),
                new EscapedRange("`")
        };

        queryLoop:
        for (int i = 0, l = query.length(); i < l; ) {
            // Check for escaped ranges in which query parameters can't appear.
            for (EscapedRange range : escapedRanges) {
                if (range.inRange) {
                    // Is this the end of an escaped range?
                    int length = range.end.length();
                    int endIndex = i + range.end.length();

                    if (endIndex <= l && query.substring(i, endIndex).equals(range.end)) {
                        range.inRange = false;
                        queryBuilder.append(range.end);
                        i += length;
                    } else {
                        queryBuilder.append(query.charAt(i));
                        i++;
                    }

                    continue queryLoop;
                } else {
                    // Is this the start of an escaped range?
                    int length = range.start.length();
                    int endIndex = i + length;

                    if (endIndex <= l && query.substring(i, endIndex).equals(range.start)) {
                        range.inRange = true;
                        queryBuilder.append(range.start);
                        i += length;

                        continue queryLoop;
                    }
                }
            }

            char c = query.charAt(i);

            if (c == '?')
                throw new IllegalArgumentException("Illegal character '?'. Only named parameters are allowed.");

            if (c == ':') {
                i++;

                // Empty parameter at end.
                if (i == l)
                    throw new IllegalArgumentException("Empty parameter at query end.");

                char ps = query.charAt(i);

                // Illegal parameter start.
                if (!Character.isJavaIdentifierStart(ps))
                    throw new IllegalArgumentException("Character " + ps + " is not a valid parameter (character position " + i + ").");

                // Build parameter string.
                parameterBuilder.setLength(0);
                parameterBuilder.append(ps);
                i++;

                for (; i < l; i++) {
                    char p = query.charAt(i);
                    if (!Character.isJavaIdentifierPart(query.charAt(i))) break;
                    parameterBuilder.append(p);
                }

                // Add parameter to parameter indexes map and substitute it with a ? in the resulting query.
                String parameter = parameterBuilder.toString();
                ArrayList<Integer> indexes = parameterIndexes.get(parameter);

                if (indexes == null) {
                    indexes = new ArrayList<Integer>();
                    parameterIndexes.put(parameter, indexes);
                }

                indexes.add(parameterIndex);
                parameterIndex++;
                queryBuilder.append('?');

                continue;
            }

            queryBuilder.append(c);
            i++;
        }

        return queryBuilder.toString();
    }

    private static final class EscapedRange {
        final String start;
        final String end;
        boolean inRange;

        EscapedRange(String start, String end) {
            this.start = start;
            this.end = end;
        }

        EscapedRange(String startAndEnd) {
            this(startAndEnd, startAndEnd);
        }
    }

    /**
     * A builder for complex queries.
     */
    public static final class QueryBuilder {

        private final ArrayList<PermutationQuery.Permutation> permutations =
                new ArrayList<PermutationQuery.Permutation>();

        // Hidden constructor
        private QueryBuilder() {
        }

        /**
         * Add a query permutation only applicable to a certain database driver.
         *
         * @param matcher The driver matcher which when matched the supplied query is used.
         * @param query   The query to use for this permutation.
         */
        public QueryBuilder matcher(DriverMatcher matcher, String query) {
            if (matcher == null) throw new NullPointerException("Matcher can't be null");
            if (query == null) throw new NullPointerException("Query can't be null");

            permutations.add(new PermutationQuery.Permutation(matcher, query));

            return this;
        }

        public QueryBuilder type(String type, String query) {
            return matcher(DriverMatchers.forType(type), query);
        }

        public QueryBuilder patternVersion(String type, Pattern version, String query) {
            return matcher(DriverMatchers.forPatternVersion(type, version), query);
        }

        public QueryBuilder minimumVersion(String type, int[] version, String query) {
            return matcher(DriverMatchers.forMinimumVersion(type, version), query);
        }

        public QueryBuilder any(String query) {
            return matcher(DriverMatchers.forAny(), query);
        }

        /**
         * Build the query with selected columns and parameters types.
         *
         * @param types The types of the selected columns and parameters in the query.
         */
        public Query build(final Map<String, ? extends Class<?>> types) {
            return new PermutationQuery(permutations, types);
        }

        /**
         * Build the query with no selected columns or parameters.
         */
        public Query build() {
            return build(Collections.<String, Class<?>>emptyMap());
        }

    }

    private static class PermutationQuery implements Query {

        private final ArrayList<Permutation> permutations;
        private final Map<String, ? extends Class<?>> types;
        public PermutationQuery(ArrayList<Permutation> permutations, Map<String, ? extends Class<?>> types) {
            this.permutations = permutations;
            this.types = types;
        }

        @Override
        public String create(Driver driver, Map<String, ArrayList<Integer>> parameterIndexes) {
            for (Permutation permutation : permutations) {
                if (permutation.matcher.matches(driver)) return permutation.query;
            }

            throw new DatabaseException("No applicable query permutation found for database driver " + driver);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Class<T> typeOf(String name) {
            return (Class<T>) types.get(name);
        }

        private static final class Permutation {

            final DriverMatcher matcher;
            final String query;

            Permutation(DriverMatcher matcher, String query) {
                this.matcher = matcher;
                this.query = query;
            }

        }

    }

    private static abstract class ConcatenatedQuery implements Query {

        private final Iterable<Query> queries;

        private ConcatenatedQuery(Iterable<Query> queries) {
            this.queries = queries;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> Class<T> typeOf(String name) {
            Set<Class<?>> types = new HashSet<Class<?>>();

            for (Query query : queries) {
                Class<?> type = query.typeOf(name);

                if (type != null) types.add(type);
            }

            if (types.isEmpty()) return null;
            else if (types.size() > 0)
                throw new DatabaseException("Multiple different types are defined for parameter with name " + name +
                        ": " + types);

            return (Class<T>) types.iterator().next();
        }

    }

    private static final class ArrayIterable<E> implements Iterable<E> {

        private final E[] array;

        private ArrayIterable(E[] array) {
            this.array = array;
        }

        @Override
        public Iterator<E> iterator() {
            return new ArrayIterator<E>(array);
        }

    }

    private static final class ArrayIterator<E> implements Iterator<E> {

        private final E[] array;
        private int position = 0;

        private ArrayIterator(E[] array) {
            this.array = array;
        }

        @Override
        public boolean hasNext() {
            return position < array.length;
        }

        @Override
        public E next() {
            return array[position];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

}
