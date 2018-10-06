package com.devexed.dalwit;

import com.devexed.dalwit.util.ClosingCursor;

import java.util.*;

public final class Query {

    private static Map<String, Class<?>> emptyTypeMap = Collections.emptyMap();
    private static Map<String, Integer> emptyListSizeMap = Collections.emptyMap();

    public static String parameterListIndexer(String parameter, int index) {
        return parameter + '$' + index;
    }

    public static QueryBuilder builder(String sql) {
        return new QueryBuilder(sql);
    }

    public static Query of(String sql) {
        return new Query(sql, emptyTypeMap, emptyListSizeMap, emptyTypeMap, emptyTypeMap, true);
    }

    public static Query of(String sql, Map<String, Class<?>> parameters, Map<String, Class<?>> columns, Map<String, Class<?>> keys) {
        return new Query(sql, parameters, emptyListSizeMap, columns, keys, true);
    }

    public static Query of(String sql, Map<String, Class<?>> parameters, Map<String, Class<?>> columns) {
        return new Query(sql, parameters, emptyListSizeMap, columns, emptyTypeMap, true);
    }

    public static Query of(String sql, Map<String, Class<?>> types) {
        return new Query(sql, types, emptyListSizeMap, types, emptyTypeMap, false);
    }

    private final String rawSql;
    private final Map<String, Class<?>> parameters;
    private final Map<String, Integer> parameterListsSizes;
    private final Map<String, Class<?>> columns;
    private final Map<String, Class<?>> keys;
    private final Map<String, int[]> parameterIndices;

    private Query(String sql, Map<String, Class<?>> parameters, Map<String, Integer> parameterListsSizes, Map<String, Class<?>> columns, Map<String, Class<?>> keys, boolean checkParameters) {
        this.parameterListsSizes = parameterListsSizes;
        this.columns = columns;
        this.keys = keys;

        // Parse sql for named parameters
        Map<String, List<Integer>> boxedParameterIndices = new HashMap<>();
        rawSql = parseParameterQuery(sql, boxedParameterIndices, this.parameterListsSizes);

        // Un-box parsed parameters indices for better performance
        HashMap<String, int[]> mutableParameterIndices = new HashMap<>(boxedParameterIndices.size());

        for (Map.Entry<String, List<Integer>> e : boxedParameterIndices.entrySet()) {
            List<Integer> boxedIndices = e.getValue();
            int[] indices = new int[boxedIndices.size()];

            for (int i = 0; i < indices.length; i++) indices[i] = boxedIndices.get(i);

            mutableParameterIndices.put(e.getKey(), indices);
        }

        this.parameterIndices = Collections.unmodifiableMap(mutableParameterIndices);

        if (checkParameters) {
            // Ensure no parameters are left undefined
            LinkedHashSet<String> missingTypes = new LinkedHashSet<>(mutableParameterIndices.keySet());
            missingTypes.removeAll(parameters.keySet());

            if (!missingTypes.isEmpty()) {
                throw new DatabaseException(String.format(
                        "Parameter%s " + String.join(",", missingTypes) + " must have a type declaration for the query:\n" + sql,
                        missingTypes.size() > 1 ? "s" : ""));
            }

            this.parameters = Collections.unmodifiableMap(parameters);
        } else {
            HashMap<String, Class<?>> mutableParameters = new HashMap<>(parameters.size());

            for (Map.Entry<String, Class<?>> e : parameters.entrySet()) {
                if (parameterIndices.containsKey(e.getKey()))
                    parameters.put(e.getKey(), e.getValue());
            }

            this.parameters = Collections.unmodifiableMap(mutableParameters);
        }
    }

    public String sql() {
        return rawSql;
    }

    public Map<String, Class<?>> columns() {
        return columns;
    }

    public Map<String, Class<?>> parameters() {
        return parameters;
    }

    public Map<String, int[]> parameterIndices() {
        return parameterIndices;
    }

    public Map<String, Integer> parameterListSizes() {
        return parameterListsSizes;
    }

    public Map<String, Class<?>> keys() {
        return keys;
    }

    public ReadonlyStatementBuilder on(ReadonlyDatabase database) {
        return new ReadonlyStatementBuilder(database.prepare(this));
    }

    public StatementBuilder on(Database database) {
        return new StatementBuilder(database.prepare(this));
    }

    public static class QueryBuilder {

        private final String sql;
        private final HashMap<String, Class<?>> parameters;
        private final HashMap<String, Integer> parameterListSizes;
        private final HashMap<String, Class<?>> columns;
        private final HashMap<String, Class<?>> keys;

        private QueryBuilder(String sql) {
            this.sql = sql;
            parameters = new HashMap<>();
            parameterListSizes = new HashMap<>();
            columns = new HashMap<>();
            keys = new HashMap<>();
        }

        private QueryBuilder add(String desc, Map<String, Class<?>> map, String name, Class<?> type) {
            // Ensure columns are not declared multiple times with different types
            name = name.toLowerCase();
            Class<?> t = map.get(name);

            if (t != null && !t.equals(type)) {
                throw new DatabaseException(desc + " " + name + " with type " + t + " was re-declared with different type " + type);
            }

            map.put(name, type);

            return this;
        }

        public QueryBuilder column(String name, Class<?> type) {
            return add("Column", columns, name, type);
        }

        public QueryBuilder columns(Map<String, Class<?>> types) {
            for (Map.Entry<String, Class<?>> e : types.entrySet()) {
                column(e.getKey(), e.getValue());
            }

            return this;
        }

        public QueryBuilder parameter(String name, Class<?> type) {
            return add("Parameter", parameters, name, type);
        }

        public QueryBuilder parameters(Map<String, Class<?>> types) {
            for (Map.Entry<String, Class<?>> e : types.entrySet()) {
                parameter(e.getKey(), e.getValue());
            }

            return this;
        }

        public QueryBuilder parameter(String name, Class<?> type, int size) {
            if (size <= 0) {
                throw new DatabaseException("List parameters size must be one or greater");
            }

            parameterListSizes.put(name, size);

            for (int i = 0; i < size; i++) {
                parameter(parameterListIndexer(name, i), type);
            }

            return this;
        }

        public QueryBuilder key(String name, Class<?> type) {
            return add("Key", keys, name, type);
        }

        public QueryBuilder keys(Map<String, Class<?>> types) {
            for (Map.Entry<String, Class<?>> e : types.entrySet()) {
                key(e.getKey(), e.getValue());
            }

            return this;
        }

        public Query build() {
            return new Query(
                    sql,
                    Collections.unmodifiableMap(parameters),
                    Collections.unmodifiableMap(parameterListSizes),
                    Collections.unmodifiableMap(columns),
                    Collections.unmodifiableMap(keys),
                    true);
        }

    }

    public static class ReadonlyStatementBuilder {

        protected final ReadonlyStatement statement;

        private ReadonlyStatementBuilder(ReadonlyStatement statement) {
            this.statement = statement;
        }

        public <T> ReadonlyStatementBuilder bind(String name, T value) {
            try {
                statement.bind(name, value);
            } catch (DatabaseException e) {
                statement.close();
                throw e;
            }

            return this;
        }

        public Cursor query() {
            return new ClosingCursor(statement, statement.query());
        }

    }

    public static class StatementBuilder extends ReadonlyStatementBuilder {

        private StatementBuilder(Statement statement) {
            super(statement);
        }

        public void execute() {
            try {
                ((Statement) statement).execute();
            } finally {
                statement.close();
            }
        }

        public long update() {
            try {
                return ((Statement) statement).update();
            } finally {
                statement.close();
            }
        }

        public Cursor insert() {
            return new ClosingCursor(statement, ((Statement) statement).insert());
        }

    }

    /**
     * <p>Parse a query for named parameters named in the form of a colon (:) followed by
     * a java identifier and insert a ? at these occurrences.
     * Additionally map the replaced occurrences to unique sequential indexes starting
     * at zero and store the result in the provided parameters map.</p>
     * <p/>
     * <p>For example <code>SELECT name FROM person WHERE name = :name AND (mother_surname = :surname OR father_surname
     * = :surname)</code> becomes <code>SELECT name FROM person WHERE name = ? AND (mother_surname = ? OR father_surname
     * = ?)</code> and the parameters index map will contain the values <code>{"name": [0], "surname": [1, 2]}</code></p>
     *
     * @param query            The query to parse.
     * @param parameterIndexes The map which to fill with parameters indexes.
     * @return The query with the named parameters replaced with ?.
     */
    private static String parseParameterQuery(String query, Map<String, List<Integer>> parameterIndexes, Map<String, Integer> listParameters) {
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

                // Add parameter to parameters indexes map and substitute it with a ? in the resulting query.
                String parameter = parameterBuilder.toString();
                Integer listParameterSize = listParameters.get(parameter);

                if (listParameterSize != null) {
                    queryBuilder.append('(');
                    parameterIndexes
                            .computeIfAbsent(parameterListIndexer(parameter, 0), k -> new ArrayList<>())
                            .add(parameterIndex);
                    parameterIndex++;
                    queryBuilder.append('?');

                    for (int p = 1; p < listParameterSize; p++) {
                        queryBuilder.append(",?");
                        parameterIndexes
                                .computeIfAbsent(parameterListIndexer(parameter, p), k -> new ArrayList<>())
                                .add(parameterIndex);
                        parameterIndex++;
                    }

                    queryBuilder.append(')');
                } else {
                    parameterIndexes.computeIfAbsent(parameter, k -> new ArrayList<>()).add(parameterIndex);
                    parameterIndex++;
                    queryBuilder.append('?');
                }

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

}
