package com.devexed.dalwit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class Query {

    public static Builder builder(String sql) {
        return new Builder(sql);
    }

    public static Query of(String sql, Map<String, Class<?>> types) {
        return new Query(sql, types);
    }

    public static Query of(String sql) {
        return new Query(sql, new HashMap<>());
    }

    private final String rawSql;
    private final HashMap<String, Class<?>> types;
    private final HashMap<String, int[]> parameterIndices;

    private Query(String sql, Map<String, Class<?>> types) {
        // Convert parameters and columns to lowercase so matching parameters and types is case insensitive
        this.types = new HashMap<>(types.size());
        for (Map.Entry<String, Class<?>> e : types.entrySet()) this.types.put(e.getKey().toLowerCase(), e.getValue());

        // Parse sql for named parameters
        Map<String, List<Integer>> boxedParameterIndices = new HashMap<>();
        rawSql = parseParameterQuery(sql, boxedParameterIndices);

        // Un-box parsed parameter indices for better performance
        parameterIndices = new HashMap<>(boxedParameterIndices.size());

        for (Map.Entry<String, List<Integer>> e : boxedParameterIndices.entrySet()) {
            List<Integer> boxedIndices = e.getValue();
            int[] indices = new int[boxedIndices.size()];

            for (int i = 0; i < indices.length; i++) indices[i] = boxedIndices.get(i);

            parameterIndices.put(e.getKey(), indices);
        }
    }

    public String createSql() {
        return rawSql;
    }

    public int[] indicesOf(String parameter) {
        int[] indices = parameterIndices.get(parameter.toLowerCase());

        if (indices == null) {
            throw new DatabaseException("Parameter " + parameter + " not found in query string");
        }

        return indices;
    }

    public Class<?> typeOf(String name) {
        Class<?> type = types.get(name.toLowerCase());

        if (type == null) {
            throw new DatabaseException("Type of " + name + " has not been specified");
        }

        return type;
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
    private static String parseParameterQuery(String query, Map<String, List<Integer>> parameterIndexes) {
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
                List<Integer> indexes = parameterIndexes.computeIfAbsent(parameter, k -> new ArrayList<>());

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

    public static final class Builder {

        private final String sql;
        private final HashMap<String, Class<?>> types;

        private Builder(String sql) {
            this.sql = sql;
            types = new HashMap<>();
        }

        public Builder declare(String name, Class<?> type) {
            types.put(name, type);
            return this;
        }

        public Builder declareAll(Map<String, Class<?>> types) {
            this.types.putAll(types);
            return this;
        }

        public Query build() {
            return new Query(sql, types);
        }

    }

}
