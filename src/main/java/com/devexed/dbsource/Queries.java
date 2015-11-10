package com.devexed.dbsource;

import java.util.*;
import java.util.regex.Pattern;

/** Helpers for building database engine independent queries. */
public final class Queries {

    private static abstract class QueryPermutation {

        final String query;

        QueryPermutation(String query) {
            this.query = query;
        }

        final String getQuery() {
            return query;
        }

        abstract boolean matchesDatabase(Database database);

    }

    private static class TypeQueryPermutation extends QueryPermutation {

        final String type;

        private TypeQueryPermutation(String type, String query) {
            super(query);
            this.type = type;
        }

        @Override
        boolean matchesDatabase(Database database) {
            return type.equals(database.getType());
        }

    }

    private static final class PatternQueryPermutation extends TypeQueryPermutation {

        final Pattern version;

        private PatternQueryPermutation(String type, Pattern version, String query) {
            super(type, query);
            this.version = version;
        }

        @Override
        public boolean matchesDatabase(Database database) {
            return super.matchesDatabase(database) && version.matcher(database.getVersion()).find();

        }

    }

    private static final class MinimumVersionQueryPermutation extends TypeQueryPermutation {

        final int[] version;

        private MinimumVersionQueryPermutation(String type, int[] version, String query) {
            super(type, query);
            this.version = version;
        }

        @Override
        public boolean matchesDatabase(Database database) {
            if (!super.matchesDatabase(database)) return false;

            int versionIndex = 0;

            for (String part: database.getType().split("\\.")) {
                if (versionIndex >= version.length) break;

                try {
                    if (Integer.parseInt(part) < version[versionIndex]) return false;
                } catch (Exception e) {
                    return false;
                }
            }

            return true;
        }

    }

    public static final class QueryBuilder {

        private final ArrayList<QueryPermutation> permutations = new ArrayList<>();
        private String defaultQuery = null;

        private QueryBuilder() {}

        public QueryBuilder forVersion(String type, Pattern versionPattern, String query) {
            permutations.add(new PatternQueryPermutation(type, versionPattern, query));
            return this;
        }

        public QueryBuilder forVersion(String type, int[] minimumVersion, String query) {
            permutations.add(new MinimumVersionQueryPermutation(type, minimumVersion, query));
            return this;
        }

        public QueryBuilder forType(String type, String query) {
            permutations.add(new TypeQueryPermutation(type, query));
            return this;
        }

        public QueryBuilder forDefault(String query) {
            defaultQuery = query;
            return this;
        }

        public Query build() {
            return build(Collections.<String, Class<?>>emptyMap());
        }

        public Query build(final Map<String, Class<?>> types) {
            return new Query() {

                @Override
                public String create(Database database, Map<String, int[]> parameterIndexes) {
                    String query = defaultQuery;

                    for (QueryPermutation permutation: permutations) {
                        if (permutation.matchesDatabase(database)) {
                            query = permutation.query;
                            break;
                        }
                    }

                    if (query == null)
                        throw new DatabaseException("No applicable query permutation found for database " + database);

                    return query;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <T> Class<T> typeOf(String name) {
                    return (Class<T>) types.get(name);
                }

            };
        }

    }

    public static QueryBuilder builder() {
        return new QueryBuilder();
    }

    public static Query of(final String sql, final Map<String, Class<?>> types) {
        return new Query() {

            @Override
            public String create(Database database, Map<String, int[]> parameterIndexes) {
                return parseParameterQuery(sql, parameterIndexes);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Class<T> typeOf(String name) {
                return (Class<T>) types.get(name);
            }

        };
    }

    public static Query of(final String sql) {
        return of(sql, Collections.<String, Class<?>>emptyMap());
    }

    /**
     * <p>Parse a query for named parameter named in the form of a colon (:) followed by
     * a java identifier and split the query at these occurrences.
     * Additionally map the replaced occurrences to unique sequential indexes starting
     * at zero and store the result in the provided parameter map.</p>
     *
     * <p>For example</p>
     * <code>"SELECT name, mother_surname, father_surname FROM person WHERE name =
     * :name AND (mother_surname = :surname OR father_surname = :surname)"</code>
     * <p>becomes</p>
     * <code>["SELECT name, mother_surname, father_surname FROM person WHERE name = ", " AND
     * (mother_surname = ", " OR father_surname = ", ")"]</code>
     * <p>and the parameter index map will contain the values {name: [1], surname: [2, 3]}</p>
     *
     * @param query The query to parse.
     * @param parameterIndexes The map which to fill with parameter indexes.
     * @return The query split at the named parameters.
     */
    public static String parseParameterQuery(String query, Map<String, int[]> parameterIndexes) {
        final StringBuilder queryBuilder = new StringBuilder();
        final StringBuilder parameterBuilder = new StringBuilder();
        final HashMap<String, ArrayList<Integer>> parameterMapBuilder = new HashMap<>();
        int currentIndex = 0;

        /* Various ranges where parameters aren't parsed. Handling escaped characters inside the ranges is unnecessary
           because SQL handles escaping by doubling the character. The parser will simply immediately begin the range
           again after closing it when a doubled range end is encountered. For example 'abc''def' will be understood as
           two separate strings. */
        boolean inComment = false;
        boolean inDoubleQuote = false;
        boolean inSingleQuote = false;
        boolean inBracket = false;
        boolean inTick = false;

        for (int i = 0, l = query.length(); i < l; i++) {
            char c = query.charAt(i);

            // Exclude comments from query string.
            if (inComment) {
                if (c == '\n') {
                    inComment = false;
                    continue;
                }
            } else {
                int endIndex = i + 2;

                if (endIndex <= l && query.substring(i, endIndex).equals("--")) {
                    inComment = true;
                    i = endIndex - 1; // -1 to account for for-loop's increment.
                    continue;
                }
            }

            if (c == '\'') {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"') {
                inDoubleQuote = !inDoubleQuote;
            } else if (c == '[') {
                inBracket = true;
            } else if (inBracket && c == ']') {
                inBracket = false;
            } else if (c == '`') {
                inTick = !inTick;
            } else if (!inSingleQuote && !inDoubleQuote && !inBracket && !inTick) {
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
                    ArrayList<Integer> indexes = parameterMapBuilder.get(parameter);

                    if (indexes == null) {
                        indexes = new ArrayList<>();
                        parameterMapBuilder.put(parameter, indexes);
                    }

                    indexes.add(currentIndex);
                    currentIndex++;
                    queryBuilder.append('?');

                    // Append character after the named parameter.
                    c = query.charAt(i);
                }
            }

            queryBuilder.append(c);
        }

        // Convert Map<ArrayList<Integer>> to HashMap<int[]>.
        for (Map.Entry<String, ArrayList<Integer>> e: parameterMapBuilder.entrySet()) {
            ArrayList<Integer> indexList = e.getValue();
            int[] indexes = new int[indexList.size()];
            parameterIndexes.put(e.getKey(), indexes);

            for (int i = 0; i < indexes.length; i++) indexes[i] = indexList.get(i);
        }

        return queryBuilder.toString();
    }
	
}
