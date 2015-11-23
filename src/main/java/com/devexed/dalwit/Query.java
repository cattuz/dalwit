package com.devexed.dalwit;

import java.util.List;
import java.util.Map;

/**
 * Class for building SQL queries capable of supporting differing SQL implementations.
 */
public interface Query {

    /**
     * Create the query with indexed parameters, suitable for most databases.
     *
     * @param driver           The driver to apply the query to, with type info to allow different queries for
     *                         different database drivers.
     * @param parameterIndexes The parameters mapped to the list of indexes of ? in the created string.
     * @param indexParameters  The indexes mapped to the name of their associated parameter.
     * @return The query with indexed parameters.
     */
    String create(Driver driver, Map<String, List<Integer>> parameterIndexes,
                  Map<Integer, String> indexParameters);

    /**
     * Get the type of a named parameter or column in the query.
     *
     * @param name The name of the parameter or column.
     * @return The java class of the parameter at the position.
     */
    <T> Class<T> typeOf(String name);

    int hashCode();

    boolean equals(Object object);

}
