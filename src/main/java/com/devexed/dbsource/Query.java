package com.devexed.dbsource;

import java.util.Map;

/**
 * Class for building SQL queries capable of supporting differing SQL implementations.
 */
public interface Query {

    /**
     * Create the query with indexed parameters, suitable for most databases.
     * @param database The database to apply the query to, with type info to allow different queries for different
     *                 databases.
     * @param parameterIndexes The indexes of the named parameters in the query.
     * @return The query with indexed parameters.
     */
	String create(Database database, Map<String, int[]> parameterIndexes);

	/**
	 * Get the type of a named parameter or column (in the case of select queries).
	 *
	 * @param name The name of the parameter or column.
	 * @return The java class of the parameter at the position.
	 */
	<T> Class<?> typeOf(String name);
	
}
