package com.devexed.dalwit;

public interface ReadonlyDatabase extends Closeable {

    /**
     * Prepares a query into a statement that reads from the database. IN SQL databases this is typically a SELECT
     * statement.
     *
     * @param query The SQL statement.
     * @return The prepared query statement.
     * @throws DatabaseException If the statement could not be prepared.
     */
    QueryStatement createQuery(Query query);

}
