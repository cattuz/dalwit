package com.devexed.dalwit;

public interface ReadonlyDatabase extends Driver, Closer<Statement> {

    /**
     * Prepares a query into a statement that reads from the database. IN SQL databases this is typically a SELECT
     * statement.
     *
     * @param query The SQL statement.
     * @return The prepared query statement.
     * @throws DatabaseException If the statement could not be prepared.
     */
    QueryStatement createQuery(Query query);

    /**
     * Close a statement opened by this database. Also, closes all the statement's resources. If <code>statement</code>
     * is null this is a no-op.
     *
     * @param statement The statement to close.
     * @throws DatabaseException If the statement could not be closed.
     */
    @Override
    void close(Statement statement);

}
