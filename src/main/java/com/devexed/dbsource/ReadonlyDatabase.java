package com.devexed.dbsource;

public interface ReadonlyDatabase extends Driver {

    /**
     * Prepares a query into a statement that reads from the database.
     */
    QueryStatement createQuery(Query query);

    /**
     * Close a statement opened by this database.
     *
     * @param statement The statement to close.
     */
    void close(Statement statement);

}
