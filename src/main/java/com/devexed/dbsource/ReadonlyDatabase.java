package com.devexed.dbsource;

import java.io.Closeable;

public interface ReadonlyDatabase extends Driver, Closeable {

    /**
     * Prepares a query into a statement that reads from the database.
     */
    QueryStatement createQuery(Query query);

    @Override
    void close();

}
