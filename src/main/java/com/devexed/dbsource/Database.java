package com.devexed.dbsource;

import java.io.Closeable;

public interface Database extends Closeable {

    /** Prepares a query into a statement that reads from the database. */
    QueryStatement createQuery(Query query);

    @Override
    void close();

    String getType();

    String getVersion();

}
