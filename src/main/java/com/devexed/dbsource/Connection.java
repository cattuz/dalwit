package com.devexed.dbsource;

/**
 * A driver which can open a database for reading and writing.
 */
public interface Connection {

    /**
     * Open a writable database.
     *
     * @return A writable database.
     */
    Database write();

    /**
     * Open a read only database.
     *
     * @return A read only database.
     */
    ReadonlyDatabase read();

    /**
     * Close a database opened by this driver.
     *
     * @param database The database opened by this driver to close. For convenience sake, if database is null this is
     *                 a no-op.
     */
    void close(ReadonlyDatabase database);

}
