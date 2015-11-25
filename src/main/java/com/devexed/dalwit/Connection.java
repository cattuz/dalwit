package com.devexed.dalwit;

/**
 * A driver which can open a database for reading and writing.
 */
public interface Connection {

    /**
     * Open a writable database.
     *
     * @return A writable database.
     * @throws DatabaseException If the database could not be opened for writing.
     */
    Database write();

    /**
     * Open a read only database.
     *
     * @return A read only database.
     * @throws DatabaseException If the database could not be opened for reading.
     */
    ReadonlyDatabase read();

}
