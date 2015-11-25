package com.devexed.dalwit;

/**
 * A driver which can open a database for reading and writing.
 */
public interface Connection extends Closer<ReadonlyDatabase> {

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

    /**
     * Close a database opened by this driver.
     *
     * @param database The database opened by this driver to close. For convenience sake, if <code>database</code> is
     *                 null this is a no-op.
     */
    @Override
    void close(ReadonlyDatabase database);

}
