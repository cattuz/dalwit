package com.devexed.dalwit;


/**
 * Represents a database transaction. Updates to a {@link Database} can only occur within an open transaction.
 * Statements opened by a method on a transaction object are opened, closed and reusable in the database's "scope".
 */
public interface Transaction extends Database {

    /**
     * Commit the transaction. If the transaction is not committed before it is closed is will be rolled back.
     *
     * @throws DatabaseException If the transaction could not be committed.
     */
    void commit();

}
