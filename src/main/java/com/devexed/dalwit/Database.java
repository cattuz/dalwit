package com.devexed.dalwit;

/**
 * Database supporting transactions (modifying the database).
 */
public interface Database extends ReadonlyDatabase {

    /**
     * Start a transaction to update the database.
     *
     * @return The transaction which when committed will update the database.
     * @throws DatabaseException If the transaction could not be started.
     */
    Transaction transact();

}
