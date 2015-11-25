package com.devexed.dalwit.util;

import com.devexed.dalwit.Database;

public final class Databases {

    /**
     * Creates a closable transaction on a database.
     *
     * @param database The database with which to create the transaction.
     * @return The closable transaction.
     */
    public static ClosableTransaction transact(Database database) {
        return new ClosableTransaction(database, database.transact());
    }

    private Databases() {
    }

}
