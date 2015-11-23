package com.devexed.dalwit.util;

import com.devexed.dalwit.Database;
import com.devexed.dalwit.Transaction;

public final class Transactions {

    private Transactions() {
    }

    /**
     * Creates a transaction and supplies it to a callback. If the callback produces a runtime exception the transaction
     * is rolled back, otherwise it is committed.
     *
     * @param database The database with which to create the transaction.
     * @param callback The callback which receives the transaction.
     */
    public static void transact(Database database, TransactionCallback callback) {
        Transaction transaction = null;

        try {
            transaction = database.transact();
            callback.call(transaction);
        } catch (RuntimeException e) {
            if (transaction != null) database.rollback(transaction);
            throw e;
        }

        database.commit(transaction);
    }

    public interface TransactionCallback {

        void call(Transaction transaction);

    }

}
