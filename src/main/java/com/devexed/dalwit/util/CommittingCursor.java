package com.devexed.dalwit.util;

import com.devexed.dalwit.Closeable;
import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.Transaction;

/**
 * Cursor which closes and commits a parent closable when it itself is closed.
 */
public class CommittingCursor extends ClosingCursor {

    private final Transaction transaction;

    CommittingCursor(Transaction transaction, Closeable parent, Cursor cursor) {
        super(parent, cursor);
        this.transaction = transaction;
    }

    @Override
    public void close() {
        super.close();
        transaction.commit();
        transaction.close();
    }

}
