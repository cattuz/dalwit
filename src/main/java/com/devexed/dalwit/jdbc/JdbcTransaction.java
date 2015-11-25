package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Transaction;

import java.sql.SQLException;

abstract class JdbcTransaction extends JdbcAbstractDatabase implements Transaction {

    private final JdbcAbstractDatabase parent;
    private boolean committed = false;

    /**
     * Create a root level transaction. Committing this transaction will update the database.
     */
    JdbcTransaction(JdbcAbstractDatabase parent) {
        super("transaction", parent.connection, parent.accessorFactory, parent.generatedKeysSelector);
        this.parent = parent;
    }

    abstract void commitTransaction() throws SQLException;

    abstract void rollbackTransaction() throws SQLException;

    @Override
    void checkChildTransaction(Transaction transaction) {
        super.checkChildTransaction(transaction);
        parent.checkChildTransaction(this);
    }

    @Override
    public final Transaction transact() {
        checkActive();
        return openTransaction(new JdbcNestedTransaction(this));
    }

    @Override
    public final void commit() {
        checkActive();
        parent.checkChildTransaction(this);

        try {
            commitTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        committed = true;
    }

    @Override
    public final void close() {
        if (isClosed()) return;

        checkActive();
        parent.checkChildTransaction(this);

        if (!committed) {
            try {
                rollbackTransaction();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        parent.closeChildTransaction(this);
        super.close();
    }

}
