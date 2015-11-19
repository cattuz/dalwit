package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

import java.sql.SQLException;

abstract class JdbcTransaction extends JdbcAbstractDatabase implements Transaction {

    private final JdbcAbstractDatabase parent;
	private boolean committed = false;
	
	/**
	 * Create a root level transaction. Committing this transaction will
	 * update the database.
	 */
	JdbcTransaction(JdbcAbstractDatabase parent) {
		super(parent.connection, parent.accessorFactory, parent.generatedKeysSelector);
        this.parent = parent;
	}

    private void checkNotCommitted() {
        if (committed) throw new DatabaseException("Already committed");
    }

    final void checkActive() {
        checkChildClosed();
		checkNotCommitted();
		checkNotClosed();
	}

    abstract void commitTransaction() throws SQLException;

    abstract void rollbackTransaction() throws SQLException;

    @Override
    public final Transaction transact() {
        checkActive();

        JdbcNestedTransaction transaction = new JdbcNestedTransaction(this);
        onOpenChild(transaction);
        return transaction;
    }

    @Override
    public final void commit() {
        checkActive();

        try {
            commitTransaction();
            committed = true;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final void close() {
        if (isClosed()) return;

        super.close();
        parent.onCloseChild();

        try {
            if (!committed) rollbackTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
	
}
