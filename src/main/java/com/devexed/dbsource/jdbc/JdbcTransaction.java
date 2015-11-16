package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

import java.sql.SQLException;

abstract class JdbcTransaction extends JdbcAbstractDatabase implements Transaction {

	private boolean committed = false;
    private boolean hasChild = false;
	
	/**
	 * Create a root level transaction. Committing this transaction will
	 * update the database.
	 */
	JdbcTransaction(JdbcAbstractDatabase parent) {
		super(parent.connection, parent.accessors, parent.generatedKeysSelector);
	}

    private void checkNotCommitted() {
        if (committed) throw new DatabaseException("Already committed");
    }

    /** Check if this transaction has an open child transaction. */
    private void checkChildClosed() {
        if (hasChild) throw new DatabaseException("Transaction has an open child transaction.");
    }

    final void checkActive() {
        checkChildClosed();
		checkNotCommitted();
		checkNotClosed();
	}

    abstract void commitTransaction() throws SQLException;

    abstract void rollbackTransaction() throws SQLException;

    final void closeActiveTransaction() {
        hasChild = false;
    }

    @Override
    public final Transaction transact() {
        checkActive();

        hasChild = true;
        return new JdbcNestedTransaction(this);
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
    public void close() {
        if (isClosed()) return;

        checkChildClosed();

        try {
            if (!committed) rollbackTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        super.close();
    }
	
}
