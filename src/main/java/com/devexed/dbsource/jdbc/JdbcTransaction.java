package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Transaction;

import java.sql.SQLException;

abstract class JdbcTransaction extends JdbcAbstractDatabase implements Transaction {

    /**
     * Create a root level transaction. Committing this transaction will
     * update the database.
     */
    JdbcTransaction(JdbcAbstractDatabase parent) {
        super(Transaction.class, parent.connection, parent.accessorFactory, parent.generatedKeysSelector);
    }

    abstract void commitTransaction() throws SQLException;

    abstract void rollbackTransaction() throws SQLException;

    @Override
    public final Transaction transact() {
        checkActive();
        return openTransaction(new JdbcNestedTransaction(this));
    }

}
