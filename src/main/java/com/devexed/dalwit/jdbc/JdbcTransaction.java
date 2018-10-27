package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;

import java.sql.SQLException;

abstract class JdbcTransaction extends JdbcAbstractDatabase implements Transaction {

    private final JdbcAbstractDatabase parent;
    private boolean committed = false;

    /**
     * Create a root level transaction. Committing this transaction will update the database.
     */
    JdbcTransaction(JdbcAbstractDatabase parent) {
        super(parent.readonly, parent.connection, parent.accessorFactory, parent.generatedKeysSelector, parent.columnNameMapper);
        this.parent = parent;
    }

    abstract void commitTransaction() throws SQLException;

    abstract void rollbackTransaction() throws SQLException;

    @Override
    public final Transaction transact() {
        checkActive();
        return openTransaction(new JdbcNestedTransaction(this));
    }

    @Override
    public Statement prepare(Query query) {
        checkActive();
        return new JdbcStatement(this, query);
    }

    @Override
    public final void commit() {
        checkActive();
        parent.checkIsChildTransaction(this);

        try {
            commitTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        committed = true;
    }

    @Override
    void closeResource() {
        checkActive();
        parent.checkIsChildTransaction(this);

        if (!readonly && !committed) {
            try {
                rollbackTransaction();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        parent.closeChildTransaction(this);
    }

}
