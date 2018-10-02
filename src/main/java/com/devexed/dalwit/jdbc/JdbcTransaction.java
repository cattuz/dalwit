package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;

import java.sql.SQLException;
import java.util.Map;

abstract class JdbcTransaction extends JdbcAbstractDatabase implements Transaction {

    private final JdbcAbstractDatabase parent;
    private boolean committed = false;

    /**
     * Create a root level transaction. Committing this transaction will update the database.
     */
    JdbcTransaction(JdbcAbstractDatabase parent) {
        super(parent.connection, parent.accessorFactory, parent.generatedKeysSelector, parent.columnNameMapper);
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
    public UpdateStatement createUpdate(Query query) {
        checkActive();
        return new JdbcUpdateStatement(this, query);
    }

    @Override
    public ExecutionStatement createExecution(Query query) {
        checkActive();
        return new JdbcExecutionStatement(this, query);
    }

    @Override
    public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
        checkActive();
        return new JdbcInsertStatement(this, query, keys);
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

        if (!committed) {
            try {
                rollbackTransaction();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        parent.closeChildTransaction(this);
    }

}
