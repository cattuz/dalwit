package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

abstract class JdbcAbstractDatabase extends AbstractCloseable implements Database {

    final boolean readonly;
    final java.sql.Connection connection;
    final AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory;
    final JdbcGeneratedKeysSelector generatedKeysSelector;
    final ColumnNameMapper columnNameMapper;

    private JdbcTransaction child = null;

    JdbcAbstractDatabase(boolean readonly,
                         java.sql.Connection connection,
                         AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                         JdbcGeneratedKeysSelector generatedKeysSelector,
                         ColumnNameMapper columnNameMapper) {
        this.readonly = readonly;
        this.connection = connection;
        this.accessorFactory = accessorFactory;
        this.generatedKeysSelector = generatedKeysSelector;
        this.columnNameMapper = columnNameMapper;
    }

    @Override
    protected final boolean isClosed() {
        try {
            return super.isClosed() || connection.isClosed();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    abstract void closeResource();

    @Override
    public final void close() {
        if (child != null) child.close();
        closeResource();
        super.close();
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    private boolean hasChildTransaction() {
        return child != null;
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkActive() {
        checkNotClosed();
        if (hasChildTransaction()) throw new DatabaseException("Transaction has an open child transaction");
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    void checkIsChildTransaction(JdbcTransaction transaction) {
        if (transaction != child) throw new DatabaseException("Child transaction not open");
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void closeChildTransaction(JdbcTransaction transaction) {
        checkIsChildTransaction(transaction);
        child = null;
    }

    final JdbcTransaction openTransaction(JdbcTransaction child) {
        checkActive();
        this.child = child;
        return child;
    }

    @Override
    public Statement prepare(Query query) {
        checkActive();
        return new JdbcStatement(this, query);
    }

    @Override
    public String toString() {
        String url;

        try {
            url = connection.getMetaData().getURL();
        } catch (SQLException e) {
            url = ":unavailable:";
        }

        return "[" + JdbcDatabase.class.getSimpleName() + "; url=" + url + "]";
    }

}
