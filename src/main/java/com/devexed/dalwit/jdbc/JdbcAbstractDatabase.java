package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

abstract class JdbcAbstractDatabase extends AbstractCloseable implements Database {

    final java.sql.Connection connection;
    final AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory;
    final JdbcGeneratedKeysSelector generatedKeysSelector;

    private String type = null;
    private String version = null;
    private JdbcTransaction child = null;

    JdbcAbstractDatabase(java.sql.Connection connection,
                         AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory,
                         JdbcGeneratedKeysSelector generatedKeysSelector) {
        this.connection = connection;
        this.accessorFactory = accessorFactory;
        this.generatedKeysSelector = generatedKeysSelector;
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
    final boolean hasChildTransaction() {
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
    public String getType() {
        checkNotClosed();

        if (type == null) {
            try {
                type = connection.getMetaData().getDatabaseProductName();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        return type;
    }

    @Override
    public String getVersion() {
        checkNotClosed();

        if (version == null) {
            try {
                return connection.getMetaData().getDatabaseProductVersion();
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

        return version;
    }

    @Override
    public QueryStatement createQuery(Query query) {
        checkActive();
        return new JdbcQueryStatement(this, query);
    }

    @Override
    public String toString() {
        String url;

        try {
            url = connection.getMetaData().getURL();
        } catch (SQLException e) {
            url = ":unavailable:";
        }

        return "[" + JdbcDatabase.class.getSimpleName() + "; " +
                "type=" + getType() + "; " +
                "version=" + getVersion() + "; " +
                "url=" + url + "]";
    }

}
