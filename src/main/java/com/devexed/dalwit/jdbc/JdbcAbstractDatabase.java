package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

abstract class JdbcAbstractDatabase extends AbstractCloseable implements Database {

    private final String managerType;
    final java.sql.Connection connection;
    final AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory;
    final JdbcGeneratedKeysSelector generatedKeysSelector;

    private String type = null;
    private String version = null;
    private JdbcTransaction child = null;

    JdbcAbstractDatabase(String managerType, java.sql.Connection connection,
                         AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory,
                         JdbcGeneratedKeysSelector generatedKeysSelector) {
        try {
            connection.setAutoCommit(false); // Needs to be false for transactions to work.
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        this.managerType = managerType;
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

    @Override
    public void close() {
        if (child != null) closeChildTransaction(child);
        super.close();
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void checkActive() {
        if (child != null) throw new DatabaseException("Child transaction is still open");
        checkNotClosed();
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    void checkChildTransaction(Transaction transaction) {
        if (transaction != child)
            throw new DatabaseException("Child transaction was not started by this " + managerType);
    }

    /**
     * Check if this transaction has an open child transaction.
     */
    final void closeChildTransaction(Transaction transaction) {
        checkChildTransaction(transaction);
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
        checkNotClosed();
        return new JdbcQueryStatement(this, query);
    }

    @Override
    public UpdateStatement createUpdate(Query query) {
        checkNotClosed();
        return new JdbcUpdateStatement(this, query);
    }

    @Override
    public ExecutionStatement createExecution(Query query) {
        checkNotClosed();
        return new JdbcExecutionStatement(this, query);
    }

    @Override
    public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
        checkNotClosed();
        return new JdbcInsertStatement(this, query, keys);
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
