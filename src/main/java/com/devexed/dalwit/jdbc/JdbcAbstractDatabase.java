package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseable;
import com.devexed.dalwit.util.CloseableManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

abstract class JdbcAbstractDatabase extends AbstractCloseable implements Database {

    private final Class<?> managerType;
    final CloseableManager<JdbcStatement> statementManager;
    final java.sql.Connection connection;
    final AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory;
    final JdbcGeneratedKeysSelector generatedKeysSelector;

    private String type = null;
    private String version = null;
    private JdbcTransaction child = null;

    JdbcAbstractDatabase(Class<?> managerType, CloseableManager<JdbcStatement> statementManager,
                         java.sql.Connection connection,
                         AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory,
                         JdbcGeneratedKeysSelector generatedKeysSelector) {
        try {
            connection.setAutoCommit(false); // Needs to be false for transactions to work.
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        this.managerType = managerType;
        this.statementManager = statementManager;
        this.connection = connection;
        this.accessorFactory = accessorFactory;
        this.generatedKeysSelector = generatedKeysSelector;
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
    final void checkTransaction(Transaction transaction) {
        if (transaction != child)
            throw new DatabaseException("Child transaction was not started by this " + managerType.getSimpleName());
    }

    final JdbcTransaction openTransaction(JdbcTransaction child) {
        checkActive();
        this.child = child;
        return child;
    }

    @Override
    public void commit(Transaction transaction) {
        if (transaction == null) throw new NullPointerException("Transaction is null");

        checkTransaction(transaction);
        child.checkActive();

        try {
            child.commitTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        child.close();
        child = null;
    }

    @Override
    public void rollback(Transaction transaction) {
        if (transaction == null) return;

        checkTransaction(transaction);
        child.checkActive();

        try {
            child.rollbackTransaction();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        child.close();
        child = null;
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
        return statementManager.open(new JdbcQueryStatement(this, query));
    }

    @Override
    public UpdateStatement createUpdate(Query query) {
        checkNotClosed();
        return statementManager.open(new JdbcUpdateStatement(this, query));
    }

    @Override
    public ExecutionStatement createExecution(Query query) {
        checkNotClosed();
        return statementManager.open(new JdbcExecutionStatement(this, query));
    }

    @Override
    public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
        checkNotClosed();
        return statementManager.open(new JdbcInsertStatement(this, query, keys));
    }

    @Override
    public void close(Statement statement) {
        statementManager.close(statement);
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
