package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;
import com.devexed.dbsource.util.AbstractCloseable;
import com.devexed.dbsource.util.CloseableManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

abstract class JdbcAbstractDatabase extends AbstractCloseable implements Database {

    final String managerType;
    final java.sql.Connection connection;
    final AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory;
    final JdbcGeneratedKeysSelector generatedKeysSelector;
    private final CloseableManager<JdbcStatement> statementManager =
            new CloseableManager<JdbcStatement>(Connection.class, Database.class);
    private String type = null;
    private String version = null;
    private JdbcTransaction child = null;

    JdbcAbstractDatabase(Class<?> managerClass, java.sql.Connection connection,
                         AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                         JdbcGeneratedKeysSelector generatedKeysSelector) {
        try {
            connection.setAutoCommit(false); // Needs to be false for transactions to work.
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        this.managerType = managerClass.getSimpleName();
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
            throw new DatabaseException("Child transaction was not started by this " + managerType);
    }

    final JdbcTransaction openTransaction(JdbcTransaction child) {
        checkActive();
        this.child = child;
        return child;
    }

    @Override
    public void commit(Transaction transaction) {
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
    public void close() {
        statementManager.close();
        super.close();
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
