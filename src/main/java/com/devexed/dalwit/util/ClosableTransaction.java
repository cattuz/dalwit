package com.devexed.dalwit.util;

import com.devexed.dalwit.*;

import java.util.Map;

/**
 * A transaction which rollbacks or commits on close. If any database exception occurs on access the transaction rolls
 * back on close, otherwise it commits.
 */
public final class ClosableTransaction implements Transaction, Closeable {

    private final Database database;
    private final Transaction transaction;
    private boolean succeeded = true;

    public ClosableTransaction(Database database, Transaction transaction) {
        this.database = database;
        this.transaction = transaction;
    }

    @Override
    public ExecutionStatement createExecution(Query query) {
        try {
            return transaction.createExecution(query);
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public UpdateStatement createUpdate(Query query) {
        try {
            return transaction.createUpdate(query);
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
        try {
            return transaction.createInsert(query, keys);
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public QueryStatement createQuery(Query query) {
        try {
            return transaction.createQuery(query);
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public Transaction transact() {
        try {
            return transaction.transact();
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public void commit(Transaction child) {
        try {
            transaction.commit(child);
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public void rollback(Transaction child) {
        try {
            transaction.rollback(child);
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public void close(Statement statement) {
        try {
            transaction.close(statement);
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public String getType() {
        try {
            return transaction.getType();
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    @Override
    public String getVersion() {
        try {
            return transaction.getVersion();
        } catch (DatabaseException e) {
            fail();
            throw e;
        }
    }

    private void fail() {
        succeeded = false;
    }

    @Override
    public void close() {
        if (succeeded) database.commit(transaction);
        else database.rollback(transaction);
    }


}
