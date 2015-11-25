package com.devexed.dalwit.util;

import com.devexed.dalwit.*;

import java.util.Map;

/**
 * FIXME Document!
 */
public final class Connections {

    private Connections() {
    }

    public static ClosableDatabase write(Connection connection) {
        Database database = null;

        try {
            database = connection.write();
            return new ClosableDatabase(connection, database);
        } catch (DatabaseException e){
            connection.close(database);
            throw e;
        }
    }

    public static ClosableReadonlyDatabase read(Connection connection) {
        ReadonlyDatabase database = null;

        try {
            database = connection.read();
            return new ClosableReadonlyDatabase(connection, database);
        } catch (DatabaseException e){
            connection.close(database);
            throw e;
        }
    }

    private static final class ClosableDatabase implements Database, Closeable {

        private final Connection connection;
        private final Database database;

        public ClosableDatabase(Connection connection, Database database) {
            this.connection = connection;
            this.database = database;
        }

        @Override
        public ExecutionStatement createExecution(Query query) {
            return database.createExecution(query);
        }

        @Override
        public UpdateStatement createUpdate(Query query) {
            return database.createUpdate(query);
        }

        @Override
        public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
            return database.createInsert(query, keys);
        }

        @Override
        public QueryStatement createQuery(Query query) {
            return database.createQuery(query);
        }

        @Override
        public Transaction transact() {
            return database.transact();
        }

        @Override
        public void commit(Transaction child) {
            database.commit(child);
        }

        @Override
        public void rollback(Transaction child) {
            database.rollback(child);
        }

        @Override
        public void close(Statement statement) {
            database.close(statement);
        }

        @Override
        public String getType() {
            return database.getType();
        }

        @Override
        public String getVersion() {
            return database.getVersion();
        }

        @Override
        public void close() {
            connection.close(database);
        }

    }

    private static final class ClosableReadonlyDatabase implements ReadonlyDatabase, Closeable {

        private final Connection connection;
        private final ReadonlyDatabase database;

        public ClosableReadonlyDatabase(Connection connection, ReadonlyDatabase database) {
            this.connection = connection;
            this.database = database;
        }

        @Override
        public QueryStatement createQuery(Query query) {
            return database.createQuery(query);
        }

        @Override
        public void close(Statement statement) {
            database.close(statement);
        }

        @Override
        public String getType() {
            return database.getType();
        }

        @Override
        public String getVersion() {
            return database.getVersion();
        }

        @Override
        public void close() {
            connection.close(database);
        }

    }

}
