package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public final class JdbcDatabase extends JdbcAbstractDatabase {

    public JdbcDatabase(Connection connection,
                 AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                 JdbcGeneratedKeysSelector generatedKeysSelector) {
        super(connection, accessorFactory, generatedKeysSelector);
    }

    @Override
    public Transaction transact() {
        checkActive();
        JdbcRootTransaction transaction = new JdbcRootTransaction(this);
        openTransaction(transaction);
        return transaction;
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
    void closeResource() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
