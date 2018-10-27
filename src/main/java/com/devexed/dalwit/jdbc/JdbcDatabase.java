package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

public final class JdbcDatabase extends JdbcAbstractDatabase {

    public JdbcDatabase(boolean readonly, Connection connection,
                        AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                        JdbcGeneratedKeysSelector generatedKeysSelector,
                        Function<String, String> columnNameMapper) {
        super(readonly, connection, accessorFactory, generatedKeysSelector, columnNameMapper);
    }

    @Override
    public Transaction transact() {
        checkActive();
        JdbcRootTransaction transaction = new JdbcRootTransaction(this);
        openTransaction(transaction);
        return transaction;
    }

    @Override
    public Statement prepare(Query query) {
        checkActive();
        return new JdbcStatement(this, query);
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
