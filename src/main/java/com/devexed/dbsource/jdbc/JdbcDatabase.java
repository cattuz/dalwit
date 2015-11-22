package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.AccessorFactory;
import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

final class JdbcDatabase extends JdbcAbstractDatabase {

    JdbcDatabase(Connection connection, AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                 JdbcGeneratedKeysSelector generatedKeysSelector) {
        super(Database.class, connection, accessorFactory, generatedKeysSelector);
    }

    @Override
    public Transaction transact() {
        checkActive();
        JdbcRootTransaction transaction = new JdbcRootTransaction(this);
        openTransaction(transaction);
        return transaction;
    }

    @Override
    public void close() {
        super.close();

        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
