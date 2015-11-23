package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.Transaction;
import com.devexed.dalwit.UpdateStatement;

import java.sql.SQLException;

final class JdbcUpdateStatement extends JdbcStatement implements UpdateStatement {

    public JdbcUpdateStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            setStatement(database.connection.prepareStatement(query.create(database, parameterIndexes,
                    indexParameters)));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public long update(Transaction transaction) {
        checkNotClosed();
        checkActiveTransaction(transaction);

        try {
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
