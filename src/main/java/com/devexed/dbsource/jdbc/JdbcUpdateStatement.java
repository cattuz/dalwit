package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.Transaction;
import com.devexed.dbsource.UpdateStatement;

import java.sql.SQLException;

final class JdbcUpdateStatement extends JdbcStatement implements UpdateStatement {

    public JdbcUpdateStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            setStatement(database.connection.prepareStatement(query.create(database, parameterIndexes)));
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
