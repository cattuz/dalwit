package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.ExecutionStatement;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.Transaction;

import java.sql.SQLException;

final class JdbcExecutionStatement extends JdbcStatement implements ExecutionStatement {

    public JdbcExecutionStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            setStatement(database.connection.prepareStatement(query.create(database, parameterIndexes)));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void execute(Transaction transaction) {
        checkNotClosed();
        checkActiveTransaction(transaction);

        try {
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
