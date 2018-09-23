package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.ExecutionStatement;
import com.devexed.dalwit.Query;

import java.sql.SQLException;

final class JdbcExecutionStatement extends JdbcStatement implements ExecutionStatement {

    JdbcExecutionStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            assignStatement(database.connection.prepareStatement(query.createSql()));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void execute() {
        checkNotClosed();
        database.checkActive();

        try {
            statement.execute();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
