package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.UpdateStatement;

import java.sql.SQLException;

final class JdbcUpdateStatement extends JdbcStatement implements UpdateStatement {

    JdbcUpdateStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            assignStatement(database.connection.prepareStatement(query.createSql()));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public long update() {
        checkNotClosed();
        database.checkActive();

        try {
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
