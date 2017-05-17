package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.DatabaseException;

import java.sql.SQLException;

final class JdbcRootTransaction extends JdbcTransaction {

    /**
     * Create a root level transaction. Committing this transaction will
     * update the database.
     */
    JdbcRootTransaction(JdbcAbstractDatabase parent) {
        super(parent);

        // Disable auto commit while starting a root transaction.
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void closeResource() {
        super.closeResource();

        // Disable auto commit when leaving root transaction.
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    void commitTransaction() throws SQLException {
        connection.commit();
    }

    @Override
    void rollbackTransaction() throws SQLException {
        connection.rollback();
    }

}
