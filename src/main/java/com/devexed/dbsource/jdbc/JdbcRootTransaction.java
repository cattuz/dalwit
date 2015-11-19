package com.devexed.dbsource.jdbc;

import java.sql.SQLException;

final class JdbcRootTransaction extends JdbcTransaction {

    /**
     * Create a root level transaction. Committing this transaction will
     * update the database.
     */
    JdbcRootTransaction(JdbcAbstractDatabase parent) {
        super(parent);
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
