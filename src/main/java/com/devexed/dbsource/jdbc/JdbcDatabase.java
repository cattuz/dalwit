package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

import java.sql.Connection;
import java.sql.SQLException;

public final class JdbcDatabase extends JdbcAbstractDatabase {

    public static JdbcDatabase open(Connection connection, JdbcAccessorFactory accessorFactory,
                                    GeneratedKeysSelector generatedKeysSelector) {
        try {
            connection.setAutoCommit(false); // Needs to be false for transactions to work.
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return new JdbcDatabase(connection, accessorFactory, generatedKeysSelector);
    }

    public static JdbcDatabase open(Connection connection) {
        return open(connection, new DefaultJdbcAccessorFactory(), new DefaultJdbcGeneratedKeysSelector());
    }

    private JdbcDatabase(Connection connection, JdbcAccessorFactory accessorFactory,
                         GeneratedKeysSelector generatedKeysSelector) {
		super(connection, accessorFactory, generatedKeysSelector);
	}

	@Override
	public Transaction transact() {
        checkChildClosed();
		checkNotClosed();

        JdbcRootTransaction transaction = new JdbcRootTransaction(this);
        onOpenChild(transaction);
		return transaction;
	}

	@Override
	public void close() {
        if (isClosed()) return;

        super.close();

        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
	}

}
