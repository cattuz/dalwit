package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;
import com.devexed.dbsource.TransactionDatabase;

import java.sql.*;
import java.util.Properties;

public final class JdbcDatabase extends JdbcAbstractDatabase {

    public static Database openReadable(String url, Properties properties, JdbcAccessorFactory accessorFactory,
                                        GeneratedKeysSelector generatedKeysSelector) {
        return open(url, properties, accessorFactory, generatedKeysSelector, false);
    }

    public static TransactionDatabase openWritable(String url, Properties properties,
                                                   JdbcAccessorFactory accessorFactory,
                                                   GeneratedKeysSelector generatedKeysSelector) {
        return open(url, properties, accessorFactory, generatedKeysSelector, true);
    }

	private static JdbcDatabase open(String url, Properties properties, JdbcAccessorFactory accessorFactory,
                                     GeneratedKeysSelector generatedKeysSelector, boolean writable) {
		try {
			Connection connection = DriverManager.getConnection(url, properties);
            connection.setReadOnly(!writable); // Enforce readability constraint.
            connection.setAutoCommit(false); // Needs to be false for transactions to work.
			return new JdbcDatabase(connection, accessorFactory, generatedKeysSelector);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
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
