package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

abstract class JdbcAbstractDatabase extends AbstractCloseable implements Database {

	final Connection connection;
	final JdbcAccessorFactory accessorFactory;
    final GeneratedKeysSelector generatedKeysSelector;

	private JdbcTransaction child = null;
	
	JdbcAbstractDatabase(Connection connection, JdbcAccessorFactory accessorFactory,
                         GeneratedKeysSelector generatedKeysSelector) {
		this.connection = connection;
		this.accessorFactory = accessorFactory;
        this.generatedKeysSelector = generatedKeysSelector;
    }

	/** Check if this transaction has an open child transaction. */
    final void checkChildClosed() {
		if (child != null) throw new DatabaseException("Child transaction is still open");
	}

    final void onCloseChild() {
        if (child == null) throw new DatabaseException("No child transaction open");

        child = null;
    }

    final void onOpenChild(JdbcTransaction child) {
        checkChildClosed();

        this.child = child;
    }
	
	@Override
	public String getType() {
		checkNotClosed();
		
		try {
			return connection.getMetaData().getDatabaseProductName();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    @Override
    public String getVersion() {
        checkNotClosed();

        try {
            return connection.getMetaData().getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
	public QueryStatement createQuery(Query query) {
		checkNotClosed();

        return new JdbcQueryStatement(this, query);
	}

	@Override
	public UpdateStatement createUpdate(Query query) {
		checkNotClosed();

        return new JdbcUpdateStatement(this, query);
	}

	@Override
	public ExecutionStatement createExecution(Query query) {
		checkNotClosed();

        return new JdbcExecutionStatement(this, query);
	}

	@Override
	public InsertStatement createInsert(Query query, Map<String, Class<?>> keys) {
		checkNotClosed();

        return new JdbcInsertStatement(this, query, keys);
	}

	@Override
	public void close() {
		if (isClosed()) return;

        // Close child hierarchy, allowing easy cleanup on failure.
		if (child != null && !child.isClosed()) child.close();

		super.close();
	}

	@Override
	public String toString() {
		String url;
		
		try {
			url = connection.getMetaData().getURL();
		} catch (SQLException e) {
			url = ":unavailable:";
		}
		
		return "[" + JdbcDatabase.class.getSimpleName() + "; " +
                "type=" + getType() + "; " +
                "version=" + getVersion() + "; " +
                "url=" + url + "]";
	}
	
}
