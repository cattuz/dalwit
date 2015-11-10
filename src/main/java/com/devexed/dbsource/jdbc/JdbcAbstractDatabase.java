package com.devexed.dbsource.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import com.devexed.dbsource.*;

abstract class JdbcAbstractDatabase extends AbstractCloseable implements TransactionDatabase {

	final Connection connection;
	final Map<Class<?>, JdbcAccessor> accessors;
    final GeneratedKeysSelector generatedKeysSelector;
	
	JdbcAbstractDatabase(Connection connection, Map<Class<?>, JdbcAccessor> accessors,
                         GeneratedKeysSelector generatedKeysSelector) {
		this.connection = connection;
		this.accessors = accessors;
        this.generatedKeysSelector = generatedKeysSelector;
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
	public UpdateStatement prepareUpdate(Query query) {
		checkNotClosed();

        return new JdbcUpdateStatement(this, query);
	}

	@Override
	public ExecutionStatement prepareExecution(Query query) {
		checkNotClosed();

        return new JdbcExecutionStatement(this, query);
	}

	@Override
	public InsertStatement prepareInsert(Query query, Map<String, Class<?>> keys) {
		checkNotClosed();

        return new JdbcInsertStatement(this, query, keys);
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
