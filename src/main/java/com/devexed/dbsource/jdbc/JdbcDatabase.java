package com.devexed.dbsource.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.devexed.dbsource.*;

public final class JdbcDatabase extends JdbcAbstractDatabase {

    /**
     * Definitions for core java accessors that have a corresponding JDBC setter and getter.
     *
     * The primitive classes (e.g. Integer.TYPE) are non-nullable while the boxed primitives classes are nullable.
     *
     * Includes accessors for all primitives and additionally supports {@link String}, {@link Date}, {@link BigDecimal}
     * and byte[].
     */
    public static final Map<Class<?>, JdbcAccessor> accessors = new HashMap<Class<?>, JdbcAccessor>() {{
        put(Boolean.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type boolean can not be null.");
                statement.setBoolean(index, (Boolean) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                boolean v = resultSet.getBoolean(index);
                if (resultSet.wasNull()) throw new NullPointerException("Illegal null value for type boolean in result set.");
                return v;
            }
        });
        put(Byte.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type byte can not be null.");
                statement.setByte(index, (Byte) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                byte v = resultSet.getByte(index);
                if (resultSet.wasNull()) throw new NullPointerException("Illegal null value for type byte in result set.");
                return v;
            }
        });
        put(Short.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type short can not be null.");
                statement.setShort(index, (Short) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                short v = resultSet.getShort(index);
                if (resultSet.wasNull()) throw new NullPointerException("Illegal null value for type short in result set.");
                return v;
            }
        });
        put(Integer.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type int can not be null.");
                statement.setInt(index, (Integer) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                int v = resultSet.getInt(index);
                if (resultSet.wasNull()) throw new NullPointerException("Illegal null value for type int in result set.");
                return v;
            }
        });
        put(Long.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type long can not be null.");
                statement.setLong(index, (Long) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                long v = resultSet.getLong(index);
                if (resultSet.wasNull()) throw new NullPointerException("Illegal null value for type long in result set.");
                return v;
            }
        });
        put(Float.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type float can not be null.");
                statement.setFloat(index, (Float) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                float v = resultSet.getFloat(index);
                if (resultSet.wasNull()) throw new NullPointerException("Illegal null value for type float in result set.");
                return v;
            }
        });
        put(Double.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type double can not be null.");
                statement.setDouble(index, (Double) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                double v = resultSet.getDouble(index);
                if (resultSet.wasNull()) throw new NullPointerException("Illegal null value for type double in result set.");
                return v;
            }
        });
        put(Boolean.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index, Types.BOOLEAN);
                else statement.setBoolean(index, (Boolean) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                boolean v = resultSet.getBoolean(index);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Byte.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index, Types.TINYINT);
                else statement.setByte(index, (Byte) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                byte v = resultSet.getByte(index);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Short.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index, Types.SMALLINT);
                else statement.setShort(index, (Short) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                short v = resultSet.getShort(index);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Integer.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index, Types.INTEGER);
                else statement.setInt(index, (Integer) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                int v = resultSet.getInt(index);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Long.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index, Types.BIGINT);
                else statement.setLong(index, (Long) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                long v = resultSet.getLong(index);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Float.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index, Types.FLOAT);
                else statement.setFloat(index, (Float) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                float v = resultSet.getFloat(index);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Double.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index, Types.DOUBLE);
                else statement.setDouble(index, (Double) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                double v = resultSet.getDouble(index);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(String.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setString(index, (String) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getString(index);
            }
        });
        put(Date.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setTimestamp(index, new Timestamp(((Date) value).getTime()));
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getTimestamp(index);
            }
        });
        put(BigDecimal.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setBigDecimal(index, (BigDecimal) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getBigDecimal(index);
            }
        });
        put(byte[].class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setBytes(index, (byte[]) value);
            }
            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getBytes(index);
            }
        });
    }};

    public static Database openReadable(String url, Properties properties, Map<Class<?>, JdbcAccessor> accessors,
                                        GeneratedKeysSelector generatedKeysSelector) {
        return open(url, properties, accessors, generatedKeysSelector, false);
    }

    public static TransactionDatabase openWritable(String url, Properties properties, Map<Class<?>, JdbcAccessor> accessors,
                                                   GeneratedKeysSelector generatedKeysSelector) {
        return open(url, properties, accessors, generatedKeysSelector, true);
    }

	private static JdbcDatabase open(String url, Properties properties, Map<Class<?>, JdbcAccessor> accessors,
                                     GeneratedKeysSelector generatedKeysSelector, boolean writable) {
		try {
			Connection connection = DriverManager.getConnection(url, properties);
            connection.setReadOnly(!writable); // Enforce readability constraint.
            connection.setAutoCommit(false); // Needs to be false for transactions to work.
			return new JdbcDatabase(connection, accessors, generatedKeysSelector);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    private JdbcDatabase(Connection connection, Map<Class<?>, JdbcAccessor> accessors, GeneratedKeysSelector generatedKeysSelector) {
		super(connection, accessors, generatedKeysSelector);
	}

	@Override
	public Transaction transact() {
		checkNotClosed();

		return new JdbcRootTransaction(this);
	}

	@Override
	public void close() {
		checkNotClosed();

		try {
			connection.close();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
