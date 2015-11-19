package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Accessor factory which maps the default JDBC types to classes.
 */
public final class DefaultJdbcAccessorFactory implements JdbcAccessorFactory {

    /**
     * Definitions for core java accessors that have a corresponding JDBC setter and getter.
     * <p/>
     * The primitive classes (e.g. Integer.TYPE) are non-nullable while the boxed primitives classes are nullable.
     * <p/>
     * Includes accessors for all primitives and additionally supports {@link String}, {@link Date}, {@link BigDecimal}
     * {@link InputStream} and byte[].
     */
    private static final Map<Class<?>, JdbcAccessor> accessors = new HashMap<Class<?>, JdbcAccessor>() {{
        put(Boolean.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type boolean can not be null.");
                statement.setBoolean(index + 1, (Boolean) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                boolean v = resultSet.getBoolean(index + 1);
                if (resultSet.wasNull())
                    throw new NullPointerException("Illegal null value for type boolean in result set.");
                return v;
            }
        });
        put(Byte.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type byte can not be null.");
                statement.setByte(index + 1, (Byte) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                byte v = resultSet.getByte(index + 1);
                if (resultSet.wasNull())
                    throw new NullPointerException("Illegal null value for type byte in result set.");
                return v;
            }
        });
        put(Short.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type short can not be null.");
                statement.setShort(index + 1, (Short) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                short v = resultSet.getShort(index + 1);
                if (resultSet.wasNull())
                    throw new NullPointerException("Illegal null value for type short in result set.");
                return v;
            }
        });
        put(Integer.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type int can not be null.");
                statement.setInt(index + 1, (Integer) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                int v = resultSet.getInt(index + 1);
                if (resultSet.wasNull())
                    throw new NullPointerException("Illegal null value for type int in result set.");
                return v;
            }
        });
        put(Long.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type long can not be null.");
                statement.setLong(index + 1, (Long) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                long v = resultSet.getLong(index + 1);
                if (resultSet.wasNull())
                    throw new NullPointerException("Illegal null value for type long in result set.");
                return v;
            }
        });
        put(Float.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type float can not be null.");
                statement.setFloat(index + 1, (Float) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                float v = resultSet.getFloat(index + 1);
                if (resultSet.wasNull())
                    throw new NullPointerException("Illegal null value for type float in result set.");
                return v;
            }
        });
        put(Double.TYPE, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) throw new NullPointerException("Parameter with type double can not be null.");
                statement.setDouble(index + 1, (Double) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                double v = resultSet.getDouble(index + 1);
                if (resultSet.wasNull())
                    throw new NullPointerException("Illegal null value for type double in result set.");
                return v;
            }
        });
        put(Boolean.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index + 1, Types.BOOLEAN);
                else statement.setBoolean(index + 1, (Boolean) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                boolean v = resultSet.getBoolean(index + 1);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Byte.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index + 1, Types.TINYINT);
                else statement.setByte(index + 1, (Byte) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                byte v = resultSet.getByte(index + 1);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Short.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index + 1, Types.SMALLINT);
                else statement.setShort(index + 1, (Short) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                short v = resultSet.getShort(index + 1);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Integer.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index + 1, Types.INTEGER);
                else statement.setInt(index + 1, (Integer) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                int v = resultSet.getInt(index + 1);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Long.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index + 1, Types.BIGINT);
                else statement.setLong(index + 1, (Long) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                long v = resultSet.getLong(index + 1);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Float.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index + 1, Types.FLOAT);
                else statement.setFloat(index + 1, (Float) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                float v = resultSet.getFloat(index + 1);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(Double.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setNull(index + 1, Types.DOUBLE);
                else statement.setDouble(index + 1, (Double) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                double v = resultSet.getDouble(index + 1);
                return resultSet.wasNull() ? null : v;
            }
        });
        put(String.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setString(index + 1, (String) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getString(index + 1);
            }
        });
        put(Date.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setTimestamp(index + 1, new Timestamp(((Date) value).getTime()));
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getTimestamp(index + 1);
            }
        });
        put(BigInteger.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                if (value == null) statement.setBigDecimal(index + 1, null);
                else statement.setBigDecimal(index + 1, new BigDecimal((BigInteger) value));
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                BigDecimal v = resultSet.getBigDecimal(index + 1);
                return v != null ? v.unscaledValue() : null;
            }
        });
        put(BigDecimal.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setBigDecimal(index + 1, (BigDecimal) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getBigDecimal(index + 1);
            }
        });
        put(byte[].class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                statement.setBytes(index + 1, (byte[]) value);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getBytes(index + 1);
            }
        });
        put(InputStream.class, new JdbcAccessor() {
            @Override
            public void set(PreparedStatement statement, int index, Object value) throws SQLException {
                InputStream is = (InputStream) value;
                statement.setBinaryStream(index + 1, is);
            }

            @Override
            public Object get(ResultSet resultSet, int index) throws SQLException {
                return resultSet.getBinaryStream(index + 1);
            }
        });
    }};

    @Override
    public JdbcAccessor create(Class<?> type) {
        JdbcAccessor accessor = accessors.get(type);
        if (accessor == null) throw new DatabaseException("No accessor is defined for " + type);

        return accessor;
    }

}
