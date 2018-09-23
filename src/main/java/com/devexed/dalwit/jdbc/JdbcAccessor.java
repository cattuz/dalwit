package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Accessor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Accessor to bind values to and retrieve values from JDBC interfaces.
 */
interface JdbcAccessor extends Accessor<PreparedStatement, ResultSet, SQLException> {}
