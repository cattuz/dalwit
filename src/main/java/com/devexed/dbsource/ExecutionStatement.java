package com.devexed.dbsource;

/**
 * A statement that modifies the database, tables or rows, in any way.
 */
public interface ExecutionStatement extends Statement {

	void execute(Transaction transaction);

}
