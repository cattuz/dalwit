package com.devexed.dalwit;


/**
 * Represents a database transaction. Updates to a {@link Database} can only occur within an open transaction.
 * Statements opened by a method on a transaction object are opened, closed and reusable in the database's "scope".
 */
public interface Transaction extends Database {}
