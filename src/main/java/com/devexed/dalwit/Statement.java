package com.devexed.dalwit;

/**
 * A statement to query or modify the database.
 */
public interface Statement {

    /**
     * Clears all bound parameter values from this statement.
     */
    void clear();

    /**
     * Bind a parameter to this statement.
     *
     * @param parameter The name of parameter to bind.
     * @param value     The value of the bound parameter, checked against the parameter's type as specified by the query
     *                  this statement was generated from.
     */
    <T> void bind(String parameter, T value);

}
