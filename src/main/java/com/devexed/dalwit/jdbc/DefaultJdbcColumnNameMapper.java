package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.util.SnakeCaseConverter;

import java.util.function.Function;

/**
 * Function which maps snake_case names into camelCase.
 */
public final class DefaultJdbcColumnNameMapper implements JdbcColumnNameMapper {

    @Override
    public String apply(String s) {
        return SnakeCaseConverter.toCamelCase(s);
    }

}
