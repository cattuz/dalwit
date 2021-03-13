package com.devexed.dalwit;

import com.devexed.dalwit.util.SnakeCaseConverter;

/**
 * Function which maps snake_case names into camelCase.
 */
public final class DefaultColumnNameMapper implements ColumnNameMapper {

    @Override
    public String apply(String s) {
        return SnakeCaseConverter.toCamelCase(s);
    }

}
