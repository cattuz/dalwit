package com.devexed.dalwit.jdbc;

import java.util.function.Function;

/**
 * Function which maps snake_case names into camelCase.
 */
public final class DefaultJdbcColumnNameMapper implements Function<String, String> {

    @Override
    public String apply(String s) {
        StringBuilder result = new StringBuilder();
        boolean nextIsUpperCase = false;

        for (char c : s.toCharArray()) {
            if (c == '_') {
                nextIsUpperCase = true;
            } else if (nextIsUpperCase) {
                result.append(Character.toUpperCase(c));
                nextIsUpperCase = false;
            } else {
                result.append(c);
            }
        }

        return result.toString();
    }

}
