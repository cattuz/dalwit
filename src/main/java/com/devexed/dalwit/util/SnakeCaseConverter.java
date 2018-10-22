package com.devexed.dalwit.util;

public final class SnakeCaseConverter {

    public static String toSnakeCase(String s) {
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
