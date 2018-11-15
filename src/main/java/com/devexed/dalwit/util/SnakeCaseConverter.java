package com.devexed.dalwit.util;

public final class SnakeCaseConverter {

    public static String toCamelCase(String s) {
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

    public static String toSnakeCase(String s) {
        StringBuilder result = new StringBuilder();
        boolean skipNextUnderline = true;

        for (char c : s.toCharArray()) {
            if (Character.isUpperCase(c)) {
                if (!skipNextUnderline) {
                    result.append('_');
                    skipNextUnderline = true;
                }

                result.append(Character.toLowerCase(c));
            } else {
                result.append(c);
                skipNextUnderline = false;
            }
        }

        return result.toString();
    }

}
