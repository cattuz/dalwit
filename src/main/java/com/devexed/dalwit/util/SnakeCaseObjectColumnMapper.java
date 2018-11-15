package com.devexed.dalwit.util;

public final class SnakeCaseObjectColumnMapper implements ObjectColumnMapper {
    @Override
    public String apply(String column) {
        return SnakeCaseConverter.toSnakeCase(column);
    }
}
