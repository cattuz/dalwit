package com.devexed.dalwit.util;

public final class DefaultObjectColumnMapper implements ObjectColumnMapper {
    @Override
    public String apply(String column) {
        return column;
    }
}
