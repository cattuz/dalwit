package com.devexed.dbsource;

import java.util.Iterator;
import java.util.Map;

public final class Statements {

    public static <E> void bindAll(Statement statement, Map<String, E> parameters) {
        for (Map.Entry<String, E> e: parameters.entrySet())
            statement.bind(e.getKey(), e.getValue());
    }

    public static <E> void buildInExpression(Iterable<E> values, String parameterPrefix, StringBuilder builder,
                                             Map<String, E> parameters) {
        Iterator<E> it = values.iterator();

        if (it.hasNext()) {
            int index = 0;
            String p0 = parameterPrefix + (index++);
            parameters.put(p0, it.next());
            builder.append("(:").append(p0);

            while (it.hasNext()) {
                String p = parameterPrefix + (index++);
                parameters.put(p, it.next());
                builder.append(",:").append(p);
            }

            builder.append(")");
        }
    }

    public static <E> String buildInExpression(Iterable<E> values, String parameterPrefix, Map<String, E> parameters) {
        StringBuilder builder = new StringBuilder();
        buildInExpression(values, parameterPrefix, builder, parameters);
        return builder.toString();
    }

}
