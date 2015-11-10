package com.devexed.dbsource;

import java.io.Closeable;

public interface Statement extends Closeable {

    /** Clears all bindings from this statement. */
    void clear();
	
	<T> void bind(String parameter, T value);

    void close();
	
}
