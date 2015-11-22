package com.devexed.dbsource.util;

import com.devexed.dbsource.Connection;
import com.devexed.dbsource.Database;
import com.devexed.dbsource.ReadonlyDatabase;

/**
 * FIXME Document!
 */
public final class Connections {

    private Connections() {
    }

    public static void write(Connection connection, WriteCallback callback) {
        Database database = null;

        try {
            database = connection.write();
            callback.call(database);
        } finally {
            connection.close(database);
        }
    }

    public static void read(Connection connection, ReadCallback callback) {
        ReadonlyDatabase database = null;

        try {
            database = connection.read();
            callback.call(database);
        } finally {
            connection.close(database);
        }
    }

    public interface WriteCallback {

        void call(Database database);

    }

    public interface ReadCallback {

        void call(ReadonlyDatabase database);

    }

}
