package com.devexed.dbsource;

import java.io.File;

public abstract class FileDatabaseTestCase extends DatabaseTestCase {

    private String dbPath;

    public abstract File createDatabaseFile() throws Exception;

    public final String getDatabasePath() {
        return dbPath;
    }

    @Override
    public final void createDatabase() throws Exception {
        dbPath = createDatabaseFile().getAbsolutePath();
    }

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public final void destroyDatabase() throws Exception {
        db.close();
        new File(dbPath).delete();
    }

}
