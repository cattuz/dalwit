package com.devexed.dbsource;

import java.io.File;

public abstract class FileDatabaseTestCase extends DatabaseTestCase {

    private String dbPath;

    public abstract File createDatabaseFile() throws Exception;

    public final String getDatabasePath() {
        return dbPath;
    }

    @Override
    public void createDatabase() throws Exception {
        dbPath = createDatabaseFile().getAbsolutePath();
    }

    @Override
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void destroyDatabase() throws Exception {
        db.close();
        new File(dbPath).delete();
    }

}
