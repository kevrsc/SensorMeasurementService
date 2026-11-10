package com.example.sensor;

import org.flywaydb.core.Flyway;

public final class DatabaseMigrator {

    private DatabaseMigrator() {
    }

    public static void migrate(String jdbcUrl, String user, String password) {
        Flyway.configure()
                .dataSource(jdbcUrl, user, password)
                .load()
                .migrate();
    }
}
