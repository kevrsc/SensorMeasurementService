package com.kevrsc.sensor.storage;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class DatabaseMigratorTest {

    @Container
    private static final PostgreSQLContainer<?> TIMESCALE = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:latest-pg17").asCompatibleSubstituteFor("postgres"));

    @Test
    void createsMeasurementHypertables() throws Exception {
        DatabaseMigrator.migrate(TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword());

        var hypertables = new ArrayList<String>();
        try (var connection = DriverManager.getConnection(
                TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword());
             var rows = connection.createStatement().executeQuery(
                     "SELECT hypertable_name FROM timescaledb_information.hypertables ORDER BY hypertable_name")) {
            while (rows.next()) {
                hypertables.add(rows.getString(1));
            }
        }

        assertEquals(List.of("device_status", "moisture_readings", "sensor_readings"), hypertables);
    }
}
