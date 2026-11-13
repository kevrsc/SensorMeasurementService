package com.kevrsc.sensor.storage;

import com.kevrsc.sensor.event.DeviceStatus;
import com.kevrsc.sensor.event.MoistureReading;
import com.kevrsc.sensor.event.SensorMeasurementEvent;
import com.kevrsc.sensor.event.SensorReading;
import com.kevrsc.sensor.pipeline.business.BusinessValidationStage;
import com.kevrsc.sensor.pipeline.business.calibration.MoistureCalibration;
import com.kevrsc.sensor.pipeline.business.calibration.MoistureCalibrations;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class TimescaleMeasurementWriterTest {

    @Container
    private static final PostgreSQLContainer<?> TIMESCALE = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:latest-pg17").asCompatibleSubstituteFor("postgres"));

    private static final BusinessValidationStage STAGE_2 = new BusinessValidationStage(MoistureCalibrations.defaultOnly(new MoistureCalibration(3500, 1550)));

    private TimescaleMeasurementWriter writer() {
        return new TimescaleMeasurementWriter(TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword());
    }

    @BeforeAll
    static void migrate() {
        DatabaseMigrator.migrate(TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword());
    }

    @Test
    void writesReadingsMetadataOutliersAndStatus() throws Exception {
        writer().write(List.of(STAGE_2.process(sampleEvent("device-a"))));

        try (var db = connect()) {
            assertEquals(2, count(db, "sensor_readings", "device-a"));
            assertEquals(1, count(db, "moisture_readings", "device-a"));
            assertEquals(1, count(db, "device_status", "device-a"));

            try (var rows = db.createStatement().executeQuery("""
                    SELECT fahrenheit, outlier_reasons FROM sensor_readings
                    WHERE device = 'device-a' AND sensor_index = 2""")) {
                rows.next();
                assertEquals(104.0, rows.getDouble(1));
                assertArrayEquals(new String[]{"temperature 104.0F is outside 0F-100F"},
                        (String[]) rows.getArray(2).getArray());
            }
            try (var rows = db.createStatement().executeQuery("""
                    SELECT expected_percent, outlier_reasons FROM moisture_readings WHERE device = 'device-a'""")) {
                rows.next();
                assertEquals(83, rows.getInt(1));
                assertEquals(List.of(), Arrays.asList((String[]) rows.getArray(2).getArray()));
            }
        }
    }

    @Test
    void replayedBatchDoesNotDuplicateRows() throws Exception {
        var batch = List.of(STAGE_2.process(sampleEvent("device-b")));

        writer().write(batch);
        writer().write(batch);

        try (var db = connect()) {
            assertEquals(2, count(db, "sensor_readings", "device-b"));
            assertEquals(1, count(db, "moisture_readings", "device-b"));
            assertEquals(1, count(db, "device_status", "device-b"));
        }
    }

    private static SensorMeasurementEvent sampleEvent(String device) {
        return new SensorMeasurementEvent(device,
                List.of(new SensorReading(0, "28107bd400000057", 18.06),
                        new SensorReading(2, "284df1d600000042", 40.0)),
                List.of(new MoistureReading(0, 35, 1879, 83)),
                new DeviceStatus(229504121L, "POWERON", 3, -58, "192.168.1.20", 2, 255, 256L,
                        187372L, 146388L, "0123456789abcdeffedcba9876543210", "disabled", null, 4),
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword());
    }

    private static int count(Connection db, String table, String device) throws SQLException {
        try (var statement = db.prepareStatement("SELECT count(*) FROM " + table + " WHERE device = ?")) {
            statement.setString(1, device);
            try (var rows = statement.executeQuery()) {
                rows.next();
                return rows.getInt(1);
            }
        }
    }
}
