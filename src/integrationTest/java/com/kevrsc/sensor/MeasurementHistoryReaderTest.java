package com.kevrsc.sensor;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MeasurementHistoryReaderTest {

    @Container
    private static final PostgreSQLContainer<?> TIMESCALE = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb:latest-pg17").asCompatibleSubstituteFor("postgres"));

    private static final BusinessValidationStage STAGE_2 = new BusinessValidationStage(
            MoistureCalibrations.defaultOnly(new MoistureCalibration(3500, 1550)));

    @BeforeAll
    static void migrate() {
        DatabaseMigrator.migrate(TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword());
    }

    @Test
    @Order(1)
    void emptyDatabaseHasNoHistoryOrLatest() {
        assertEquals(List.of(), reader().history());
        assertEquals(Optional.empty(), reader().latest());
    }

    @Test
    @Order(2)
    void rebuildsStoredEventsOldestFirst() {
        var withEverything = new SensorMeasurementEvent("vermiculture-esp32",
                List.of(new SensorReading(0, "2800000000000001", 18.06),
                        new SensorReading(1, "2800000000000002", 19.94)),
                List.of(new MoistureReading(0, 35, 1879, 83)),
                new DeviceStatus(229504121L, "POWERON", 3, -58, "192.168.1.20", 2, 255, 256L,
                        187372L, 146388L, "0123456789abcdeffedcba9876543210", "disabled", null, 4),
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));
        var sensorsOnly = new SensorMeasurementEvent("vermiculture-esp32",
                List.of(new SensorReading(0, "2800000000000001", 18.5)),
                List.of(), null,
                OffsetDateTime.parse("2026-11-09T11:41:34-08:00"));
        var statusOnly = new SensorMeasurementEvent("vermiculture-esp32",
                List.of(), List.of(),
                new DeviceStatus(230404121L, "POWERON", 3, -60, "192.168.1.20", 2, 255, 257L,
                        187000L, 146388L, "0123456789abcdeffedcba9876543210", "ok", 1200L, 0),
                OffsetDateTime.parse("2026-11-09T12:11:34-08:00"));

        new TimescaleMeasurementWriter(TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword())
                .write(List.of(STAGE_2.process(withEverything), STAGE_2.process(sensorsOnly), STAGE_2.process(statusOnly)));

        assertEquals(List.of(sensorsOnly, withEverything, statusOnly), reader().history());
        assertEquals(Optional.of(statusOnly), reader().latest());
    }

    private static MeasurementHistoryReader reader() {
        return new MeasurementHistoryReader(TIMESCALE.getJdbcUrl(), TIMESCALE.getUsername(), TIMESCALE.getPassword(),
                ZoneId.of("America/Los_Angeles"));
    }
}
