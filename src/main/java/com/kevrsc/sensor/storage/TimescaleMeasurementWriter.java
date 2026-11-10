package com.kevrsc.sensor.storage;

import com.kevrsc.sensor.event.SensorMeasurementEvent;
import com.kevrsc.sensor.pipeline.business.EnrichedMeasurementEvent;
import com.kevrsc.sensor.pipeline.business.Outlier;
import com.kevrsc.sensor.pipeline.persistence.MeasurementWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class TimescaleMeasurementWriter implements MeasurementWriter {

    private static final String INSERT_SENSOR = """
            INSERT INTO sensor_readings (time, device, sensor_index, address, celsius, fahrenheit, outlier_reasons)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING""";

    private static final String INSERT_MOISTURE = """
            INSERT INTO moisture_readings (time, device, probe_index, pin, raw, percent, expected_percent, outlier_reasons)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING""";

    private static final String INSERT_STATUS = """
            INSERT INTO device_status (time, device, seq, uptime_ms, reset_reason, boot_count, rssi_dbm, ip,
                                       wifi_reconnects, post_failures, free_heap, min_free_heap, sketch_md5,
                                       keepalive, keepalive_age_ms, probe_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING""";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public TimescaleMeasurementWriter(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    @Override
    public void write(List<EnrichedMeasurementEvent> events) {
        try (var db = DriverManager.getConnection(jdbcUrl, user, password)) {
            db.setAutoCommit(false);
            try (var sensors = db.prepareStatement(INSERT_SENSOR);
                 var moisture = db.prepareStatement(INSERT_MOISTURE);
                 var status = db.prepareStatement(INSERT_STATUS)) {
                for (var enriched : events) {
                    addSensorRows(db, sensors, enriched);
                    addMoistureRows(db, moisture, enriched);
                    addStatusRow(status, enriched.event());
                }
                sensors.executeBatch();
                moisture.executeBatch();
                status.executeBatch();
                db.commit();
            } catch (SQLException e) {
                db.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("failed to write " + events.size() + " measurement events", e);
        }
    }

    private static void addSensorRows(Connection db, PreparedStatement insert, EnrichedMeasurementEvent enriched)
            throws SQLException {
        var event = enriched.event();
        for (var sensor : enriched.sensors()) {
            var reading = sensor.reading();
            insert.setObject(1, event.receivedAt());
            insert.setString(2, event.device());
            insert.setInt(3, reading.index());
            insert.setString(4, reading.address());
            insert.setDouble(5, reading.celsius());
            insert.setDouble(6, sensor.fahrenheit());
            insert.setArray(7, db.createArrayOf("text", reasons(enriched, Outlier.Kind.SENSOR, reading.index())));
            insert.addBatch();
        }
    }

    private static void addMoistureRows(Connection db, PreparedStatement insert, EnrichedMeasurementEvent enriched)
            throws SQLException {
        var event = enriched.event();
        for (var probe : enriched.moisture()) {
            var reading = probe.reading();
            insert.setObject(1, event.receivedAt());
            insert.setString(2, event.device());
            insert.setInt(3, reading.index());
            insert.setInt(4, reading.pin());
            insert.setInt(5, reading.raw());
            insert.setInt(6, reading.percent());
            insert.setInt(7, probe.expectedPercent());
            insert.setArray(8, db.createArrayOf("text", reasons(enriched, Outlier.Kind.MOISTURE, reading.index())));
            insert.addBatch();
        }
    }

    private static void addStatusRow(PreparedStatement insert, SensorMeasurementEvent event) throws SQLException {
        var status = event.status();
        if (status == null) {
            return;
        }
        insert.setObject(1, event.receivedAt());
        insert.setString(2, event.device());
        insert.setLong(3, status.seq());
        insert.setLong(4, status.uptimeMs());
        insert.setString(5, status.resetReason());
        insert.setInt(6, status.bootCount());
        insert.setInt(7, status.rssiDbm());
        insert.setString(8, status.ip());
        insert.setInt(9, status.wifiReconnects());
        insert.setInt(10, status.postFailures());
        insert.setLong(11, status.freeHeap());
        insert.setLong(12, status.minFreeHeap());
        insert.setString(13, status.sketchMd5());
        insert.setString(14, status.keepalive());
        insert.setObject(15, status.keepaliveAgeMs(), Types.BIGINT);
        insert.setInt(16, status.probeCount());
        insert.addBatch();
    }

    private static String[] reasons(EnrichedMeasurementEvent enriched, Outlier.Kind kind, int index) {
        return enriched.outliers().stream()
                .filter(o -> o.kind() == kind && o.index() == index)
                .map(Outlier::reason)
                .toArray(String[]::new);
    }
}
