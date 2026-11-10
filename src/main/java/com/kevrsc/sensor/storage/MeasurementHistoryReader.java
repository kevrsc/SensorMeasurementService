package com.kevrsc.sensor.storage;

import com.kevrsc.sensor.event.DeviceStatus;
import com.kevrsc.sensor.event.MoistureReading;
import com.kevrsc.sensor.event.SensorMeasurementEvent;
import com.kevrsc.sensor.event.SensorReading;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class MeasurementHistoryReader implements MeasurementHistory {

    private static final String SENSORS = """
            SELECT time, device, sensor_index, address, celsius FROM sensor_readings %s
            ORDER BY time, device, sensor_index""";

    private static final String MOISTURE = """
            SELECT time, device, probe_index, pin, raw, percent FROM moisture_readings %s
            ORDER BY time, device, probe_index""";

    private static final String STATUS = """
            SELECT time, device, uptime_ms, reset_reason, boot_count, rssi_dbm, ip, wifi_reconnects, post_failures,
                   seq, free_heap, min_free_heap, sketch_md5, keepalive, keepalive_age_ms, probe_count
            FROM device_status %s""";

    private static final String LATEST = """
            SELECT time, device FROM (
                SELECT time, device FROM sensor_readings
                UNION ALL SELECT time, device FROM moisture_readings
                UNION ALL SELECT time, device FROM device_status) AS events
            ORDER BY time DESC, device DESC
            LIMIT 1""";

    private static final String ONE_EVENT = "WHERE time = ? AND device = ?";

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final ZoneId zone;

    public MeasurementHistoryReader(String jdbcUrl, String user, String password, ZoneId zone) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.zone = zone;
    }

    @Override
    public List<SensorMeasurementEvent> history() {
        try (var db = connect()) {
            return load(db, "", null);
        } catch (SQLException e) {
            throw new IllegalStateException("failed to read measurement history", e);
        }
    }

    @Override
    public Optional<SensorMeasurementEvent> latest() {
        try (var db = connect(); var rows = db.createStatement().executeQuery(LATEST)) {
            if (!rows.next()) {
                return Optional.empty();
            }
            var key = new EventKey(rows.getObject(1, OffsetDateTime.class).toInstant(), rows.getString(2));
            return load(db, ONE_EVENT, key).stream().findFirst();
        } catch (SQLException e) {
            throw new IllegalStateException("failed to read latest measurement", e);
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, user, password);
    }

    private List<SensorMeasurementEvent> load(Connection db, String filter, EventKey only) throws SQLException {
        var events = new TreeMap<EventKey, PartialEvent>(
                Comparator.comparing(EventKey::time).thenComparing(EventKey::device));

        query(db, SENSORS.formatted(filter), only, rows -> partial(events, rows).sensors.add(
                new SensorReading(rows.getInt(3), rows.getString(4), rows.getDouble(5))));
        query(db, MOISTURE.formatted(filter), only, rows -> partial(events, rows).moisture.add(
                new MoistureReading(rows.getInt(3), rows.getInt(4), rows.getInt(5), rows.getInt(6))));
        query(db, STATUS.formatted(filter), only, rows -> partial(events, rows).status = new DeviceStatus(
                rows.getLong(3), rows.getString(4), rows.getInt(5), rows.getInt(6), rows.getString(7),
                rows.getInt(8), rows.getInt(9), rows.getLong(10), rows.getLong(11), rows.getLong(12),
                rows.getString(13), rows.getString(14), rows.getObject(15, Long.class), rows.getInt(16)));

        var result = new ArrayList<SensorMeasurementEvent>(events.size());
        for (Map.Entry<EventKey, PartialEvent> entry : events.entrySet()) {
            var event = entry.getValue();
            result.add(new SensorMeasurementEvent(entry.getKey().device(), List.copyOf(event.sensors),
                    List.copyOf(event.moisture), event.status, entry.getKey().time().atZone(zone).toOffsetDateTime()));
        }
        return result;
    }

    private static PartialEvent partial(Map<EventKey, PartialEvent> events, ResultSet rows) throws SQLException {
        var key = new EventKey(rows.getObject(1, OffsetDateTime.class).toInstant(), rows.getString(2));
        return events.computeIfAbsent(key, k -> new PartialEvent());
    }

    private static void query(Connection db, String sql, EventKey only, RowHandler handler) throws SQLException {
        try (var statement = db.prepareStatement(sql)) {
            if (only != null) {
                statement.setObject(1, only.time().atOffset(ZoneOffset.UTC));
                statement.setString(2, only.device());
            }
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    handler.handle(rows);
                }
            }
        }
    }

    @FunctionalInterface
    private interface RowHandler {
        void handle(ResultSet rows) throws SQLException;
    }

    private record EventKey(Instant time, String device) {
    }

    private static final class PartialEvent {
        private final List<SensorReading> sensors = new ArrayList<>();
        private final List<MoistureReading> moisture = new ArrayList<>();
        private DeviceStatus status;
    }
}
