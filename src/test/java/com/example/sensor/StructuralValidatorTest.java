package com.example.sensor;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StructuralValidatorTest {

    private final StructuralValidator validator = new StructuralValidator();

    @Test
    void acceptsValidEvent() {
        var event = eventForDevice("vermiculture-esp32");

        assertEquals(List.of(), validator.validate(event));
    }

    @Test
    void rejectsBlankDevice() {
        var event = eventForDevice(" ");

        assertEquals(List.of("device is required"), validator.validate(event));
    }

    @Test
    void acceptsEmptySensorsAndMoisture() {
        var event = event("vermiculture-esp32", List.of(), List.of(),
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));

        assertEquals(List.of(), validator.validate(event));
    }

    @Test
    void rejectsMissingReceivedAt() {
        var event = event("vermiculture-esp32", List.of(new SensorReading(0, "2800000000000001", 18.06)), null);

        assertEquals(List.of("receivedAt is required"), validator.validate(event));
    }

    @Test
    void rejectsSensorWithBlankAddress() {
        var event = event("vermiculture-esp32", List.of(new SensorReading(2, " ", 18.06)));

        assertEquals(List.of("sensors[2].address is required"), validator.validate(event));
    }

    @Test
    void rejectsDuplicateSensorIndex() {
        var event = event("vermiculture-esp32", List.of(
                new SensorReading(0, "2800000000000001", 18.06),
                new SensorReading(1, "2800000000000002", 19.94),
                new SensorReading(1, "2800000000000003", 16.38)));

        assertEquals(List.of("sensors[1] index is duplicated"), validator.validate(event));
    }

    private static SensorMeasurementEvent eventForDevice(String device) {
        return event(device, List.of(new SensorReading(0, "2800000000000001", 18.06)));
    }

    private static SensorMeasurementEvent event(String device, List<SensorReading> sensors) {
        return event(device, sensors, OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));
    }

    private static SensorMeasurementEvent event(String device, List<SensorReading> sensors, OffsetDateTime receivedAt) {
        return event(device, sensors, List.of(new MoistureReading(0, 35, 1879, 83)), receivedAt);
    }

    private static SensorMeasurementEvent event(
            String device, List<SensorReading> sensors, List<MoistureReading> moisture, OffsetDateTime receivedAt) {
        return new SensorMeasurementEvent(
                device,
                sensors,
                moisture,
                new DeviceStatus(229504121L, "POWERON", 3, -58, "192.168.1.20", 2, 255, 256L,
                        187372L, 146388L, "0123456789abcdeffedcba9876543210", "disabled", null, 4),
                receivedAt);
    }
}
