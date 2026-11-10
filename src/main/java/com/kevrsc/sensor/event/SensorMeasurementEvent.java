package com.kevrsc.sensor.event;

import java.time.OffsetDateTime;
import java.util.List;

public record SensorMeasurementEvent(
        String device,
        List<SensorReading> sensors,
        List<MoistureReading> moisture,
        DeviceStatus status,
        OffsetDateTime receivedAt) {

    public SensorMeasurementEvent {
        sensors = sensors == null ? List.of() : sensors;
        moisture = moisture == null ? List.of() : moisture;
    }
}
