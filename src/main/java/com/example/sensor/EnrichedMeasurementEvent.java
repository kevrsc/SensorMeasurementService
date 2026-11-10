package com.example.sensor;

import java.util.List;

public record EnrichedMeasurementEvent(
        SensorMeasurementEvent event,
        List<EnrichedSensorReading> sensors,
        List<EnrichedMoistureReading> moisture,
        List<Outlier> outliers) {
}
