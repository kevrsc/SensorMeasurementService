package com.kevrsc.sensor.pipeline.business;

import com.kevrsc.sensor.event.SensorMeasurementEvent;

import java.util.List;

public record EnrichedMeasurementEvent(
        SensorMeasurementEvent event,
        List<EnrichedSensorReading> sensors,
        List<EnrichedMoistureReading> moisture,
        List<Outlier> outliers) {
}
