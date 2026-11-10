package com.kevrsc.sensor.pipeline.business;

import com.kevrsc.sensor.event.SensorReading;

public record EnrichedSensorReading(SensorReading reading, double fahrenheit) {
}
