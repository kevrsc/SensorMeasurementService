package com.example.sensor;

public record EnrichedSensorReading(SensorReading reading, double fahrenheit) {
}
