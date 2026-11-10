package com.example.sensor;

public record EnrichedMoistureReading(MoistureReading reading, int expectedPercent) {
}
