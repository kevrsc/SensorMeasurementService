package com.kevrsc.sensor;

public record EnrichedMoistureReading(MoistureReading reading, int expectedPercent) {
}
