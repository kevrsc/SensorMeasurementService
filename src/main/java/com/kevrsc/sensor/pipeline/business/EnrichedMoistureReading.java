package com.kevrsc.sensor.pipeline.business;

import com.kevrsc.sensor.event.MoistureReading;

public record EnrichedMoistureReading(MoistureReading reading, int expectedPercent) {
}
