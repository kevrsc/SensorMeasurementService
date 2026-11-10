package com.example.sensor;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class BusinessValidationStageTest {

    private final BusinessValidationStage stage = new BusinessValidationStage(new OutlierDetector());

    @Test
    void attachesOutliersToTheOriginalEvent() {
        var event = new SensorMeasurementEvent("vermiculture-esp32",
                List.of(new SensorReading(2, "2800000000000003", 40.0)),
                List.of(new MoistureReading(0, 35, 1879, 83)),
                null,
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));

        var enriched = stage.process(event);

        assertSame(event, enriched.event());
        assertEquals(List.of(Outlier.sensor(2, "temperature 104.0F is outside 0F-100F")), enriched.outliers());
    }
}
