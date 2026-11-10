package com.kevrsc.sensor;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class BusinessValidationStageTest {

    private final BusinessValidationStage stage = new BusinessValidationStage(MoistureCalibrations.defaultOnly(new MoistureCalibration(3500, 1550)));

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

    @Test
    void usesEachProbesOwnCalibration() {
        var calibrations = new MoistureCalibrations(new MoistureCalibration(3500, 1550),
                Map.of("vermiculture-esp32", Map.of(1, new MoistureCalibration(4000, 2000))));
        var probe0 = new MoistureReading(0, 35, 1879, 83);
        var probe1 = new MoistureReading(1, 34, 3000, 50);
        var event = new SensorMeasurementEvent("vermiculture-esp32", List.of(), List.of(probe0, probe1), null,
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));

        var enriched = new BusinessValidationStage(calibrations).process(event);

        assertEquals(List.of(new EnrichedMoistureReading(probe0, 83), new EnrichedMoistureReading(probe1, 50)),
                enriched.moisture());
        assertEquals(List.of(), enriched.outliers());
    }

    @Test
    void addsFahrenheitAndExpectedMoisturePercentMetadata() {
        var sensor = new SensorReading(0, "2800000000000001", 18.06);
        var probe = new MoistureReading(0, 35, 1879, 83);
        var event = new SensorMeasurementEvent("vermiculture-esp32", List.of(sensor), List.of(probe), null,
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));

        var enriched = stage.process(event);

        assertEquals(List.of(new EnrichedSensorReading(sensor, 64.5)), enriched.sensors());
        assertEquals(List.of(new EnrichedMoistureReading(probe, 83)), enriched.moisture());
    }
}
