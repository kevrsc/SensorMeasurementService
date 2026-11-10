package com.kevrsc.sensor;

import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OutlierDetectorTest {

    private final OutlierDetector detector = new OutlierDetector(MoistureCalibrations.defaultOnly(new MoistureCalibration(3500, 1550)));

    @Test
    void flagsTemperatureAbove100F() {
        var event = eventWithSensors(new SensorReading(2, "2800000000000003", 40.0));

        assertEquals(List.of(Outlier.sensor(2, "temperature 104.0F is outside 0F-100F")), detector.detect(event));
    }

    @Test
    void flagsTemperatureBelow0F() {
        var event = eventWithSensors(new SensorReading(1, "2800000000000002", -20.0));

        assertEquals(List.of(Outlier.sensor(1, "temperature -4.0F is outside 0F-100F")), detector.detect(event));
    }

    @Test
    void treatsTemperatureBoundariesAsNormal() {
        var event = eventWithSensors(
                new SensorReading(0, "2800000000000001", (0.0 - 32.0) * 5.0 / 9.0),
                new SensorReading(1, "2800000000000002", (100.0 - 32.0) * 5.0 / 9.0));

        assertEquals(List.of(), detector.detect(event));
    }

    @Test
    void roundsToTenthOfDegreeBeforeComparing() {
        var event = eventWithSensors(
                new SensorReading(0, "2800000000000001", -17.78),
                new SensorReading(1, "2800000000000002", 37.78));

        assertEquals(List.of(), detector.detect(event));
    }

    @Test
    void flagsMoistureAbove100PercentAsNeedingRecalibration() {
        var event = eventWithMoisture(new MoistureReading(0, 35, 1258, 115));

        assertEquals(List.of(Outlier.moisture(0, "percent 115 is outside 0-100; probe needs recalibration")),
                detector.detect(event));
    }

    @Test
    void flagsMoistureBelow0PercentAsNeedingRecalibration() {
        var event = eventWithMoisture(new MoistureReading(0, 35, 3695, -10));

        assertEquals(List.of(Outlier.moisture(0, "percent -10 is outside 0-100; probe needs recalibration")),
                detector.detect(event));
    }

    @Test
    void flagsMoisturePercentThatDoesNotMatchRawCalibration() {
        var event = eventWithMoisture(new MoistureReading(0, 35, 1300, 100));

        assertEquals(List.of(Outlier.moisture(0, "percent 100 does not match raw 1300 (expected 113); check device calibration")),
                detector.detect(event));
    }

    @Test
    void acceptsMoistureFromSamplePost() {
        var event = eventWithMoisture(new MoistureReading(0, 35, 1879, 83));

        assertEquals(List.of(), detector.detect(event));
    }

    private static SensorMeasurementEvent eventWithSensors(SensorReading... sensors) {
        return new SensorMeasurementEvent("vermiculture-esp32", List.of(sensors), List.of(), null,
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));
    }

    private static SensorMeasurementEvent eventWithMoisture(MoistureReading... moisture) {
        return new SensorMeasurementEvent("vermiculture-esp32", List.of(), List.of(moisture), null,
                OffsetDateTime.parse("2026-11-09T11:56:34-08:00"));
    }
}
