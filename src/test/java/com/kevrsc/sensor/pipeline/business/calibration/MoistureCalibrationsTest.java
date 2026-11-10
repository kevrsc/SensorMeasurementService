package com.kevrsc.sensor.pipeline.business.calibration;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MoistureCalibrationsTest {

    private static final MoistureCalibration DEFAULT = new MoistureCalibration(3500, 1550);
    private static final MoistureCalibration PROBE_1 = new MoistureCalibration(3400, 1600);

    private final MoistureCalibrations calibrations = new MoistureCalibrations(DEFAULT,
            Map.of("vermiculture-esp32", Map.of(1, PROBE_1)));

    @Test
    void usesProbeSpecificCalibrationWhenConfigured() {
        assertEquals(PROBE_1, calibrations.forProbe("vermiculture-esp32", 1));
    }

    @Test
    void fallsBackToDefaultForUnconfiguredProbeOrDevice() {
        assertEquals(DEFAULT, calibrations.forProbe("vermiculture-esp32", 0));
        assertEquals(DEFAULT, calibrations.forProbe("greenhouse-esp32", 1));
    }
}
