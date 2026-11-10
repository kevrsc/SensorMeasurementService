package com.kevrsc.sensor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MoistureCalibrationTest {

    @Test
    void mapsDryRawTo0AndWetRawTo100() {
        var calibration = new MoistureCalibration(4000, 2000);

        assertEquals(0, calibration.expectedPercent(4000));
        assertEquals(50, calibration.expectedPercent(3000));
        assertEquals(100, calibration.expectedPercent(2000));
    }

    @Test
    void matchesCurrentProbeCalibration() {
        assertEquals(83, new MoistureCalibration(3500, 1550).expectedPercent(1879));
    }

    @Test
    void rejectsDryRawThatIsNotHigherThanWetRaw() {
        assertThrows(IllegalArgumentException.class, () -> new MoistureCalibration(1550, 3500));
        assertThrows(IllegalArgumentException.class, () -> new MoistureCalibration(2000, 2000));
    }
}
