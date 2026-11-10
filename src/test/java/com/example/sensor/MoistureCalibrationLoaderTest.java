package com.example.sensor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MoistureCalibrationLoaderTest {

    @TempDir
    Path tempDir;

    @Test
    void loadsDefaultAndPerProbeCalibrations() throws Exception {
        var file = tempDir.resolve("moisture-calibration.json");
        Files.writeString(file, """
                {
                  "default": { "dry_raw": 3500, "wet_raw": 1550 },
                  "devices": {
                    "vermiculture-esp32": { "1": { "dry_raw": 3400, "wet_raw": 1600 } }
                  }
                }
                """);

        var calibrations = MoistureCalibrationLoader.load(file);

        assertEquals(new MoistureCalibration(3500, 1550), calibrations.forProbe("vermiculture-esp32", 0));
        assertEquals(new MoistureCalibration(3400, 1600), calibrations.forProbe("vermiculture-esp32", 1));
    }

    @Test
    void loadsTheCalibrationFileShippedWithTheProject() {
        var calibrations = MoistureCalibrationLoader.loadResource("moisture-calibration.json");

        assertEquals(new MoistureCalibration(3500, 1550), calibrations.forProbe("vermiculture-esp32", 0));
        assertEquals(new MoistureCalibration(3500, 1550), calibrations.defaultCalibration());
    }

    @Test
    void rejectsMissingClasspathResource() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> MoistureCalibrationLoader.loadResource("no-such-calibration.json"));
        assertEquals("moisture calibration resource no-such-calibration.json not found on classpath", thrown.getMessage());
    }

    @Test
    void rejectsFileWithoutDefault() throws Exception {
        var file = tempDir.resolve("moisture-calibration.json");
        Files.writeString(file, """
                { "devices": {} }
                """);

        var thrown = assertThrows(IllegalArgumentException.class, () -> MoistureCalibrationLoader.load(file));
        assertEquals("moisture calibration file " + file + " must define \"default\"", thrown.getMessage());
    }
}
