package com.example.sensor;

import java.util.Map;

public record MoistureCalibrations(
        MoistureCalibration defaultCalibration,
        Map<String, Map<Integer, MoistureCalibration>> byDeviceAndProbe) {

    public MoistureCalibrations {
        byDeviceAndProbe = byDeviceAndProbe == null ? Map.of() : Map.copyOf(byDeviceAndProbe);
    }

    public static MoistureCalibrations defaultOnly(MoistureCalibration calibration) {
        return new MoistureCalibrations(calibration, Map.of());
    }

    public MoistureCalibration forProbe(String device, int probeIndex) {
        return byDeviceAndProbe.getOrDefault(device, Map.of()).getOrDefault(probeIndex, defaultCalibration);
    }
}
