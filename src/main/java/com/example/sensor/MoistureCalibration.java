package com.example.sensor;

public record MoistureCalibration(int dryRaw, int wetRaw) {

    public MoistureCalibration {
        if (dryRaw <= wetRaw) {
            throw new IllegalArgumentException(
                    "dry raw (" + dryRaw + ") must be higher than wet raw (" + wetRaw + "); higher raw is drier");
        }
    }

    public int expectedPercent(int raw) {
        return (int) Math.round((dryRaw - raw) * 100.0 / (dryRaw - wetRaw));
    }
}
