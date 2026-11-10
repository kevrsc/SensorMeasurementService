package com.example.sensor;

public final class MeasurementConversions {

    static final int MOISTURE_DRY_RAW = 3500;
    static final int MOISTURE_WET_RAW = 1550;

    private MeasurementConversions() {
    }

    public static double fahrenheit(double celsius) {
        return Math.round((celsius * 9.0 / 5.0 + 32.0) * 10.0) / 10.0;
    }

    public static int expectedMoisturePercent(int raw) {
        return (int) Math.round((MOISTURE_DRY_RAW - raw) * 100.0 / (MOISTURE_DRY_RAW - MOISTURE_WET_RAW));
    }
}
