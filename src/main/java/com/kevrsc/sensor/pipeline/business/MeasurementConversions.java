package com.kevrsc.sensor.pipeline.business;

public final class MeasurementConversions {

    private MeasurementConversions() {
    }

    public static double fahrenheit(double celsius) {
        return Math.round((celsius * 9.0 / 5.0 + 32.0) * 10.0) / 10.0;
    }
}
