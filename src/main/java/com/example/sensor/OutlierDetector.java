package com.example.sensor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class OutlierDetector {

    private static final double MIN_FAHRENHEIT = 0.0;
    private static final double MAX_FAHRENHEIT = 100.0;
    private static final int MIN_MOISTURE_PERCENT = 0;
    private static final int MAX_MOISTURE_PERCENT = 100;
    private static final int MOISTURE_DRY_RAW = 3500;
    private static final int MOISTURE_WET_RAW = 1550;
    private static final int MOISTURE_PERCENT_TOLERANCE = 1;

    public List<Outlier> detect(SensorMeasurementEvent event) {
        var outliers = new ArrayList<Outlier>();
        for (var reading : event.sensors()) {
            var fahrenheit = Math.round((reading.celsius() * 9.0 / 5.0 + 32.0) * 10.0) / 10.0;
            if (fahrenheit < MIN_FAHRENHEIT || fahrenheit > MAX_FAHRENHEIT) {
                outliers.add(Outlier.sensor(reading.index(), String.format(Locale.ROOT,
                        "temperature %.1fF is outside 0F-100F", fahrenheit)));
            }
        }
        for (var reading : event.moisture()) {
            if (reading.percent() < MIN_MOISTURE_PERCENT || reading.percent() > MAX_MOISTURE_PERCENT) {
                outliers.add(Outlier.moisture(reading.index(),
                        "percent " + reading.percent() + " is outside 0-100; probe needs recalibration"));
            }
            var expectedPercent = Math.round(
                    (MOISTURE_DRY_RAW - reading.raw()) * 100.0 / (MOISTURE_DRY_RAW - MOISTURE_WET_RAW));
            if (Math.abs(expectedPercent - reading.percent()) > MOISTURE_PERCENT_TOLERANCE) {
                outliers.add(Outlier.moisture(reading.index(),
                        "percent " + reading.percent() + " does not match raw " + reading.raw()
                                + " (expected " + expectedPercent + "); check device calibration"));
            }
        }
        return outliers;
    }
}
