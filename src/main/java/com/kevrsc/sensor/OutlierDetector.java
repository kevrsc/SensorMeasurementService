package com.kevrsc.sensor;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class OutlierDetector {

    private static final double MIN_FAHRENHEIT = 0.0;
    private static final double MAX_FAHRENHEIT = 100.0;
    private static final int MIN_MOISTURE_PERCENT = 0;
    private static final int MAX_MOISTURE_PERCENT = 100;
    private static final int MOISTURE_PERCENT_TOLERANCE = 1;

    private final MoistureCalibrations calibrations;

    public OutlierDetector(MoistureCalibrations calibrations) {
        this.calibrations = calibrations;
    }

    public List<Outlier> detect(SensorMeasurementEvent event) {
        var outliers = new ArrayList<Outlier>();
        for (var reading : event.sensors()) {
            var fahrenheit = MeasurementConversions.fahrenheit(reading.celsius());
            if (fahrenheit < MIN_FAHRENHEIT || fahrenheit > MAX_FAHRENHEIT) {
                outliers.add(Outlier.sensor(reading.index(), String.format(Locale.ROOT,
                        "temperature %.1fF is outside 0F-100F", fahrenheit)));
            }
        }
        for (var reading : event.moisture()) {
            var calibration = calibrations.forProbe(event.device(), reading.index());
            if (reading.raw() < calibration.wetRaw()) {
                outliers.add(Outlier.moisture(reading.index(), "raw " + reading.raw()
                        + " is wetter than the calibrated wet limit " + calibration.wetRaw() + "; probe needs recalibration"));
            } else if (reading.raw() > calibration.dryRaw()) {
                outliers.add(Outlier.moisture(reading.index(), "raw " + reading.raw()
                        + " is drier than the calibrated dry limit " + calibration.dryRaw() + "; probe needs recalibration"));
            }
            // The board caps percent at 0-100, so compare against the capped expectation.
            var expectedPercent = Math.clamp(calibration.expectedPercent(reading.raw()),
                    MIN_MOISTURE_PERCENT, MAX_MOISTURE_PERCENT);
            if (Math.abs(expectedPercent - reading.percent()) > MOISTURE_PERCENT_TOLERANCE) {
                outliers.add(Outlier.moisture(reading.index(),
                        "percent " + reading.percent() + " does not match raw " + reading.raw()
                                + " (expected " + expectedPercent + "); check device calibration"));
            }
        }
        return outliers;
    }
}
