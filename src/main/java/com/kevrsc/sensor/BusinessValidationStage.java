package com.kevrsc.sensor;

public class BusinessValidationStage {

    private final OutlierDetector outlierDetector;
    private final MoistureCalibrations calibrations;

    public BusinessValidationStage(MoistureCalibrations calibrations) {
        this.outlierDetector = new OutlierDetector(calibrations);
        this.calibrations = calibrations;
    }

    public EnrichedMeasurementEvent process(SensorMeasurementEvent event) {
        var sensors = event.sensors().stream()
                .map(r -> new EnrichedSensorReading(r, MeasurementConversions.fahrenheit(r.celsius())))
                .toList();
        var moisture = event.moisture().stream()
                .map(r -> new EnrichedMoistureReading(r,
                        calibrations.forProbe(event.device(), r.index()).expectedPercent(r.raw())))
                .toList();
        return new EnrichedMeasurementEvent(event, sensors, moisture, outlierDetector.detect(event));
    }
}
