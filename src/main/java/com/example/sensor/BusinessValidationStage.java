package com.example.sensor;

public class BusinessValidationStage {

    private final OutlierDetector outlierDetector;

    public BusinessValidationStage(OutlierDetector outlierDetector) {
        this.outlierDetector = outlierDetector;
    }

    public EnrichedMeasurementEvent process(SensorMeasurementEvent event) {
        var sensors = event.sensors().stream()
                .map(r -> new EnrichedSensorReading(r, MeasurementConversions.fahrenheit(r.celsius())))
                .toList();
        var moisture = event.moisture().stream()
                .map(r -> new EnrichedMoistureReading(r, MeasurementConversions.expectedMoisturePercent(r.raw())))
                .toList();
        return new EnrichedMeasurementEvent(event, sensors, moisture, outlierDetector.detect(event));
    }
}
