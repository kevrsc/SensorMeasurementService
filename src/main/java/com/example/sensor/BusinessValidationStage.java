package com.example.sensor;

public class BusinessValidationStage {

    private final OutlierDetector outlierDetector;

    public BusinessValidationStage(OutlierDetector outlierDetector) {
        this.outlierDetector = outlierDetector;
    }

    public EnrichedMeasurementEvent process(SensorMeasurementEvent event) {
        return new EnrichedMeasurementEvent(event, outlierDetector.detect(event));
    }
}
