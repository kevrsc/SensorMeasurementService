package com.example.sensor;

import java.util.List;

public record EnrichedMeasurementEvent(SensorMeasurementEvent event, List<Outlier> outliers) {
}
