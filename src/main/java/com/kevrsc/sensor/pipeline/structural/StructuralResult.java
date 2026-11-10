package com.kevrsc.sensor.pipeline.structural;

import com.kevrsc.sensor.event.SensorMeasurementEvent;

import java.util.List;

public sealed interface StructuralResult {

    record Valid(SensorMeasurementEvent event) implements StructuralResult {
    }

    record Invalid(List<String> errors) implements StructuralResult {
    }
}
