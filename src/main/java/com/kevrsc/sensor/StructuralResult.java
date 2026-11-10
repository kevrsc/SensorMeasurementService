package com.kevrsc.sensor;

import java.util.List;

public sealed interface StructuralResult {

    record Valid(SensorMeasurementEvent event) implements StructuralResult {
    }

    record Invalid(List<String> errors) implements StructuralResult {
    }
}
