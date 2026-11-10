package com.kevrsc.sensor;

import java.util.List;

public class StructuralValidationStage {

    private final SensorEventParser parser;
    private final StructuralValidator validator;

    public StructuralValidationStage(SensorEventParser parser, StructuralValidator validator) {
        this.parser = parser;
        this.validator = validator;
    }

    public StructuralResult process(String json) {
        SensorMeasurementEvent event;
        try {
            event = parser.parse(json);
        } catch (MalformedEventException e) {
            return new StructuralResult.Invalid(List.of(e.getMessage()));
        }
        var errors = validator.validate(event);
        return errors.isEmpty() ? new StructuralResult.Valid(event) : new StructuralResult.Invalid(errors);
    }
}
