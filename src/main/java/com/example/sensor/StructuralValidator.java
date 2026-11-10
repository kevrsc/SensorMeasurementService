package com.example.sensor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class StructuralValidator {

    public List<String> validate(SensorMeasurementEvent event) {
        var errors = new ArrayList<String>();
        if (event.device() == null || event.device().isBlank()) {
            errors.add("device is required");
        }
        var seen = new HashSet<Integer>();
        var duplicated = new HashSet<Integer>();
        for (var reading : event.sensors()) {
            if (!seen.add(reading.index()) && duplicated.add(reading.index())) {
                errors.add("sensors[" + reading.index() + "] index is duplicated");
            }
            if (reading.address() == null || reading.address().isBlank()) {
                errors.add("sensors[" + reading.index() + "].address is required");
            }
        }
        if (event.receivedAt() == null) {
            errors.add("receivedAt is required");
        }
        return errors;
    }
}
