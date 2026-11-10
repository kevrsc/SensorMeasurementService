package com.example.sensor;

import java.util.List;

public interface MeasurementWriter {

    void write(List<EnrichedMeasurementEvent> events);
}
