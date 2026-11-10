package com.kevrsc.sensor.pipeline.persistence;

import com.kevrsc.sensor.pipeline.business.EnrichedMeasurementEvent;

import java.util.List;

public interface MeasurementWriter {

    void write(List<EnrichedMeasurementEvent> events);
}
