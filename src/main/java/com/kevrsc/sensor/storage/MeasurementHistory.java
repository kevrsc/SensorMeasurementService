package com.kevrsc.sensor.storage;

import com.kevrsc.sensor.event.SensorMeasurementEvent;

import java.util.List;
import java.util.Optional;

public interface MeasurementHistory {

    List<SensorMeasurementEvent> history();

    Optional<SensorMeasurementEvent> latest();
}
