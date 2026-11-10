package com.kevrsc.sensor;

import java.util.List;
import java.util.Optional;

public interface MeasurementHistory {

    List<SensorMeasurementEvent> history();

    Optional<SensorMeasurementEvent> latest();
}
