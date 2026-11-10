package com.kevrsc.sensor.event;

public record SensorReading(int index, String address, double celsius) {
}
