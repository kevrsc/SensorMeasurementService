package com.kevrsc.sensor.event;

public record MoistureReading(int index, int pin, int raw, int percent) {
}
