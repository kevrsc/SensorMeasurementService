package com.kevrsc.sensor;

public record MoistureReading(int index, int pin, int raw, int percent) {
}
