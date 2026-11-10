package com.example.sensor;

public record Outlier(Kind kind, int index, String reason) {

    public enum Kind { SENSOR, MOISTURE }

    public static Outlier sensor(int index, String reason) {
        return new Outlier(Kind.SENSOR, index, reason);
    }

    public static Outlier moisture(int index, String reason) {
        return new Outlier(Kind.MOISTURE, index, reason);
    }
}
