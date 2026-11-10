package com.kevrsc.sensor.event;

public record DeviceStatus(
        long uptimeMs,
        String resetReason,
        int bootCount,
        int rssiDbm,
        String ip,
        int wifiReconnects,
        int postFailures,
        long seq,
        long freeHeap,
        long minFreeHeap,
        String sketchMd5,
        String keepalive,
        Long keepaliveAgeMs,
        int probeCount) {
}
