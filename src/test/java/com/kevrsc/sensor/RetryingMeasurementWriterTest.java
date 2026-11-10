package com.kevrsc.sensor;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryingMeasurementWriterTest {

    private final List<Duration> sleeps = new ArrayList<>();

    @Test
    void retriesWithExponentialBackoffUntilWriteSucceeds() {
        var attempts = new int[1];
        MeasurementWriter flaky = events -> {
            if (++attempts[0] < 3) {
                throw new IllegalStateException("db down");
            }
        };
        var writer = new RetryingMeasurementWriter(flaky, 5, Duration.ofSeconds(1), Duration.ofSeconds(30), sleeps::add);

        writer.write(List.of());

        assertEquals(3, attempts[0]);
        assertEquals(List.of(Duration.ofSeconds(1), Duration.ofSeconds(2)), sleeps);
    }

    @Test
    void capsBackoffAndRethrowsAfterMaxAttempts() {
        MeasurementWriter broken = events -> {
            throw new IllegalStateException("db down");
        };
        var writer = new RetryingMeasurementWriter(broken, 4, Duration.ofSeconds(1), Duration.ofSeconds(3), sleeps::add);

        var thrown = assertThrows(IllegalStateException.class, () -> writer.write(List.of()));

        assertEquals("db down", thrown.getMessage());
        assertEquals(List.of(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(3)), sleeps);
    }
}
