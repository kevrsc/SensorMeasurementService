package com.kevrsc.sensor.pipeline.persistence;

import com.kevrsc.sensor.pipeline.business.EnrichedMeasurementEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class RetryingMeasurementWriter implements MeasurementWriter {

    private static final Logger LOG = LoggerFactory.getLogger(RetryingMeasurementWriter.class);

    @FunctionalInterface
    public interface Sleeper {
        void sleep(Duration duration) throws InterruptedException;
    }

    private final MeasurementWriter delegate;
    private final int maxAttempts;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final Sleeper sleeper;

    public RetryingMeasurementWriter(
            MeasurementWriter delegate, int maxAttempts, Duration initialBackoff, Duration maxBackoff, Sleeper sleeper) {
        this.delegate = delegate;
        this.maxAttempts = maxAttempts;
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.sleeper = sleeper;
    }

    @Override
    public void write(List<EnrichedMeasurementEvent> events) {
        var backoff = initialBackoff;
        for (int attempt = 1; ; attempt++) {
            try {
                delegate.write(events);
                if (attempt > 1) {
                    LOG.info("Write succeeded on attempt {}/{} for {} events from {}",
                            attempt, maxAttempts, events.size(), devices(events));
                }
                return;
            } catch (RuntimeException e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
                LOG.warn("Write attempt {}/{} failed for {} events from {}; retrying in {}: {}",
                        attempt, maxAttempts, events.size(), devices(events), backoff, causeMessage(e));
                sleep(backoff, e);
                backoff = min(backoff.multipliedBy(2), maxBackoff);
            }
        }
    }

    private void sleep(Duration backoff, RuntimeException cause) {
        try {
            sleeper.sleep(backoff);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            cause.addSuppressed(ie);
            throw cause;
        }
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    private static String devices(List<EnrichedMeasurementEvent> events) {
        var devices = events.stream()
                .map(event -> event.event().device())
                .filter(device -> device != null && !device.isBlank())
                .distinct()
                .toList();
        return devices.isEmpty() ? "(none)" : devices.toString();
    }

    private static String causeMessage(RuntimeException failure) {
        var cause = failure.getCause();
        if (cause != null && cause.getMessage() != null) {
            return cause.getMessage();
        }
        return failure.getMessage();
    }
}
