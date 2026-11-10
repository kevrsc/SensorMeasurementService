package com.kevrsc.sensor.pipeline.business.calibration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class MoistureCalibrationLoader {

    private MoistureCalibrationLoader() {
    }

    public static MoistureCalibrations load(Path file) {
        JsonNode root;
        try {
            root = new ObjectMapper().readTree(file.toFile());
        } catch (IOException e) {
            throw new UncheckedIOException("could not read moisture calibration file " + file, e);
        }
        return parse(root, "file " + file);
    }

    public static MoistureCalibrations loadResource(String resource) {
        try (InputStream in = MoistureCalibrationLoader.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("moisture calibration resource " + resource + " not found on classpath");
            }
            return parse(new ObjectMapper().readTree(in), "resource " + resource);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read moisture calibration resource " + resource, e);
        }
    }

    private static MoistureCalibrations parse(JsonNode root, String source) {
        if (root == null || !root.hasNonNull("default")) {
            throw new IllegalArgumentException("moisture calibration " + source + " must define \"default\"");
        }

        var byDevice = new HashMap<String, Map<Integer, MoistureCalibration>>();
        var devices = root.path("devices");
        for (var device : (Iterable<Map.Entry<String, JsonNode>>) devices::fields) {
            var byProbe = new HashMap<Integer, MoistureCalibration>();
            for (var probe : (Iterable<Map.Entry<String, JsonNode>>) device.getValue()::fields) {
                byProbe.put(Integer.parseInt(probe.getKey()), calibration(probe.getValue()));
            }
            byDevice.put(device.getKey(), Map.copyOf(byProbe));
        }
        return new MoistureCalibrations(calibration(root.get("default")), byDevice);
    }

    private static MoistureCalibration calibration(JsonNode node) {
        return new MoistureCalibration(node.path("dry_raw").asInt(), node.path("wet_raw").asInt());
    }
}
