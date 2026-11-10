package com.kevrsc.sensor.pipeline.structural;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StructuralValidationStageTest {

    private final StructuralValidationStage stage =
            new StructuralValidationStage(new SensorEventParser(), new StructuralValidator());

    @Test
    void returnsValidResultForValidJson() {
        var result = stage.process("""
                {"device": "vermiculture-esp32",
                 "sensors": [{"index": 0, "address": "2800000000000001", "celsius": 18.06}],
                 "received_at": "2026-11-09T11:56:34-08:00"}
                """);

        var valid = assertInstanceOf(StructuralResult.Valid.class, result);
        assertEquals("vermiculture-esp32", valid.event().device());
    }

    @Test
    void returnsInvalidResultWhenStructuralRuleFails() {
        var result = stage.process("""
                {"device": " ", "sensors": [], "received_at": "2026-11-09T11:56:34-08:00"}
                """);

        var invalid = assertInstanceOf(StructuralResult.Invalid.class, result);
        assertEquals(List.of("device is required"), invalid.errors());
    }

    @Test
    void returnsInvalidResultForMalformedJson() {
        var result = stage.process("{\"device\": ");

        var invalid = assertInstanceOf(StructuralResult.Invalid.class, result);
        assertEquals(1, invalid.errors().size());
        assertTrue(invalid.errors().get(0).startsWith("malformed event JSON"));
    }
}
