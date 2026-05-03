package com.harmonicmesh.cep;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Six tests verbatim from the build session, driving the same
 * {@link ThermalVibrationCascadeJob#attachCascadePipeline} that production
 * runs. The bounded {@code fromCollection} source advances watermarks to
 * MAX on completion, which deterministically flushes pending matches and
 * {@code .within(10m)} timeouts.
 */
public class ThermalVibrationCascadeJobTest {

    private static MachineBaselines M3;
    private static double TEMP_THRESHOLD;
    private static double VIB_THRESHOLD;
    private static double CUR_HIGH;
    private static final double VIB_BASELINE = 3.0;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void loadBaselines() throws IOException {
        // Loaded from the classpath resource bundled by the Maven build.
        M3 = MachineBaselines.load(null, "Machine-03");
        TEMP_THRESHOLD = M3.baselineTemperatureC + M3.cascadeTemperatureOffsetC;          // 380.0
        VIB_THRESHOLD = M3.cascadeVibrationThresholdMmS;                                  // 4.5
        // Comfortably above the 15% deviation cutoff (415 * 1.15 = 477.25).
        CUR_HIGH = M3.baselineCurrentA * (1.0 + M3.cascadeCurrentDeviationPct) + 5.0;     // ~482.25
    }

    private static final long T0 = 1_767_225_600_000L; // 2026-01-01T00:00:00.000Z

    private static long at(double seconds) {
        return T0 + (long) (seconds * 1000);
    }

    private static String iso(long ms) {
        // Match the simulator's "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'" form.
        return DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(ms));
    }

    private static SensorEvent event(String machineId, long ms,
                                     double temp, double vib, double cur) {
        String isoStr = iso(ms);
        String raw = String.format(
            "{\"machine_id\":\"%s\",\"event_time\":\"%s\",\"event_type\":\"telemetry\","
            + "\"sensors\":{\"temperature_c\":%s,\"vibration_rms_mm_s\":%s,\"current_a\":%s}}",
            machineId, isoStr, temp, vib, cur);
        return new SensorEvent(machineId, ms, isoStr, temp, vib, cur, raw);
    }

    private static List<JsonNode> runPipeline(List<SensorEvent> events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorEvent> src = env.fromCollection(events);
        DataStream<String> matches =
            ThermalVibrationCascadeJob.attachCascadePipeline(src, M3);

        List<JsonNode> out = new ArrayList<>();
        try (CloseableIterator<String> it = matches.executeAndCollect()) {
            while (it.hasNext()) {
                out.add(MAPPER.readTree(it.next()));
            }
        }
        return out;
    }

    // ===========================================================
    // Test 1 — Happy path
    // ===========================================================
    @Test
    void happy_path_emits_one_match() throws Exception {
        List<SensorEvent> events = Arrays.asList(
            event("Machine-03", at(0),   TEMP_THRESHOLD + 20, VIB_BASELINE,        M3.baselineCurrentA),
            event("Machine-03", at(60),  M3.baselineTemperatureC, VIB_THRESHOLD + 0.5, M3.baselineCurrentA),
            event("Machine-03", at(120), M3.baselineTemperatureC, VIB_BASELINE,        CUR_HIGH)
        );
        List<JsonNode> matches = runPipeline(events);
        assertEquals(1, matches.size(), () -> "expected 1 match, got " + matches.size());
        JsonNode m = matches.get(0);
        assertEquals("ThermalVibrationCascade", m.path("pattern_name").asText());
        assertEquals("Machine-03", m.path("machine_id").asText());
        assertEquals("CRITICAL", m.path("severity").asText());
        assertEquals("1.0", m.path("schema_version").asText());
        // detected_at must equal the third (current) event's event_time.
        assertEquals(iso(at(120)), m.path("detected_at").asText());
        assertEquals(3, m.path("source_events").size());
    }

    // ===========================================================
    // Test 2 — Out of order in event time
    // ===========================================================
    @Test
    void out_of_order_in_event_time_does_not_match() throws Exception {
        // Vibration's event_time (8 s) is BEFORE temperature's (10 s). The
        // 2 s gap is within the 5 s out-of-orderness budget so the vib event
        // is reordered (not dropped as late) into event-time position before
        // the temp event, where it fails the temp_anomaly start condition.
        List<SensorEvent> events = Arrays.asList(
            event("Machine-03", at(10), TEMP_THRESHOLD + 20, VIB_BASELINE,        M3.baselineCurrentA),
            event("Machine-03", at(8),  M3.baselineTemperatureC, VIB_THRESHOLD + 0.5, M3.baselineCurrentA),
            event("Machine-03", at(60), M3.baselineTemperatureC, VIB_BASELINE,        CUR_HIGH)
        );
        List<JsonNode> matches = runPipeline(events);
        assertEquals(0, matches.size(), () -> "expected 0 matches, got " + matches);
    }

    // ===========================================================
    // Test 3 — Just inside the within boundary
    // ===========================================================
    @Test
    void third_event_just_inside_within_boundary_matches() throws Exception {
        // 599 s gap from first to third < 600 s window → match.
        List<SensorEvent> events = Arrays.asList(
            event("Machine-03", at(0),   TEMP_THRESHOLD + 20, VIB_BASELINE,        M3.baselineCurrentA),
            event("Machine-03", at(100), M3.baselineTemperatureC, VIB_THRESHOLD + 0.5, M3.baselineCurrentA),
            event("Machine-03", at(599), M3.baselineTemperatureC, VIB_BASELINE,        CUR_HIGH)
        );
        List<JsonNode> matches = runPipeline(events);
        assertEquals(1, matches.size());
        assertEquals(iso(at(599)), matches.get(0).path("detected_at").asText());
    }

    // ===========================================================
    // Test 4 — Just outside the within boundary
    // ===========================================================
    @Test
    void third_event_just_outside_within_boundary_does_not_match() throws Exception {
        // 601 s gap > 600 s window. The deadline is set by within(...), not
        // the watermark; even with the 5 s OOO budget the candidate match is
        // rejected by the pattern operator because event_time(cur) -
        // event_time(temp) > 10 min.
        List<SensorEvent> events = Arrays.asList(
            event("Machine-03", at(0),   TEMP_THRESHOLD + 20, VIB_BASELINE,        M3.baselineCurrentA),
            event("Machine-03", at(100), M3.baselineTemperatureC, VIB_THRESHOLD + 0.5, M3.baselineCurrentA),
            event("Machine-03", at(601), M3.baselineTemperatureC, VIB_BASELINE,        CUR_HIGH)
        );
        List<JsonNode> matches = runPipeline(events);
        assertEquals(0, matches.size(), () -> "expected 0 matches, got " + matches);
    }

    // ===========================================================
    // Test 5 — Different machines (per-machine keying isolates state)
    // ===========================================================
    @Test
    void events_split_across_machines_does_not_match() throws Exception {
        List<SensorEvent> events = Arrays.asList(
            event("Machine-03", at(0),   TEMP_THRESHOLD + 20, VIB_BASELINE,        M3.baselineCurrentA),
            event("Machine-04", at(60),  M3.baselineTemperatureC, VIB_THRESHOLD + 0.5, M3.baselineCurrentA),
            event("Machine-03", at(120), M3.baselineTemperatureC, VIB_BASELINE,        CUR_HIGH)
        );
        List<JsonNode> matches = runPipeline(events);
        assertEquals(0, matches.size(), () -> "expected 0 matches, got " + matches);
    }

    // ===========================================================
    // Test 6 — Threshold edge (strict `>` is documented)
    // ===========================================================
    @Test
    void temperature_exactly_at_threshold_does_not_match() throws Exception {
        // TemperatureAnomaly uses strict `>`. At temp == 380.0, predicate is
        // false and the pattern never enters its first state.
        List<SensorEvent> events = Arrays.asList(
            event("Machine-03", at(0),   TEMP_THRESHOLD,           VIB_BASELINE,        M3.baselineCurrentA),
            event("Machine-03", at(60),  M3.baselineTemperatureC,  VIB_THRESHOLD + 0.5, M3.baselineCurrentA),
            event("Machine-03", at(120), M3.baselineTemperatureC,  VIB_BASELINE,        CUR_HIGH)
        );
        List<JsonNode> matches = runPipeline(events);
        assertEquals(0, matches.size(),
            "TemperatureAnomaly uses strict `>`; equality at threshold must not trigger.");
    }
}
