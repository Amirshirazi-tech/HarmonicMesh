package com.harmonicmesh.cep;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Phase 3 — Thermal-Vibration Cascade detector.
 *
 * Consumes Machine-03 telemetry from {@code harmonicmesh.sensors.machine-03},
 * detects a three-step cascade in event time, and publishes pattern matches
 * to {@code harmonicmesh.patterns.machine-03}.
 *
 * <pre>
 *   temp_anomaly        temperature_c   &gt; baseline + 60
 *   --&gt; vib_anomaly     vibration_rms   &gt; 4.5 absolute
 *   --&gt; current_anomaly |current - baseline| / baseline &gt; 0.15
 *   within(Duration.ofMinutes(10))
 * </pre>
 *
 * Watermarks: bounded out-of-orderness 5 s, idleness 30 s. Stream is keyed by
 * machine_id so per-machine state is isolated.
 *
 * The pipeline is split out into {@link #attachCascadePipeline} so the test
 * suite drives the same definition production runs, with a bounded
 * {@code fromCollection} source instead of Kafka.
 */
public class ThermalVibrationCascadeJob {

    public static final String PATTERN_NAME = "ThermalVibrationCascade";
    public static final String SCHEMA_VERSION = "1.0";
    public static final String DEFAULT_GROUP_ID = "harmonicmesh-cep-thermal-vibration";
    public static final String DEFAULT_SOURCE_TOPIC = "harmonicmesh.sensors.machine-03";
    public static final String DEFAULT_SINK_TOPIC = "harmonicmesh.patterns.machine-03";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrap = params.get("bootstrap",
            envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));
        String sourceTopic = params.get("source-topic", DEFAULT_SOURCE_TOPIC);
        String sinkTopic = params.get("sink-topic", DEFAULT_SINK_TOPIC);
        String groupId = params.get("group-id", DEFAULT_GROUP_ID);
        String machineId = params.get("machine-id", "Machine-03");
        String startingOffsets = params.get("starting-offsets", "latest");

        String saslUser = requireEnv("KAFKA_SASL_USERNAME");
        String saslPassword = requireEnv("KAFKA_SASL_PASSWORD");

        MachineBaselines baselines = MachineBaselines.load(params.get("baselines"), machineId);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(params.getInt("parallelism", 1));

        KafkaSource<String> source = buildKafkaSource(
            bootstrap, sourceTopic, groupId, saslUser, saslPassword, startingOffsets);
        KafkaSink<String> sink = buildKafkaSink(bootstrap, sinkTopic, saslUser, saslPassword);

        DataStream<String> raw = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "kafka:" + sourceTopic);
        DataStream<SensorEvent> parsed = raw.map(new ParseTelemetry()).name("parse-json");
        DataStream<String> matches = attachCascadePipeline(parsed, baselines);
        matches.sinkTo(sink).name("kafka:" + sinkTopic);

        env.execute("HarmonicMesh-ThermalVibrationCascade");
    }

    // ------------------------------------------------------------------
    // Pipeline (importable for tests)
    // ------------------------------------------------------------------

    /**
     * Attach the cascade pipeline to an already-parsed event stream:
     * watermarks → keyBy(machine_id) → CEP pattern → JSON match string.
     */
    public static DataStream<String> attachCascadePipeline(
            DataStream<SensorEvent> parsed,
            MachineBaselines baselines) {

        DataStream<SensorEvent> timestamped =
            parsed.assignTimestampsAndWatermarks(makeWatermarkStrategy());

        KeyedStream<SensorEvent, String> keyed = timestamped.keyBy(
            (KeySelector<SensorEvent, String>) e -> e.machineId, Types.STRING);

        Pattern<SensorEvent, ?> pattern = buildPattern(baselines);
        PatternStream<SensorEvent> patternStream = CEP.pattern(keyed, pattern);
        return patternStream.select(new CascadeSelector());
    }

    /**
     * 5 s bounded out-of-orderness, 30 s idleness, event-time assigner pulls
     * {@code event_time_ms} from the parsed row.
     *
     * <p>The 5 s budget is generous for a synthetic stream where producer
     * threads briefly desync under high time-compression; small enough that
     * real cascades (which span minutes) are never delayed perceptibly. The
     * 30 s idleness flag stops a momentarily quiet partition from holding
     * back the watermark and starving {@code .within(10m)} timeouts.
     */
    public static WatermarkStrategy<SensorEvent> makeWatermarkStrategy() {
        return WatermarkStrategy
            .<SensorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withIdleness(Duration.ofSeconds(30))
            .withTimestampAssigner((event, recordTs) -> event.eventTimeMs);
    }

    /**
     * The cascade pattern.
     *
     * <p>Both transitions are {@code .followedBy(...)} (relaxed contiguity);
     * the simulator emits all three sensors per tick, so events between
     * cascade steps are tolerated. {@code skipPastLastEvent()} guarantees
     * one cascade fires one match.
     */
    public static Pattern<SensorEvent, ?> buildPattern(MachineBaselines b) {
        return Pattern
            .<SensorEvent>begin("temp_anomaly", AfterMatchSkipStrategy.skipPastLastEvent())
            .where(new TemperatureAnomaly(b.baselineTemperatureC, b.cascadeTemperatureOffsetC))
            .followedBy("vib_anomaly")
            .where(new VibrationAnomaly(b.cascadeVibrationThresholdMmS))
            .followedBy("current_anomaly")
            .where(new CurrentAnomaly(b.baselineCurrentA, b.cascadeCurrentDeviationPct))
            .within(Duration.ofMinutes(10));
    }

    // ------------------------------------------------------------------
    // Conditions (top-level static, so they pickle cleanly when shipped to
    // TaskManagers — no implicit reference to outer class state).
    // ------------------------------------------------------------------

    public static final class TemperatureAnomaly extends IterativeCondition<SensorEvent> {
        private static final long serialVersionUID = 1L;
        private final double threshold;

        public TemperatureAnomaly(double baselineC, double offsetC) {
            this.threshold = baselineC + offsetC;
        }

        @Override
        public boolean filter(SensorEvent e, Context<SensorEvent> ctx) {
            // Strict `>` — equality at threshold is not-yet-anomalous.
            // Asserted by the threshold-edge test case.
            return e.temperatureC > threshold;
        }
    }

    public static final class VibrationAnomaly extends IterativeCondition<SensorEvent> {
        private static final long serialVersionUID = 1L;
        private final double threshold;

        public VibrationAnomaly(double thresholdMmS) {
            this.threshold = thresholdMmS;
        }

        @Override
        public boolean filter(SensorEvent e, Context<SensorEvent> ctx) {
            return e.vibrationRmsMmS > threshold;
        }
    }

    public static final class CurrentAnomaly extends IterativeCondition<SensorEvent> {
        private static final long serialVersionUID = 1L;
        private final double baseline;
        private final double deviationPct;

        public CurrentAnomaly(double baselineA, double deviationPct) {
            this.baseline = baselineA;
            this.deviationPct = deviationPct;
        }

        @Override
        public boolean filter(SensorEvent e, Context<SensorEvent> ctx) {
            if (baseline == 0.0) return false;
            return Math.abs(e.currentA - baseline) / baseline > deviationPct;
        }
    }

    // ------------------------------------------------------------------
    // Match → output JSON
    // ------------------------------------------------------------------

    public static final class CascadeSelector implements PatternSelectFunction<SensorEvent, String> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public String select(Map<String, List<SensorEvent>> match) throws Exception {
            SensorEvent temp = match.get("temp_anomaly").get(0);
            SensorEvent vib = match.get("vib_anomaly").get(0);
            SensorEvent cur = match.get("current_anomaly").get(0);

            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("schema_version", SCHEMA_VERSION);
            payload.put("pattern_name", PATTERN_NAME);
            payload.put("machine_id", cur.machineId);
            payload.put("detected_at", cur.eventTimeIso);
            // v1 hard-coded — tiered severity needs historical context and
            // belongs in the Phase 5 agent layer, not in CEP.
            payload.put("severity", "CRITICAL");

            ArrayNode events = payload.putArray("source_events");
            events.add(rehydrate(temp));
            events.add(rehydrate(vib));
            events.add(rehydrate(cur));
            return MAPPER.writeValueAsString(payload);
        }

        private JsonNode rehydrate(SensorEvent e) throws Exception {
            if (e.rawJson != null && !e.rawJson.isEmpty()) {
                try {
                    return MAPPER.readTree(e.rawJson);
                } catch (Exception ignored) {
                    // fall through to reconstructed form
                }
            }
            ObjectNode node = MAPPER.createObjectNode();
            node.put("machine_id", e.machineId);
            node.put("event_time", e.eventTimeIso);
            ObjectNode sensors = node.putObject("sensors");
            sensors.put("temperature_c", e.temperatureC);
            sensors.put("vibration_rms_mm_s", e.vibrationRmsMmS);
            sensors.put("current_a", e.currentA);
            return node;
        }
    }

    // ------------------------------------------------------------------
    // JSON parse → SensorEvent
    // ------------------------------------------------------------------

    public static final class ParseTelemetry implements MapFunction<String, SensorEvent> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public SensorEvent map(String value) {
            try {
                JsonNode root = MAPPER.readTree(value);
                JsonNode sensors = root.path("sensors");
                String iso = root.path("event_time").asText();
                long ms = isoToMillis(iso);
                return new SensorEvent(
                    root.path("machine_id").asText(""),
                    ms,
                    iso,
                    sensors.path("temperature_c").asDouble(-1.0),
                    sensors.path("vibration_rms_mm_s").asDouble(-1.0),
                    sensors.path("current_a").asDouble(-1.0),
                    value
                );
            } catch (Exception ex) {
                // Sentinel — surfaces upstream parse bugs without crashing.
                return new SensorEvent("", 0L, "", -1.0, -1.0, -1.0, value);
            }
        }
    }

    static long isoToMillis(String iso) {
        if (iso == null || iso.isEmpty()) return 0L;
        return Instant.parse(iso).toEpochMilli();
    }

    // ------------------------------------------------------------------
    // Kafka source / sink
    // ------------------------------------------------------------------

    public static KafkaSource<String> buildKafkaSource(
            String bootstrap, String topic, String groupId,
            String saslUser, String saslPassword, String startingOffsets) {
        return KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            .setStartingOffsets("earliest".equalsIgnoreCase(startingOffsets)
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(saslProperties(saslUser, saslPassword))
            .build();
    }

    public static KafkaSink<String> buildKafkaSink(
            String bootstrap, String topic,
            String saslUser, String saslPassword) {
        return KafkaSink.<String>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                .setTopic(topic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setKafkaProducerConfig(saslProperties(saslUser, saslPassword))
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }

    private static Properties saslProperties(String user, String password) {
        Properties p = new Properties();
        p.setProperty("security.protocol", "SASL_PLAINTEXT");
        p.setProperty("sasl.mechanism", "PLAIN");
        p.setProperty("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"" + user + "\" password=\"" + password + "\";");
        return p;
    }

    private static String envOrDefault(String name, String dflt) {
        String v = System.getenv(name);
        return (v == null || v.isEmpty()) ? dflt : v;
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isEmpty()) {
            throw new IllegalStateException("Missing required env var: " + name);
        }
        return v;
    }
}
