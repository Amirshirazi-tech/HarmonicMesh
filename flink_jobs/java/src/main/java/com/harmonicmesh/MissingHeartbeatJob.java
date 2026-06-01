package com.harmonicmesh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Phase 6 — Missing Heartbeat detector (DataStream API).
 *
 * <p>Reads Machine-04 heartbeat events from {@code harmonicmesh.heartbeats.machine-04}.
 * For each heartbeat, computes the gap (in event-time seconds) since the previous
 * heartbeat on the same machine. When the gap exceeds {@link #GAP_THRESHOLD_SECONDS},
 * emits a pattern match to {@code harmonicmesh.patterns.machine-04}.
 *
 * <p>Detection fires on the ARRIVAL of the heartbeat that breaks the silence,
 * including the last pre-gap heartbeat in {@code source_events} for agent context.
 *
 * <p>Uses the same DataStream + KeyedProcessFunction pattern as
 * {@code ThermalVibrationCascadeJob} to avoid Flink SQL Table API
 * toDataStream/toChangelogStream conversion caveats.
 *
 * <p>Output schema matches {@code ThermalVibrationCascadeJob} exactly
 * (schema_version, pattern_name, machine_id, detected_at, severity, source_events).
 */
public class MissingHeartbeatJob {

    public static final String PATTERN_NAME    = "MissingHeartbeat";
    public static final String SCHEMA_VERSION  = "1.0";
    public static final String DEFAULT_SOURCE_TOPIC = "harmonicmesh.heartbeats.machine-04";
    public static final String DEFAULT_SINK_TOPIC   = "harmonicmesh.patterns.machine-04";
    public static final String DEFAULT_MACHINE_ID   = "Machine-04";
    public static final String DEFAULT_GROUP_ID     = "harmonicmesh-cep-missing-heartbeat";

    /**
     * Normal heartbeat interval is ~60 s; anything above this threshold is anomalous.
     * 63 s tolerates the ±1-tick jitter of the 1-second simulation step.
     */
    public static final int GAP_THRESHOLD_SECONDS = 63;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrap    = params.get("bootstrap",
            envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));
        String sourceTopic  = params.get("source-topic", DEFAULT_SOURCE_TOPIC);
        String sinkTopic    = params.get("sink-topic",   DEFAULT_SINK_TOPIC);
        String groupId      = params.get("group-id",     DEFAULT_GROUP_ID);
        String startOffsets = params.get("starting-offsets", "latest");
        int    gapThreshold = params.getInt("gap-threshold-seconds", GAP_THRESHOLD_SECONDS);

        String saslUser     = requireEnv("KAFKA_SASL_USERNAME");
        String saslPassword = requireEnv("KAFKA_SASL_PASSWORD");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(params.getInt("parallelism", 1));

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(sourceTopic)
            .setGroupId(groupId)
            .setStartingOffsets("earliest".equalsIgnoreCase(startOffsets)
                ? OffsetsInitializer.earliest()
                : OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(saslProperties(saslUser, saslPassword))
            .build();

        KafkaSink<String> sink = buildKafkaSink(bootstrap, sinkTopic, saslUser, saslPassword);

        DataStream<String> raw = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "kafka:" + sourceTopic);

        DataStream<HeartbeatEvent> parsed = raw
            .map(new ParseHeartbeat())
            .filter(e -> e != null && !e.machineId.isEmpty() && e.eventTimeMs > 0)
            .name("parse-heartbeat-json");

        DataStream<String> matches = parsed
            .keyBy(e -> e.machineId)
            .process(new HeartbeatGapDetector(gapThreshold))
            .name("gap-detector");

        matches.sinkTo(sink).name("kafka:" + sinkTopic);

        env.execute("HarmonicMesh-MissingHeartbeat");
    }

    // ------------------------------------------------------------------
    // HeartbeatGapDetector — KeyedProcessFunction
    // ------------------------------------------------------------------

    /**
     * For each heartbeat on a machine, computes the gap from the previous
     * heartbeat. Emits a pattern match when the gap exceeds the threshold.
     *
     * <p>Detection fires on the ARRIVAL of the post-gap heartbeat; the
     * previous (last-before-gap) heartbeat is included in {@code source_events}.
     */
    public static class HeartbeatGapDetector
            extends KeyedProcessFunction<String, HeartbeatEvent, String> {

        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private final int gapThresholdSeconds;
        private transient ValueState<Long>           lastEventTimeMs;
        private transient ValueState<HeartbeatEvent> lastEvent;

        public HeartbeatGapDetector(int gapThresholdSeconds) {
            this.gapThresholdSeconds = gapThresholdSeconds;
        }

        @Override
        public void open(Configuration cfg) {
            lastEventTimeMs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-hb-time-ms", Long.class));
            lastEvent = getRuntimeContext().getState(
                new ValueStateDescriptor<>("last-hb-event",
                    TypeInformation.of(new TypeHint<HeartbeatEvent>() {})));
        }

        @Override
        public void processElement(HeartbeatEvent event, Context ctx, Collector<String> out)
                throws Exception {
            Long prevMs = lastEventTimeMs.value();

            if (prevMs != null) {
                long gapSeconds = (event.eventTimeMs - prevMs) / 1000L;
                if (gapSeconds > gapThresholdSeconds) {
                    HeartbeatEvent prev = lastEvent.value();
                    out.collect(buildMatch(event, prev));
                }
            }

            lastEventTimeMs.update(event.eventTimeMs);
            lastEvent.update(event);
        }

        private String buildMatch(HeartbeatEvent current, HeartbeatEvent prev) throws Exception {
            ObjectNode payload = MAPPER.createObjectNode();
            payload.put("schema_version", SCHEMA_VERSION);
            payload.put("pattern_name",   PATTERN_NAME);
            payload.put("machine_id",     current.machineId);
            payload.put("detected_at",    current.eventTimeIso);
            payload.put("severity",       "CRITICAL");

            ArrayNode events = payload.putArray("source_events");
            if (prev != null && prev.rawJson != null) {
                try {
                    events.add(MAPPER.readTree(prev.rawJson));
                } catch (Exception ignored) {
                    ObjectNode node = MAPPER.createObjectNode();
                    node.put("machine_id", prev.machineId);
                    node.put("event_time", prev.eventTimeIso);
                    node.put("event_type", "heartbeat");
                    node.put("sequence",   prev.sequence);
                    events.add(node);
                }
            }
            return MAPPER.writeValueAsString(payload);
        }
    }

    // ------------------------------------------------------------------
    // HeartbeatEvent POJO
    // ------------------------------------------------------------------

    public static class HeartbeatEvent implements Serializable {
        private static final long serialVersionUID = 1L;
        public String machineId   = "";
        public long   eventTimeMs = 0L;
        public String eventTimeIso = "";
        public long   sequence    = -1L;
        public String rawJson     = "";

        public HeartbeatEvent() {}
    }

    // ------------------------------------------------------------------
    // JSON parser
    // ------------------------------------------------------------------

    public static final class ParseHeartbeat implements MapFunction<String, HeartbeatEvent> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public HeartbeatEvent map(String value) {
            try {
                JsonNode root = MAPPER.readTree(value);
                String iso = root.path("event_time").asText();
                long ms = Instant.parse(iso).toEpochMilli();
                HeartbeatEvent e = new HeartbeatEvent();
                e.machineId    = root.path("machine_id").asText("");
                e.eventTimeMs  = ms;
                e.eventTimeIso = iso;
                e.sequence     = root.path("sequence").asLong(-1L);
                e.rawJson      = value;
                return e;
            } catch (Exception ex) {
                HeartbeatEvent e = new HeartbeatEvent();
                e.rawJson = value;
                return e;
            }
        }
    }

    // ------------------------------------------------------------------
    // Kafka helpers (same pattern as ThermalVibrationCascadeJob)
    // ------------------------------------------------------------------

    static KafkaSink<String> buildKafkaSink(
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

    static Properties saslProperties(String user, String password) {
        Properties p = new Properties();
        p.setProperty("security.protocol", "SASL_PLAINTEXT");
        p.setProperty("sasl.mechanism",    "PLAIN");
        p.setProperty("sasl.jaas.config",  buildJaasConfig(user, password));
        return p;
    }

    static String buildJaasConfig(String user, String password) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required "
             + "username=\"" + user + "\" password=\"" + password + "\";";
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
