package com.harmonicmesh;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
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
import java.util.Properties;

/**
 * Phase 6 — EDI Sequence Violation detector (DataStream API).
 *
 * <p>Reads EDI events from {@code harmonicmesh.edi.events} and detects three
 * violation types via stateful KeyedProcessFunctions keyed by {@code order_id}:
 *
 * <ul>
 *   <li>{@code shipment_without_order} — SHIPMENT as the first event ever seen
 *       for an {@code order_id} (no prior ORDER).
 *   <li>{@code invoice_without_shipment} — INVOICE arriving after an ORDER but
 *       with no intervening SHIPMENT within the detection window.
 *   <li>{@code order_unfulfilled} — ORDER with no matching SHIPMENT within the
 *       detection window (timer-based).
 * </ul>
 *
 * <p>Uses pure DataStream API throughout (same pattern as
 * {@code MissingHeartbeatJob}) to avoid Flink Table API
 * {@code toChangelogStream} / {@code OutputConversionOperator} NPEs
 * observed in flink:1.19.3.
 *
 * <p>Output topic: {@code harmonicmesh.patterns.edi}.
 * Output schema: standard six fields + {@code violation_type}.
 * {@code machine_id} is {@code "EDI-System"}.
 */
public class EDISequenceViolationJob {

    public static final String PATTERN_NAME   = "EDISequenceViolation";
    public static final String SCHEMA_VERSION = "1.0";
    public static final String MACHINE_ID     = "EDI-System";
    public static final String DEFAULT_SOURCE_TOPIC = "harmonicmesh.edi.events";
    public static final String DEFAULT_SINK_TOPIC   = "harmonicmesh.patterns.edi";
    public static final String DEFAULT_GROUP_ID     = "harmonicmesh-cep-edi-violations";

    public static final long DEFAULT_WINDOW_MINUTES = 240L;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrap    = params.get("bootstrap",
            envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"));
        String sourceTopic  = params.get("source-topic", DEFAULT_SOURCE_TOPIC);
        String sinkTopic    = params.get("sink-topic",   DEFAULT_SINK_TOPIC);
        String groupId      = params.get("group-id",     DEFAULT_GROUP_ID);
        String startOffsets = params.get("starting-offsets", "latest");
        long   windowMins   = params.getLong("window-minutes", DEFAULT_WINDOW_MINUTES);
        long   windowMillis = windowMins * 60_000L;

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

        DataStream<String> raw = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "kafka:" + sourceTopic);

        DataStream<EdiEvent> parsed = raw
            .map(new ParseEdi())
            .filter(e -> e != null && e.orderId != null && e.eventType != null)
            .name("parse-edi-json");

        // Key all three detectors by order_id
        DataStream<String> swoStream = parsed
            .keyBy(e -> e.orderId)
            .process(new ShipmentWithoutOrderDetector())
            .name("shipment-without-order");

        DataStream<String> iwsStream = parsed
            .keyBy(e -> e.orderId)
            .process(new InvoiceWithoutShipmentDetector())
            .name("invoice-without-shipment");

        DataStream<String> ouStream = parsed
            .keyBy(e -> e.orderId)
            .process(new OrderUnfulfilledDetector(windowMillis))
            .name("order-unfulfilled");

        KafkaSink<String> sink = buildKafkaSink(bootstrap, sinkTopic, saslUser, saslPassword);
        swoStream.union(iwsStream).union(ouStream)
            .sinkTo(sink).name("kafka:" + sinkTopic);

        env.execute("HarmonicMesh-EDISequenceViolation");
    }

    // ------------------------------------------------------------------
    // Violation 1: shipment_without_order
    // ------------------------------------------------------------------

    /**
     * Emits when a SHIPMENT arrives as the first event for an order_id
     * (no prior ORDER has been seen in this partition).
     */
    public static class ShipmentWithoutOrderDetector
            extends KeyedProcessFunction<String, EdiEvent, String> {

        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        // true when an ORDER has been seen for this order_id
        private transient ValueState<Boolean> orderSeen;

        @Override
        public void open(Configuration cfg) {
            orderSeen = getRuntimeContext().getState(
                new ValueStateDescriptor<>("order-seen-swo", Boolean.class));
        }

        @Override
        public void processElement(EdiEvent e, Context ctx, Collector<String> out)
                throws Exception {
            if ("order".equals(e.eventType)) {
                orderSeen.update(Boolean.TRUE);
                return;
            }
            if ("shipment".equals(e.eventType)) {
                Boolean seen = orderSeen.value();
                if (seen == null || !seen) {
                    // Shipment arrived with no prior order for this order_id
                    out.collect(buildViolation(e, "shipment_without_order", "HIGH",
                        "Shipment arrived with no prior order for this order_id."));
                }
                // Even after detecting, register the shipment in state so we
                // don't fire again if somehow another order arrives later.
                orderSeen.update(Boolean.TRUE);
            }
        }
    }

    // ------------------------------------------------------------------
    // Violation 2: invoice_without_shipment
    // ------------------------------------------------------------------

    /**
     * Emits when an INVOICE arrives after an ORDER but without an
     * intervening SHIPMENT for the same order_id.
     */
    public static class InvoiceWithoutShipmentDetector
            extends KeyedProcessFunction<String, EdiEvent, String> {

        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private transient ValueState<EdiEvent> pendingOrder;
        private transient ValueState<Boolean>  shipmentSeen;

        @Override
        public void open(Configuration cfg) {
            pendingOrder = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-order-iws",
                    TypeInformation.of(new TypeHint<EdiEvent>() {})));
            shipmentSeen = getRuntimeContext().getState(
                new ValueStateDescriptor<>("shipment-seen-iws", Boolean.class));
        }

        @Override
        public void processElement(EdiEvent e, Context ctx, Collector<String> out)
                throws Exception {
            switch (e.eventType) {
                case "order":
                    pendingOrder.update(e);
                    shipmentSeen.clear();
                    break;
                case "shipment":
                    shipmentSeen.update(Boolean.TRUE);
                    break;
                case "invoice":
                    EdiEvent order = pendingOrder.value();
                    Boolean hasSeen = shipmentSeen.value();
                    if (order != null && (hasSeen == null || !hasSeen)) {
                        out.collect(buildInvoiceWithoutShipment(order, e));
                    }
                    // Clear state after invoice (transaction complete or violated)
                    pendingOrder.clear();
                    shipmentSeen.clear();
                    break;
            }
        }

        private String buildInvoiceWithoutShipment(EdiEvent order, EdiEvent invoice)
                throws Exception {
            ObjectMapper m = new ObjectMapper();
            ObjectNode out = m.createObjectNode();
            out.put("schema_version",  SCHEMA_VERSION);
            out.put("pattern_name",    PATTERN_NAME);
            out.put("machine_id",      MACHINE_ID);
            out.put("detected_at",     invoice.eventTimeIso);
            out.put("severity",        "HIGH");
            out.put("violation_type",  "invoice_without_shipment");

            ArrayNode events = out.putArray("source_events");
            events.add(ediEventNode(m, order));
            events.add(ediEventNode(m, invoice));
            return m.writeValueAsString(out);
        }
    }

    // ------------------------------------------------------------------
    // Violation 3: order_unfulfilled (timer-based)
    // ------------------------------------------------------------------

    /**
     * Registers an event-time timer on ORDER arrival; cancels it on SHIPMENT.
     * When the timer fires, emits order_unfulfilled.
     */
    public static class OrderUnfulfilledDetector
            extends KeyedProcessFunction<String, EdiEvent, String> {

        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private final long windowMillis;
        private transient ValueState<EdiEvent> pendingOrder;
        private transient ValueState<Long>     timerMs;

        public OrderUnfulfilledDetector(long windowMillis) {
            this.windowMillis = windowMillis;
        }

        @Override
        public void open(Configuration cfg) {
            pendingOrder = getRuntimeContext().getState(
                new ValueStateDescriptor<>("pending-order-ou",
                    TypeInformation.of(new TypeHint<EdiEvent>() {})));
            timerMs = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer-ms-ou", Long.class));
        }

        @Override
        public void processElement(EdiEvent e, Context ctx, Collector<String> out)
                throws Exception {
            if ("order".equals(e.eventType)) {
                // Clear any existing timer for a previous order on this key
                Long existingTimer = timerMs.value();
                if (existingTimer != null) {
                    ctx.timerService().deleteProcessingTimeTimer(existingTimer);
                }
                long fireAt = System.currentTimeMillis() + windowMillis;
                pendingOrder.update(e);
                timerMs.update(fireAt);
                ctx.timerService().registerProcessingTimeTimer(fireAt);
            } else if ("shipment".equals(e.eventType)) {
                Long t = timerMs.value();
                if (t != null) ctx.timerService().deleteProcessingTimeTimer(t);
                pendingOrder.clear();
                timerMs.clear();
            }
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            EdiEvent order = pendingOrder.value();
            if (order == null) return;
            pendingOrder.clear();
            timerMs.clear();

            out.collect(buildViolation(order, "order_unfulfilled", "HIGH",
                "Order not fulfilled with a shipment within the detection window."));
        }
    }

    // ------------------------------------------------------------------
    // Shared helpers
    // ------------------------------------------------------------------

    static String buildViolation(EdiEvent e, String violationType,
                                  String severity, String context) throws Exception {
        ObjectMapper m = new ObjectMapper();
        ObjectNode out = m.createObjectNode();
        out.put("schema_version",  SCHEMA_VERSION);
        out.put("pattern_name",    PATTERN_NAME);
        out.put("machine_id",      MACHINE_ID);
        out.put("detected_at",     e.eventTimeIso);
        out.put("severity",        severity);
        out.put("violation_type",  violationType);

        ArrayNode events = out.putArray("source_events");
        events.add(ediEventNode(m, e));
        return m.writeValueAsString(out);
    }

    static ObjectNode ediEventNode(ObjectMapper m, EdiEvent e) throws Exception {
        ObjectNode node = m.createObjectNode();
        node.put("event_id",   e.eventId);
        node.put("event_type", e.eventType);
        node.put("order_id",   e.orderId);
        node.put("event_time", e.eventTimeIso);
        if (e.payloadJson != null) {
            try { node.set("payload", m.readTree(e.payloadJson)); }
            catch (Exception ignored) { node.put("payload", e.payloadJson); }
        }
        return node;
    }

    // ------------------------------------------------------------------
    // EdiEvent POJO
    // ------------------------------------------------------------------

    public static class EdiEvent implements Serializable {
        private static final long serialVersionUID = 1L;
        public String eventId      = "";
        public String eventType    = "";
        public String orderId      = "";
        public long   eventTimeMs  = 0L;
        public String eventTimeIso = "";
        public String payloadJson  = "";

        public EdiEvent() {}
    }

    // ------------------------------------------------------------------
    // JSON parser
    // ------------------------------------------------------------------

    public static final class ParseEdi implements MapFunction<String, EdiEvent> {
        private static final long serialVersionUID = 1L;
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public EdiEvent map(String value) {
            try {
                JsonNode root = MAPPER.readTree(value);
                String iso = root.path("event_time").asText();
                EdiEvent e = new EdiEvent();
                e.eventId      = root.path("event_id").asText("");
                e.eventType    = root.path("event_type").asText("");
                e.orderId      = root.path("order_id").asText("");
                e.eventTimeMs  = Instant.parse(iso).toEpochMilli();
                e.eventTimeIso = iso;
                e.payloadJson  = root.path("payload").asText("");
                return e;
            } catch (Exception ex) {
                return null;
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
