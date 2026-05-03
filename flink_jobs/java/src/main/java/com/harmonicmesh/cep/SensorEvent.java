package com.harmonicmesh.cep;

import java.io.Serializable;

/**
 * Telemetry POJO used by the Thermal-Vibration Cascade pipeline.
 *
 * Public fields plus a public no-arg constructor — Flink's TypeExtractor
 * detects this as a POJO, giving efficient state serialization and
 * field-by-name key extraction.
 */
public class SensorEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public String machineId;
    public long eventTimeMs;
    public String eventTimeIso;
    public double temperatureC;
    public double vibrationRmsMmS;
    public double currentA;

    /** The original Kafka payload, re-emitted intact in the match output so
     *  downstream consumers see the event exactly as the simulator produced it. */
    public String rawJson;

    public SensorEvent() {}

    public SensorEvent(String machineId, long eventTimeMs, String eventTimeIso,
                       double temperatureC, double vibrationRmsMmS,
                       double currentA, String rawJson) {
        this.machineId = machineId;
        this.eventTimeMs = eventTimeMs;
        this.eventTimeIso = eventTimeIso;
        this.temperatureC = temperatureC;
        this.vibrationRmsMmS = vibrationRmsMmS;
        this.currentA = currentA;
        this.rawJson = rawJson;
    }
}
