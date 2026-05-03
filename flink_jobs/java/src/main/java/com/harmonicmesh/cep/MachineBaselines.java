package com.harmonicmesh.cep;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * Static threshold parameters for one machine, loaded from YAML at startup.
 *
 * Baselines are read once on the JobManager and referenced inside the
 * IterativeCondition instances; Flink ships those instances to TaskManagers
 * with the closure intact. Runtime baseline learning is v2 scope.
 */
public class MachineBaselines implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String CLASSPATH_RESOURCE = "machine_baselines.yaml";

    public final double baselineTemperatureC;
    public final double baselineCurrentA;
    public final double cascadeTemperatureOffsetC;
    public final double cascadeVibrationThresholdMmS;
    public final double cascadeCurrentDeviationPct;

    public MachineBaselines(double baselineTemperatureC,
                            double baselineCurrentA,
                            double cascadeTemperatureOffsetC,
                            double cascadeVibrationThresholdMmS,
                            double cascadeCurrentDeviationPct) {
        this.baselineTemperatureC = baselineTemperatureC;
        this.baselineCurrentA = baselineCurrentA;
        this.cascadeTemperatureOffsetC = cascadeTemperatureOffsetC;
        this.cascadeVibrationThresholdMmS = cascadeVibrationThresholdMmS;
        this.cascadeCurrentDeviationPct = cascadeCurrentDeviationPct;
    }

    /**
     * Load baselines for one machine.
     *
     * @param pathOrNull external YAML path; if null/empty, loads the
     *                   {@code machine_baselines.yaml} bundled on the classpath.
     */
    @SuppressWarnings("unchecked")
    public static MachineBaselines load(String pathOrNull, String machineId) throws IOException {
        Map<String, Object> root;
        Yaml yaml = new Yaml();
        if (pathOrNull != null && !pathOrNull.isEmpty()) {
            try (InputStream in = Files.newInputStream(Path.of(pathOrNull))) {
                root = yaml.load(in);
            }
        } else {
            try (InputStream in = MachineBaselines.class.getClassLoader()
                    .getResourceAsStream(CLASSPATH_RESOURCE)) {
                if (in == null) {
                    throw new IOException("Classpath resource not found: " + CLASSPATH_RESOURCE
                        + " (expected to be packaged from flink_jobs/config/)");
                }
                root = yaml.load(in);
            }
        }
        Object machine = root.get(machineId);
        if (!(machine instanceof Map)) {
            throw new IllegalArgumentException(
                "machine '" + machineId + "' not found in baselines (have: "
                + root.keySet() + ")");
        }
        Map<String, Object> m = (Map<String, Object>) machine;
        return new MachineBaselines(
            asDouble(m, "baseline_temperature_c"),
            asDouble(m, "baseline_current_a"),
            asDouble(m, "cascade_temperature_offset_c"),
            asDouble(m, "cascade_vibration_threshold_mm_s"),
            asDouble(m, "cascade_current_deviation_pct")
        );
    }

    private static double asDouble(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) {
            throw new IllegalArgumentException("Missing baseline key: " + key);
        }
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        return Double.parseDouble(v.toString());
    }
}
