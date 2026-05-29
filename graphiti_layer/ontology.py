"""Pydantic entity ontology for the HarmonicMesh temporal knowledge graph.

These classes are registered with Graphiti as a *prescribed ontology*: every
``add_episode`` call passes :data:`ENTITY_TYPES`, so Graphiti's ingestion-time
LLM extracts only these five entity types and only the attributes declared
here. Each class docstring and field description is fed to the extraction LLM,
so they are written to be read by a model, not just a human.

Five entities:
  - Machine            — a physical machine on the process line
  - Pattern            — a class of failure signature (e.g. ThermalVibrationCascade)
  - PatternOccurrence  — one detected instance of a Pattern on a Machine
  - Intervention       — a maintenance action taken on a Machine
  - Outcome            — the observed result of an Intervention

Denormalised identifiers (``machine_id`` on PatternOccurrence, etc.) are kept
deliberately: they let the agent reconstruct context from a single retrieved
node without a graph traversal, and they give the extraction LLM an explicit
anchor string to tie episodes back to the right entity.
"""
from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

# Allowed maintenance action types. Constrained so add_intervention() rejects
# any action the downstream analytics / dashboard layers do not understand.
InterventionType = Literal[
    "bearing_replacement",
    "lubrication",
    "calibration",
    "belt_adjustment",
    "sensor_recalibration",
    "inspection_no_action",
    "preventive_maintenance",
]


class Machine(BaseModel):
    """A physical machine on the simulated non-ferrous metals process line."""

    machine_id: Optional[str] = Field(
        default=None,
        description="Canonical machine identifier, e.g. 'Machine-03'.",
    )
    machine_type: Optional[str] = Field(
        default=None,
        description="Machine class in snake_case, e.g. 'rolling_mill', 'furnace'.",
    )
    process_position: Optional[str] = Field(
        default=None,
        description="Position of the machine in the process line, e.g. 'furnace', "
        "'caster', 'rolling_mill', 'annealing', 'finishing'.",
    )


class Pattern(BaseModel):
    """A class of failure signature that the CEP layer can detect."""

    pattern_name: Optional[str] = Field(
        default=None,
        description="Canonical pattern name, e.g. 'ThermalVibrationCascade'.",
    )
    pattern_family: Optional[str] = Field(
        default=None,
        description="Family the pattern belongs to, e.g. 'bearing_degradation'.",
    )
    severity_default: Optional[str] = Field(
        default=None,
        description="Default severity assigned by CEP before historical context "
        "is applied, e.g. 'CRITICAL'.",
    )


class PatternOccurrence(BaseModel):
    """One detected instance of a Pattern on a specific Machine.

    Created from a Flink CEP pattern-match event. Peak sensor readings and
    cascade duration are computed from the match's source events.
    """

    occurrence_id: Optional[str] = Field(
        default=None,
        description="Unique id for this occurrence, e.g. "
        "'hm-occ-machine-03-20260418T142300Z'.",
    )
    pattern_name: Optional[str] = Field(
        default=None,
        description="Name of the Pattern this occurrence is an instance of.",
    )
    machine_id: Optional[str] = Field(
        default=None,
        description="Canonical id of the Machine the occurrence was detected on.",
    )
    detected_at: Optional[datetime] = Field(
        default=None,
        description="Event-time timestamp at which the pattern match closed (ISO 8601).",
    )
    severity: Optional[str] = Field(
        default=None,
        description="Severity of this occurrence, e.g. 'CRITICAL'.",
    )
    peak_temperature_c: Optional[float] = Field(
        default=None,
        description="Peak temperature in degrees Celsius across the match's source events.",
    )
    peak_vibration_rms_mm_s: Optional[float] = Field(
        default=None,
        description="Peak vibration RMS in mm/s across the match's source events.",
    )
    peak_current_a: Optional[float] = Field(
        default=None,
        description="Peak current draw in amperes across the match's source events.",
    )
    cascade_duration_seconds: Optional[float] = Field(
        default=None,
        description="Event-time span in seconds from the first to the last source event.",
    )


class Intervention(BaseModel):
    """A maintenance action performed on a Machine in response to a pattern."""

    intervention_id: Optional[str] = Field(
        default=None,
        description="Unique id for this intervention.",
    )
    machine_id: Optional[str] = Field(
        default=None,
        description="Canonical id of the Machine the intervention was performed on.",
    )
    intervention_type: InterventionType = Field(
        description="Type of maintenance action. One of: bearing_replacement, "
        "lubrication, calibration, belt_adjustment, sensor_recalibration, "
        "inspection_no_action, preventive_maintenance.",
    )
    performed_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp at which the intervention was performed (ISO 8601).",
    )
    performed_by: Optional[str] = Field(
        default=None,
        description="Person or team that performed the intervention.",
    )
    notes: Optional[str] = Field(
        default=None,
        description="Free-text notes recorded about the intervention.",
    )
    target_pattern_name: Optional[str] = Field(
        default=None,
        description="Name of the Pattern the intervention was intended to address.",
    )


class AgentAlert(BaseModel):
    """An alert produced by the Phase 5 LangGraph reasoning agent.

    Reified back into the graph as Layer 2 memory so a future invocation that
    retrieves history sees not just raw pattern occurrences but the agent's
    own prior reasoning on similar cascades. Keeps ``reasoning`` (the model's
    underlying logic, preserved for training) separate from ``summary`` (the
    <=100-word narrative that becomes the episode text).
    """

    alert_id: Optional[str] = Field(
        default=None,
        description="Unique id for this alert.",
    )
    machine_id: Optional[str] = Field(
        default=None,
        description="Canonical id of the Machine the alert was raised for.",
    )
    pattern_name: Optional[str] = Field(
        default=None,
        description="Name of the Pattern the alert is about.",
    )
    detected_at: Optional[datetime] = Field(
        default=None,
        description="Event-time at which the underlying CEP pattern match closed.",
    )
    alerted_at: Optional[datetime] = Field(
        default=None,
        description="Wall-clock time at which this alert was emitted.",
    )
    severity: Optional[str] = Field(
        default=None,
        description="Severity of the alert, e.g. 'CRITICAL'.",
    )
    confidence: Optional[float] = Field(
        default=None,
        description="Final post-validation confidence in [0,1].",
    )
    confidence_raw: Optional[float] = Field(
        default=None,
        description="Pre-validation confidence as reported by the LLM in [0,1].",
    )
    validation_flags: Optional[list[str]] = Field(
        default=None,
        description="Flags raised by validate_confidence, e.g. 'no_anchors'.",
    )
    reasoning: Optional[str] = Field(
        default=None,
        description="The LLM's underlying logic for the alert, retained for training.",
    )
    summary: Optional[str] = Field(
        default=None,
        description="<=100-word natural-language episode text written to Graphiti.",
    )


class Outcome(BaseModel):
    """The observed result of an Intervention over an evaluation window."""

    outcome_id: Optional[str] = Field(
        default=None,
        description="Unique id for this outcome record.",
    )
    intervention_id: Optional[str] = Field(
        default=None,
        description="Id of the Intervention this outcome evaluates.",
    )
    machine_id: Optional[str] = Field(
        default=None,
        description="Canonical id of the Machine the outcome was observed on.",
    )
    outcome_status: Optional[str] = Field(
        default=None,
        description="Result classification, e.g. 'resolved', 'recurred', 'ongoing'.",
    )
    days_until_recurrence: Optional[int] = Field(
        default=None,
        description="Days from the intervention until the pattern recurred; "
        "null if it did not recur within the evaluation window.",
    )
    evaluation_window_days: Optional[int] = Field(
        default=None,
        description="Length in days of the window over which the outcome was evaluated.",
    )
    observed_at: Optional[datetime] = Field(
        default=None,
        description="Timestamp at which the outcome was recorded (ISO 8601).",
    )


# Passed to graphiti.add_episode(entity_types=...) on every ingestion call so
# Graphiti runs in prescribed-ontology mode against exactly these five types.
ENTITY_TYPES: dict[str, type[BaseModel]] = {
    "Machine": Machine,
    "Pattern": Pattern,
    "PatternOccurrence": PatternOccurrence,
    "Intervention": Intervention,
    "Outcome": Outcome,
    # NOTE: AgentAlert is deliberately *not* in ENTITY_TYPES. Graphiti's
    # extraction LLM uses these types to pull structured entities from
    # episode text; we don't need a structured AgentAlert node — the alert
    # is reified as an *episode* and discriminated by source_description.
    # Registering AgentAlert here also collides with Graphiti's reserved
    # ``summary`` attribute on EntityNode, which would block ingestion.
}
