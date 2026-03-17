# HL7 v2 Community Connector

Ingests **HL7 v2 message files** from a Databricks Unity Catalog Volume into
structured Delta tables.  Each HL7 segment type (MSH, PID, PV1, OBR, OBX, …)
becomes its own streaming table, enabling SQL analytics over clinical data
without a separate integration engine.

The connector uses a **zero-dependency custom parser** and supports HL7 v2.1–v2.8.
New files are picked up incrementally on every pipeline trigger — no duplicate
records, no missed files, crash-safe via SDP checkpointing.

---

## Prerequisites

- A Databricks workspace with Unity Catalog enabled
- A Unity Catalog Volume containing `.hl7` (or `.txt`) files
- Pipeline compute with read access to the volume path

No API credentials are required — Databricks handles Volume access natively.

---

## Connection Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `volume_path` | Yes | — | Path to the Volume directory, e.g. `/Volumes/healthcare/raw/hl7_feed/` |
| `file_pattern` | No | `*.hl7` | Glob pattern for file matching. Use `**/*.hl7` for recursive search or `*.txt` for text-extension files. |

---

## Setup

### 1 — Create a Unity Catalog Connection

```sql
CREATE CONNECTION hl7_hospital_feed
TYPE LAKEFLOW_COMMUNITY
OPTIONS (
  connector       = 'hl7',
  volume_path     = '/Volumes/healthcare/raw/hl7_feed/',
  file_pattern    = '*.hl7'
);
```

### 2 — Create a Lakeflow Pipeline

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

pipeline = w.pipelines.create(
    name        = "hospital-hl7-feed",
    catalog     = "healthcare_prod",
    target      = "silver",
    ingestion_definition={
        "connection_name": "hl7_hospital_feed",
        "objects": [
            {"table": {"source_table": {"name": "msh"}}},
            {"table": {"source_table": {"name": "pid"}}},
            {"table": {"source_table": {"name": "pv1"}}},
            {"table": {"source_table": {"name": "obr"}}},
            {"table": {"source_table": {"name": "obx"}}},
            {"table": {"source_table": {"name": "al1"}}},
            {"table": {"source_table": {"name": "dg1"}}},
            {"table": {"source_table": {"name": "nk1"}}},
            {"table": {"source_table": {"name": "evn"}}},
        ]
    }
)
```

---

## Supported Tables

Every table includes four common columns that serve as join keys and
traceability fields:

| Column | Source | Description |
|---|---|---|
| `message_id` | MSH-10 | Unique message control ID — join key across all segment tables |
| `message_timestamp` | MSH-7 | Message date/time (raw HL7 DTM string, e.g. `20240115120000`) |
| `hl7_version` | MSH-12 | HL7 version string (e.g. `2.5.1`) |
| `source_file` | filename | Source `.hl7` filename for traceability |

### msh — Message Header

One row per HL7 message.

| Key Fields | Description |
|---|---|
| `sending_application`, `sending_facility` | Origin system (MSH-3, MSH-4) |
| `receiving_application`, `receiving_facility` | Destination system (MSH-5, MSH-6) |
| `message_code`, `trigger_event` | Message type components (MSH-9.1, MSH-9.2), e.g. `ADT` / `A01` |
| `processing_id` | `P` (production), `T` (test), `D` (debug) |

### pid — Patient Identification

One row per message containing a PID segment.

| Key Fields | Description |
|---|---|
| `patient_id_value` | MRN / patient identifier (PID-3.1) |
| `patient_family_name`, `patient_given_name` | Name components (PID-5.1, PID-5.2) |
| `date_of_birth` | DOB in YYYYMMDD format (PID-7) |
| `administrative_sex` | M / F / U / … (PID-8) |
| `address_street`, `address_city`, `address_state`, `address_zip` | Home address (PID-11) |

### pv1 — Patient Visit

One row per encounter.

| Key Fields | Description |
|---|---|
| `patient_class` | I (inpatient), O (outpatient), E (emergency), … (PV1-2) |
| `location_point_of_care`, `location_room`, `location_bed` | Bed assignment (PV1-3) |
| `attending_doctor_id`, `attending_doctor_family_name` | Attending provider (PV1-7) |
| `visit_number` | Encounter / visit ID (PV1-19.1) |
| `admit_datetime`, `discharge_datetime` | Admission and discharge times (PV1-44, PV1-45) |

### obr — Observation Request

One row per order (lab / radiology).

| Key Fields | Description |
|---|---|
| `placer_order_number`, `filler_order_number` | Order identifiers (OBR-2, OBR-3) |
| `service_id`, `service_text`, `service_coding_system` | Ordered test (OBR-4 components) |
| `observation_datetime` | When the specimen was collected (OBR-7) |
| `ordering_provider_id`, `ordering_provider_family_name` | Ordering clinician (OBR-16) |
| `result_status` | F (final), P (preliminary), C (correction), … (OBR-25) |

### obx — Observation Result

**One row per OBX segment** — a single message with a Basic Metabolic Panel
typically produces 8–14 OBX rows.

| Key Fields | Description |
|---|---|
| `observation_id`, `observation_text` | LOINC code and display name (OBX-3.1, OBX-3.2) |
| `value_type` | NM (numeric), ST (string), TX (text), CWE (coded), … (OBX-2) |
| `observation_value` | The result value as a string (OBX-5) |
| `units_code`, `units_text` | Units of measure (OBX-6.1, OBX-6.2) |
| `references_range` | Normal range, e.g. `3.5-5.0` (OBX-7) |
| `interpretation_codes` | N (normal), H (high), L (low), … (OBX-8) |
| `observation_result_status` | F (final), P (preliminary), … (OBX-11) |

### al1 — Patient Allergy

| Key Fields | Description |
|---|---|
| `allergen_id`, `allergen_text` | Allergen code and display (AL1-3.1, AL1-3.2) |
| `allergen_type_code` | DA (drug), FA (food), EA (environmental), … (AL1-2.1) |
| `allergy_severity_code` | SV (severe), MO (moderate), MI (mild) (AL1-4.1) |
| `allergy_reaction_code` | Reaction description, e.g. `HIVES` (AL1-5) |

### dg1 — Diagnosis

| Key Fields | Description |
|---|---|
| `diagnosis_id`, `diagnosis_text`, `diagnosis_coding_system` | ICD-10 code (DG1-3) |
| `diagnosis_type` | A (admitting), W (working), F (final) (DG1-6) |
| `diagnosis_datetime` | When the diagnosis was recorded (DG1-5) |

### nk1 — Next of Kin

| Key Fields | Description |
|---|---|
| `nk_family_name`, `nk_given_name` | Contact name (NK1-2.1, NK1-2.2) |
| `relationship_code`, `relationship_text` | Relationship to patient (NK1-3) |
| `phone_number` | Primary contact number (NK1-5) |

### evn — Event Type

| Key Fields | Description |
|---|---|
| `recorded_datetime` | When the event was recorded (EVN-2) |
| `event_reason_code` | Reason for the event (EVN-4.1) |
| `operator_id` | User who triggered the event (EVN-5.1) |
| `event_occurred` | When the clinical event actually occurred (EVN-6) |

---

## Custom / Z-Segments

Site-specific Z-segments (e.g. `ZPD`, `ZIN`) are supported via the
`segment_type` table option.  The connector falls back to a generic schema
with `segment_type` + `field_1` … `field_25` columns:

```python
# In pipeline ingestion_definition objects list:
{"table": {
    "source_table": {"name": "zpd"},
    "table_configuration": {"segment_type": "ZPD"}
}}
```

---

## Ingestion Mode

| Property | Value |
|---|---|
| Ingestion type | `append` |
| Cursor | File modification timestamp (epoch seconds) |
| Deduplication | Files already seen are never re-read |
| Multi-message files | Batch files with multiple MSH segments are fully supported |

The incremental cursor is based on **file modification time**, not message
content.  After a pipeline run, only files that arrived (or were modified)
after the last run are processed.  SDP checkpointing ensures exactly-once
delivery even after cluster failures.

---

## Table Options

These can be set per-table in the pipeline configuration:

| Option | Default | Description |
|---|---|---|
| `segment_type` | table name | Override the HL7 segment type to extract (use for Z-segments or aliases) |
| `max_records_per_batch` | `10000` | Cap on records per micro-batch trigger |

---

## Joining Segment Tables

All tables share `message_id` (from MSH-10) as a join key:

```sql
SELECT
    p.patient_family_name,
    p.patient_given_name,
    p.date_of_birth,
    o.observation_id,
    o.observation_text,
    o.observation_value,
    o.units_code,
    o.interpretation_codes
FROM healthcare_prod.silver.hl7_pid  p
JOIN healthcare_prod.silver.hl7_obx  o  USING (message_id)
JOIN healthcare_prod.silver.hl7_obr  r  USING (message_id)
WHERE r.result_status = 'F'
  AND o.observation_id = '2951-2'   -- Sodium (LOINC)
ORDER BY o.message_timestamp DESC;
```

---

## Best Practices

- **OBX volume**: A single lab message can produce 10–50 OBX rows. The `obx`
  table will be significantly larger than others — partition it by
  `message_timestamp` for query efficiency.
- **Batch size**: For high-volume feeds (> 100k messages/day), set
  `max_records_per_batch = 50000` and use a triggered schedule.
- **File pattern**: Use `**/*.hl7` if files arrive in date-partitioned
  subdirectories (e.g. `2024/01/15/feed.hl7`).
- **Z-segments**: Add one pipeline table entry per Z-segment type you want
  to capture; each gets its own Delta table.
- **Schema evolution**: When upgrading to a newer HL7 version that adds
  fields, the connector already includes superset schemas (v2.1–v2.8).
  No schema changes are needed — new fields will be populated, older records
  will have `null` for those columns.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Table returns 0 rows | No matching files in `volume_path` | Verify the path and `file_pattern` match actual file locations |
| `evn` / `al1` tables empty | Those segments absent from your messages | Normal — not all message types include every segment |
| `message_id` is null | MSH-10 missing from source | Non-standard HL7 — check upstream system configuration |
| Duplicate records after restart | Pipeline checkpoint cleared | Expected on full-refresh; use triggered schedule to avoid large backlogs |
| Z-segment columns all null | `segment_type` option not set | Add `segment_type` table option with the exact segment name (e.g. `ZPD`) |
