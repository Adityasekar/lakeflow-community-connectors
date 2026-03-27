# HL7 v2 Community Connector

Ingests **HL7 v2 message files** from a Databricks Unity Catalog Volume into
structured Delta tables.  Each HL7 segment type (MSH, PID, PV1, OBR, OBX,
IN1, RXA, SCH, TXA, …) becomes its own streaming table, enabling SQL
analytics over clinical, financial, pharmacy, and scheduling data without a
separate integration engine.  The connector supports **23 segment types** out
of the box, covering patient administration, orders/results, diagnoses,
procedures, insurance, pharmacy, scheduling, and document management.

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
            # Patient Administration
            {"table": {"source_table": {"name": "msh"}}},
            {"table": {"source_table": {"name": "evn"}}},
            {"table": {"source_table": {"name": "pid"}}},
            {"table": {"source_table": {"name": "pd1"}}},
            {"table": {"source_table": {"name": "pv1"}}},
            {"table": {"source_table": {"name": "pv2"}}},
            {"table": {"source_table": {"name": "nk1"}}},
            {"table": {"source_table": {"name": "mrg"}}},
            # Clinical
            {"table": {"source_table": {"name": "al1"}}},
            {"table": {"source_table": {"name": "iam"}}},
            {"table": {"source_table": {"name": "dg1"}}},
            {"table": {"source_table": {"name": "pr1"}}},
            # Orders & Results
            {"table": {"source_table": {"name": "orc"}}},
            {"table": {"source_table": {"name": "obr"}}},
            {"table": {"source_table": {"name": "obx"}}},
            {"table": {"source_table": {"name": "nte"}}},
            {"table": {"source_table": {"name": "spm"}}},
            # Financial / Insurance
            {"table": {"source_table": {"name": "in1"}}},
            {"table": {"source_table": {"name": "gt1"}}},
            {"table": {"source_table": {"name": "ft1"}}},
            # Pharmacy
            {"table": {"source_table": {"name": "rxa"}}},
            # Scheduling & Documents
            {"table": {"source_table": {"name": "sch"}}},
            {"table": {"source_table": {"name": "txa"}}},
        ]
    }
)
```

---

## Supported Tables (23 Segments)

Every table includes four common columns that serve as join keys and
traceability fields:

| Column | Source | Description |
|---|---|---|
| `message_id` | MSH-10 | Unique message control ID — join key across all segment tables |
| `message_timestamp` | MSH-7 | Message date/time (raw HL7 DTM string, e.g. `20240115120000`) |
| `hl7_version` | MSH-12 | HL7 version string (e.g. `2.5.1`) |
| `source_file` | filename | Source `.hl7` filename for traceability |

### Patient Administration

#### msh — Message Header

One row per HL7 message.

| Key Fields | Description |
|---|---|
| `sending_application`, `sending_facility` | Origin system (MSH-3, MSH-4) |
| `receiving_application`, `receiving_facility` | Destination system (MSH-5, MSH-6) |
| `message_code`, `trigger_event` | Message type components (MSH-9.1, MSH-9.2), e.g. `ADT` / `A01` |
| `processing_id` | `P` (production), `T` (test), `D` (debug) |

#### evn — Event Type

One row per message containing an EVN segment.

| Key Fields | Description |
|---|---|
| `recorded_datetime` | When the event was recorded (EVN-2) |
| `event_reason_code` | Reason for the event (EVN-4.1) |
| `operator_id` | User who triggered the event (EVN-5.1) |
| `event_occurred` | When the clinical event actually occurred (EVN-6) |

#### pid — Patient Identification

One row per message containing a PID segment.

| Key Fields | Description |
|---|---|
| `patient_id_value` | MRN / patient identifier (PID-3.1) |
| `patient_family_name`, `patient_given_name` | Name components (PID-5.1, PID-5.2) |
| `date_of_birth` | DOB in YYYYMMDD format (PID-7) |
| `administrative_sex` | M / F / U / … (PID-8) |
| `address_street`, `address_city`, `address_state`, `address_zip` | Home address (PID-11) |

#### pd1 — Patient Additional Demographic

One row per message containing a PD1 segment.

| Key Fields | Description |
|---|---|
| `living_will_code` | Living will status (PD1-7) |
| `organ_donor_code` | Organ donor status (PD1-8) |
| `patient_primary_facility` | Primary care facility (PD1-3) |
| `advance_directive_code` | Advance directive (PD1-15) |
| `immunization_registry_status` | Registry status (PD1-16) |

#### pv1 — Patient Visit

One row per encounter.

| Key Fields | Description |
|---|---|
| `patient_class` | I (inpatient), O (outpatient), E (emergency), … (PV1-2) |
| `location_point_of_care`, `location_room`, `location_bed` | Bed assignment (PV1-3) |
| `attending_doctor_id`, `attending_doctor_family_name` | Attending provider (PV1-7) |
| `visit_number` | Encounter / visit ID (PV1-19.1) |
| `admit_datetime`, `discharge_datetime` | Admission and discharge times (PV1-44, PV1-45) |

#### pv2 — Patient Visit Additional

One row per message containing a PV2 segment.

| Key Fields | Description |
|---|---|
| `admit_reason` | Reason for admission (PV2-3) |
| `transfer_reason` | Reason for transfer (PV2-4) |
| `expected_admit_datetime`, `expected_discharge_datetime` | Expected dates (PV2-8, PV2-9) |
| `mode_of_arrival_code` | Mode of arrival — ambulance, walk-in, etc. (PV2-38) |
| `admission_level_of_care_code` | Level of care (PV2-40) |

#### nk1 — Next of Kin

Multiple rows per message (one per contact).

| Key Fields | Description |
|---|---|
| `nk_family_name`, `nk_given_name` | Contact name (NK1-2.1, NK1-2.2) |
| `relationship_code`, `relationship_text` | Relationship to patient (NK1-3) |
| `phone_number` | Primary contact number (NK1-5) |

#### mrg — Merge Patient Information

One row per merge message (ADT^A34, ADT^A40, etc.).

| Key Fields | Description |
|---|---|
| `prior_patient_id` | Prior patient identifier (MRG-1.1) |
| `prior_patient_account_number` | Prior account number (MRG-3.1) |
| `prior_patient_name` | Prior patient name (MRG-7) |

### Clinical

#### al1 — Patient Allergy

Multiple rows per message (one per allergy).

| Key Fields | Description |
|---|---|
| `allergen_id`, `allergen_text` | Allergen code and display (AL1-3.1, AL1-3.2) |
| `allergen_type_code` | DA (drug), FA (food), EA (environmental), … (AL1-2.1) |
| `allergy_severity_code` | SV (severe), MO (moderate), MI (mild) (AL1-4.1) |
| `allergy_reaction_code` | Reaction description, e.g. `HIVES` (AL1-5) |

#### iam — Patient Adverse Reaction

Multiple rows per message. Newer replacement for AL1 with action-code support.

| Key Fields | Description |
|---|---|
| `allergen_id`, `allergen_text` | Allergen code and display (IAM-3.1, IAM-3.2) |
| `allergen_type_code` | Allergen type (IAM-2) |
| `allergy_severity_code` | Severity (IAM-4) |
| `allergy_action_code` | A (add), D (delete), U (update) (IAM-6) |
| `allergy_clinical_status_code` | Clinical status — active, inactive, resolved (IAM-17) |

#### dg1 — Diagnosis

Multiple rows per message (one per diagnosis).

| Key Fields | Description |
|---|---|
| `diagnosis_id`, `diagnosis_text`, `diagnosis_coding_system` | ICD-10 code (DG1-3) |
| `diagnosis_type` | A (admitting), W (working), F (final) (DG1-6) |
| `diagnosis_datetime` | When the diagnosis was recorded (DG1-5) |

#### pr1 — Procedures

Multiple rows per message (one per procedure).

| Key Fields | Description |
|---|---|
| `procedure_code`, `procedure_text`, `procedure_coding_system` | CPT/ICD procedure (PR1-3) |
| `procedure_datetime` | When the procedure was performed (PR1-5) |
| `procedure_functional_type` | Functional type (PR1-6) |
| `procedure_minutes` | Duration in minutes (PR1-7) |
| `anesthesia_code`, `anesthesia_minutes` | Anesthesia details (PR1-9, PR1-10) |

### Orders & Results

#### orc — Common Order

Multiple rows per message (one per order).

| Key Fields | Description |
|---|---|
| `order_control` | Order action: NW (new), CA (cancel), SC (status change), RE (observations) (ORC-1) |
| `placer_order_number`, `filler_order_number` | Order identifiers (ORC-2, ORC-3) |
| `order_status` | Order status (ORC-5) |
| `ordering_provider_id`, `ordering_provider_family_name` | Ordering clinician (ORC-12) |
| `order_effective_datetime` | Effective datetime (ORC-15) |

#### obr — Observation Request

One row per order (lab / radiology).

| Key Fields | Description |
|---|---|
| `placer_order_number`, `filler_order_number` | Order identifiers (OBR-2, OBR-3) |
| `service_id`, `service_text`, `service_coding_system` | Ordered test (OBR-4 components) |
| `observation_datetime` | When the specimen was collected (OBR-7) |
| `ordering_provider_id`, `ordering_provider_family_name` | Ordering clinician (OBR-16) |
| `result_status` | F (final), P (preliminary), C (correction), … (OBR-25) |

#### obx — Observation Result

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

#### nte — Notes and Comments

Multiple rows per message. Attached to preceding order or result segments.

| Key Fields | Description |
|---|---|
| `source_of_comment` | L (ancillary/filler), P (orderer/placer), O (other) (NTE-2) |
| `comment` | Free-text comment content (NTE-3) |
| `comment_type` | Comment type code (NTE-4) |

#### spm — Specimen

Multiple rows per message (one per specimen).

| Key Fields | Description |
|---|---|
| `specimen_id` | Specimen identifier (SPM-2) |
| `specimen_type` | Specimen type — blood, urine, tissue, etc. (SPM-4) |
| `specimen_source_site` | Body site (SPM-8) |
| `specimen_collection_datetime` | Collection datetime (SPM-17) |
| `specimen_received_datetime` | When specimen was received (SPM-18) |

### Financial / Insurance

#### in1 — Insurance

Multiple rows per message (one per insurance plan).

| Key Fields | Description |
|---|---|
| `insurance_plan_id`, `insurance_plan_text` | Plan code and name (IN1-2) |
| `insurance_company_id`, `insurance_company_name` | Company identifier (IN1-3, IN1-4) |
| `group_number`, `group_name` | Group number and name (IN1-8, IN1-9) |
| `plan_effective_date`, `plan_expiration_date` | Coverage dates (IN1-12, IN1-13) |
| `insured_relationship_to_patient` | Relationship — self, spouse, child, etc. (IN1-17) |

#### gt1 — Guarantor

Multiple rows per message (one per guarantor).

| Key Fields | Description |
|---|---|
| `guarantor_family_name`, `guarantor_given_name` | Guarantor name (GT1-3) |
| `guarantor_relationship` | Relationship to patient (GT1-11) |
| `guarantor_address` | Address (GT1-5) |
| `guarantor_phone_home`, `guarantor_phone_business` | Phone numbers (GT1-6, GT1-7) |
| `guarantor_employer_name` | Employer (GT1-16) |

#### ft1 — Financial Transaction

Multiple rows per message (one per charge/payment).

| Key Fields | Description |
|---|---|
| `transaction_id` | Unique transaction ID (FT1-2) |
| `transaction_date` | Transaction date (FT1-4) |
| `transaction_type` | CG (charge), CR (credit), PA (payment), AJ (adjustment) (FT1-6) |
| `transaction_code`, `transaction_description` | Charge code (FT1-7) |
| `transaction_amount_extended` | Extended amount (FT1-11) |
| `diagnosis_code` | Associated diagnosis (FT1-19) |

### Pharmacy

#### rxa — Pharmacy/Treatment Administration

Multiple rows per message (one per administration event).

| Key Fields | Description |
|---|---|
| `administered_code`, `administered_text` | Drug/vaccine code and name (RXA-5) |
| `administered_amount`, `administered_units` | Dose amount and units (RXA-6, RXA-7) |
| `administration_start_datetime`, `administration_end_datetime` | Administration times (RXA-3, RXA-4) |
| `substance_lot_number` | Lot number (RXA-15) |
| `completion_status` | CP (complete), RE (refused), NA (not administered) (RXA-20) |

### Scheduling

#### sch — Scheduling Activity

One row per scheduling message.

| Key Fields | Description |
|---|---|
| `placer_appointment_id`, `filler_appointment_id` | Appointment identifiers (SCH-1, SCH-2) |
| `event_reason` | Scheduling event reason (SCH-6) |
| `appointment_reason` | Reason for appointment (SCH-7) |
| `appointment_type` | Appointment type (SCH-8) |
| `filler_status_code` | Status — pending, booked, complete, cancelled (SCH-25) |

### Documents

#### txa — Transcription Document Header

One row per document message.

| Key Fields | Description |
|---|---|
| `document_type` | Document type (TXA-2) |
| `unique_document_number` | Unique document identifier (TXA-12) |
| `origination_datetime` | When the document was originated (TXA-6) |
| `document_completion_status` | DI (dictated), AU (authenticated), LA (legally authenticated) (TXA-17) |
| `document_availability_status` | Availability status (TXA-19) |

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
-- Lab results with patient demographics
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

Insurance coverage for admitted patients:

```sql
SELECT
    p.patient_family_name,
    p.patient_given_name,
    v.patient_class,
    v.admit_datetime,
    i.insurance_plan_text,
    i.group_number,
    i.plan_effective_date,
    i.plan_expiration_date
FROM healthcare_prod.silver.hl7_pid  p
JOIN healthcare_prod.silver.hl7_pv1  v  USING (message_id)
JOIN healthcare_prod.silver.hl7_in1  i  USING (message_id)
WHERE v.patient_class = 'I'
ORDER BY v.admit_datetime DESC;
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
| `evn` / `al1` / `sch` / `txa` tables empty | Those segments absent from your messages | Normal — not all message types include every segment |
| `message_id` is null | MSH-10 missing from source | Non-standard HL7 — check upstream system configuration |
| Duplicate records after restart | Pipeline checkpoint cleared | Expected on full-refresh; use triggered schedule to avoid large backlogs |
| Z-segment columns all null | `segment_type` option not set | Add `segment_type` table option with the exact segment name (e.g. `ZPD`) |
