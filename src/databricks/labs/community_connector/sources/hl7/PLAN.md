# HL7 v2 Community Connector Plan

## Context

Build a LakeflowConnect community connector that ingests **HL7 v2 message files** from a Databricks Unity Catalog Volume into Databricks tables. HL7 v2 is a pipe-delimited healthcare messaging standard (not a REST API); files land as `.hl7` (or `.txt`) on a volume path. Each **segment type** (MSH, PID, PV1, OBR, OBX, etc.) becomes its own table, enabling structured analytics over clinical data.

---

## Scope


| Table (Segment) | Description                                                |
| --------------- | ---------------------------------------------------------- |
| `msh`           | Message Header — sender, receiver, timestamp, message type |
| `pid`           | Patient Identification — demographics, MRN                 |
| `pv1`           | Patient Visit — encounter details, attending provider      |
| `obr`           | Observation Request — lab/radiology orders                 |
| `obx`           | Observation Result — lab values, vital signs               |
| `al1`           | Patient Allergy                                            |
| `dg1`           | Diagnosis codes                                            |
| `nk1`           | Next of Kin                                                |
| `evn`           | Event Type — message trigger event                         |


Additional segment types are supported dynamically via `table_options["segment_type"]`.

---

## Key Paths


| Path                                                                    | Purpose                              |
| ----------------------------------------------------------------------- | ------------------------------------ |
| `src/databricks/labs/community_connector/sources/hl7/`                  | Connector source (`SRC`)             |
| `tests/unit/sources/hl7/`                                               | Tests (`TESTS`)                      |
| `src/databricks/labs/community_connector/interface/lakeflow_connect.py` | Base interface                       |
| `src/databricks/labs/community_connector/sources/example/example.py`    | Reference connector                  |
| `tests/unit/sources/test_suite.py`                                      | `LakeflowConnectTester` test harness |


---

## Architecture Decisions

### Parsing Library

Use a **custom lightweight parser** (~50–100 lines, no external dependencies). Both `python-hl7` (last updated Feb 2021, Python 3.5–3.8 only) and `hl7apy` (also inactive, no PyPI release in 12+ months) are unmaintained and have Python version compatibility concerns with Databricks Runtime 14+ (Python 3.10/3.11).

HL7 v2 parsing is straightforward — the parser needs to:

1. Split message text into segment lines
2. Read encoding characters from `MSH-1`/`MSH-2` (`|`, `^`, `~`, `&`, `\`)
3. Split each segment by field separator, then by component/repetition separators
4. Return fields by index position

The custom parser lives in `SRC/hl7_parser.py` and handles: field access by index, component access, repetition access, escape sequence decoding, and graceful handling of missing/short segments.

### Connection Parameter

Single required parameter: `volume_path` — the `/Volumes/catalog/schema/vol/subdir/` path where `.hl7` files are stored.

### Ingestion Model

- **Type**: `append` (files are immutable; new files arrive over time)
- **Cursor**: file modification timestamp (epoch seconds, string) — incremental reads pick up files modified after the last cursor
- Full-refresh fallback: offset `None` → scan all files

### SDP + Auto Loader Integration

The connector is deployed as a **Spark Declarative Pipeline (SDP)** — this is what LakeflowConnect pipelines run on. This means several features come for free without the connector needing to implement them:

| Feature | How it's provided |
|---|---|
| **Checkpointing** | SDP persists the connector's `end_offset` after each micro-batch; on restart, `start_offset` is restored from checkpoint — no files are re-read |
| **Exactly-once semantics** | SDP streaming tables guarantee exactly-once writes via Delta's transaction log |
| **Pipeline recovery** | If a run fails mid-batch, SDP replays from the last committed checkpoint offset |
| **Schema evolution** | SDP handles `ALTER TABLE` automatically when new columns appear (e.g. upgrading HL7 version adds fields) |
| **Backfill / full refresh** | User can trigger a full refresh from the pipeline UI; connector receives `start_offset=None` and re-reads all files |

The connector's cursor (file mod timestamp) acts like **Auto Loader's checkpoint file list** — it tracks exactly which files have been processed. SDP wraps this in its own durable checkpoint store, so the cursor survives cluster restarts, upgrades, and failures.

**In practice this means**: users get Auto Loader-grade reliability (no duplicate records, no missed files, crash recovery) simply by running the connector as a Lakeflow pipeline — no extra configuration needed.

### Schema Strategy — Multi-version Superset

HL7 v2 spans versions **2.1 through 2.8**. Newer versions add fields to segments but rarely remove them. Strategy:

1. **Version-agnostic parsing**: Custom parser reads pipe-delimited structure without version-specific validation.
2. **Superset schema**: Each segment schema is defined as a **superset of all fields across v2.1–v2.8**. Fields not present in a given message version are returned as `null`. Mixed-version directories work with no data loss and no configuration required.
3. `**hl7_version` column**: Every table includes `hl7_version` (from MSH-12, e.g. `"2.5.1"`) so downstream queries can filter by version if needed.
4. **Z-segments and unknown segments**: Fall back to a generic schema (`segment_type`, `field_1` … `field_N` as strings) since custom Z-segments are site-specific with no standard definition.

Fields are `StringType` by default (HL7 v2 wire format is all strings). Key fields with known types are explicitly parsed: `TimestampType` for timestamps (MSH-7, etc.), `IntegerType` for sequence numbers.

Each row includes `message_id` (MSH-10), `message_timestamp` (MSH-7), `hl7_version` (MSH-12), and `source_file` (filename) for join keys across segments.

---

## 6-Step Workflow

### Step 1 — API Research

- Subagent: `source-api-researcher`
- Output: `SRC/hl7_api_doc.md`
- Research HL7 v2 format using Caristix reference (`https://hl7-definition.caristix.com/v2/`), documenting segment field definitions for MSH, PID, PV1, OBR, OBX, AL1, DG1, NK1, EVN.

### Step 2 — Auth Setup

- Run `/authenticate-source` skill
- Parameters: `volume_path` (string, required) — no secrets needed (Databricks auth handles Volume access)
- Output: `SRC/connector_spec.yaml` (initial), `tests/unit/sources/hl7/configs/dev_config.json`

### Step 3 — Implementation

- Subagent: `connector-dev`
- Files: `SRC/hl7.py`, `SRC/hl7_schemas.py`, `SRC/hl7_parser.py`
- Key logic:
  - `__init__`: store `volume_path` from options; snapshot file list with mod times at init time
  - `list_tables()`: return `["msh","pid","pv1","obr","obx","al1","dg1","nk1","evn"]` (Z-segments supported via `segment_type` table option)
  - `get_table_schema(table_name)`: return superset StructType from `hl7_schemas.py`; always includes `message_id`, `message_timestamp`, `hl7_version`, `source_file`; Z-segment fallback returns generic `field_1..field_N` schema
  - `read_table_metadata()`: ingestion_type=`append`, cursor_field=`message_timestamp`
  - `read_table(table_name, start_offset)`: read `.hl7` files from volume path, parse with `hl7_parser.py`, yield records for the given segment type using superset schema; cursor = max file mod time seen

### Step 4 — Testing & Fixes

- Subagent: `connector-tester`
- Test file: `TESTS/test_hl7_lakeflow_connect.py`
- Requires sample `.hl7` files placed in the dev volume path configured in `dev_config.json`
- Run: `pytest tests/unit/sources/hl7/ -v --tb=short`

### Step 5 — Docs + Spec

- `connector-doc-writer` → `SRC/README.md`
- `connector-spec-generator` → `SRC/connector_spec.yaml` (finalized)
- `external_options_allowlist`: `"volume_path,segment_type,max_records_per_batch,file_pattern"`

### Step 6 — Deployment (optional)

- Run `/deploy-connector` skill with `use_local_source=true`

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          HL7 v2 File Sources                                │
│                                                                             │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌────────────┐ │
│   │  UC Volume   │   │  S3 / ADLS   │   │     SFTP     │   │  Any path  │ │
│   │ /Volumes/... │   │  s3://...    │   │  (via mount) │   │            │ │
│   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └─────┬──────┘ │
└──────────┼──────────────────┼──────────────────┼─────────────────┼────────┘
           └──────────────────┴──────────────────┴─────────────────┘
                                        │
                              volume_path parameter
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     HL7LakeflowConnect (hl7.py)                             │
│                                                                             │
│  __init__(options)                                                          │
│  ├─ reads volume_path                                                       │
│  └─ snapshots file list + mod times  ◄── cursor for incremental loading    │
│                                                                             │
│  list_tables() → [msh, pid, pv1, obr, obx, al1, dg1, nk1, evn]            │
│  get_table_schema(table) → StructType  ◄── from hl7_schemas.py (superset)  │
│  read_table_metadata() → {ingestion_type: append, cursor: message_timestamp}│
│                                                                             │
│  read_table(table, start_offset)                                            │
│  ├─ list .hl7 files newer than cursor                                       │
│  ├─ read each file as raw text                                              │
│  ├─ parse via hl7_parser.py                                                 │
│  ├─ filter segments matching table name                                     │
│  └─ yield dicts + advance cursor                                            │
└──────────────────────────────────────────────────┬─────────────────────────┘
                                                   │
┌──────────────────────────────────────────────────▼─────────────────────────┐
│                        hl7_parser.py  (custom, zero deps)                  │
│                                                                             │
│  raw .hl7 text                                                              │
│       │                                                                     │
│       ├─ split on \r or \n → segment lines                                  │
│       ├─ read MSH-1 (|) and MSH-2 (^~\&) → encoding chars                 │
│       ├─ split each line by | → fields                                      │
│       ├─ split fields by ^ → components                                     │
│       └─ decode escape sequences (\F\ \S\ \T\ etc.)                        │
│                                                                             │
│  returns: list of {segment_type, field_1..field_N, components}             │
└──────────────────────────────────────────────────┬─────────────────────────┘
                                                   │
┌──────────────────────────────────────────────────▼─────────────────────────┐
│                    hl7_schemas.py  (superset StructType per segment)        │
│                                                                             │
│  Every row always includes:                                                 │
│  ├─ message_id        (MSH-10)   ← join key across all segment tables      │
│  ├─ message_timestamp (MSH-7)    ← cursor field                            │
│  ├─ hl7_version       (MSH-12)   ← e.g. "2.5.1"                           │
│  └─ source_file       (filename) ← traceability                            │
│                                                                             │
│  Segment-specific fields (superset v2.1–v2.8, null if not in message):    │
│  ├─ MSH: sending_app, sending_facility, receiving_app, msg_type, ...       │
│  ├─ PID: patient_id, patient_name, dob, sex, address, phone, ...           │
│  ├─ PV1: visit_number, patient_class, location, attending_doctor, ...      │
│  ├─ OBX: value_type, observation_id, value, units, ref_range, status, ...  │
│  ├─ OBR: order_id, universal_service_id, observation_datetime, ...         │
│  └─ Z-segments: segment_type, field_1 … field_N  (generic fallback)       │
└──────────────────────────────────────────────────┬─────────────────────────┘
                                                   │
                              Spark Declarative Pipeline (SDP)
                                                   │
               ┌───────────────────────────────────┼────────────────────────┐
               ▼               ▼               ▼   ▼   ▼           ▼        ▼
        ┌─────────┐     ┌─────────┐     ┌─────────┐ ┌─────────┐ ┌─────────┐
        │  msh    │     │  pid    │     │  pv1    │ │  obx    │ │  obr    │
        │ table   │     │ table   │     │ table   │ │ table   │ │ table   │
        └────┬────┘     └────┬────┘     └─────────┘ └─────────┘ └─────────┘
             │               │
             └───────────────┘
               join on message_id
                     │
                     ▼
           ┌──────────────────┐
           │  Silver / Gold   │
           │  analytics layer │
           └──────────────────┘

Incremental cursor:
  Run 1:  offset=None  → reads all files → end_offset={cursor: "2024-01-15T10:00:00"}
  Run 2:  offset={cursor: "2024-01-15T10:00:00"} → only new files → no duplicates
```

---

## Databricks UI Flow

```
╔══════════════════════════════════════════════════════════════════════════╗
║  STEP 1 — Pick the connector                                             ║
║  Ingestion → Lakeflow Connect → Add connection                           ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║   Search connectors...  [ hl7_________________ ]                         ║
║                                                                          ║
║   ┌──────────────────┐                                                   ║
║   │  🏥  HL7 v2      │  ← community connector                           ║
║   │  File-based      │                                                   ║
║   └──────────────────┘                                                   ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  STEP 2 — Configure connection  (driven by connector_spec.yaml)          ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║   Connection name *                                                      ║
║   [ hospital-hl7-feed__________________________ ]                        ║
║                                                                          ║
║   Volume path *                                                          ║
║   [ /Volumes/healthcare/raw/hl7_feeds/_________ ]                        ║
║      ↑ only field — no auth secrets needed                               ║
║                                                                          ║
║   File pattern (optional)                                                ║
║   [ *.hl7_____________ ]   e.g. *.hl7, *.txt, ADT*.hl7                  ║
║                                                                          ║
║                              [ Test connection ]  [ Save & continue → ]  ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  STEP 3 — Select tables (segments to ingest)                             ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║   ☑  msh   Message Header                                                ║
║   ☑  pid   Patient Identification                                        ║
║   ☑  pv1   Patient Visit                                                 ║
║   ☑  obx   Observation Result                                            ║
║   ☑  obr   Observation Request                                           ║
║   ☐  al1   Patient Allergy                                               ║
║   ☐  dg1   Diagnosis                                                     ║
║   ☐  nk1   Next of Kin                                                   ║
║   ☐  evn   Event Type                                                    ║
║                                                                          ║
║   ▼ Advanced options (per table)                                         ║
║   ┌──────────────────────────────────────────────────────────────────┐   ║
║   │  Table: obx                                                      │   ║
║   │  max_records_per_batch  [ 10000 ]                                │   ║
║   │  segment_type  [ OBX ]  ← override for Z-segments e.g. ZPD      │   ║
║   └──────────────────────────────────────────────────────────────────┘   ║
║                              ← Back          [ Configure pipeline → ]    ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  STEP 4 — Configure pipeline                                             ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║   Destination catalog *     [ healthcare_prod ▼ ]                        ║
║   Destination schema  *     [ silver          ▼ ]                        ║
║                                                                          ║
║   Tables will be created as:                                             ║
║     healthcare_prod.silver.hl7_msh                                       ║
║     healthcare_prod.silver.hl7_pid                                       ║
║     healthcare_prod.silver.hl7_pv1                                       ║
║     healthcare_prod.silver.hl7_obx                                       ║
║     healthcare_prod.silver.hl7_obr                                       ║
║                                                                          ║
║   Compute                                                                ║
║     ● Serverless  ○ Classic                                              ║
║                                                                          ║
║   Schedule                                                               ║
║     ○ Continuous   ● Triggered  ○ Manual                                 ║
║     Every  [ 15 ] minutes                                                ║
║                                                                          ║
║                              ← Back          [ Create pipeline → ]       ║
╚══════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════╗
║  STEP 5 — Running pipeline view                                          ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  hospital-hl7-feed                          ● Running  Last: 2 min ago  ║
║                                                                          ║
║  ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐   ┌──────┐                  ║
║  │ msh  │   │ pid  │   │ pv1  │   │ obx  │   │ obr  │                  ║
║  │ 1.2M │   │ 1.2M │   │ 980K │   │ 4.7M │   │ 980K │  ← row counts   ║
║  │  ✓   │   │  ✓   │   │  ✓   │   │  ✓   │   │  ✓   │                  ║
║  └──────┘   └──────┘   └──────┘   └──────┘   └──────┘                  ║
║                                                                          ║
║  Incremental: picked up 342 new files since last run                     ║
╚══════════════════════════════════════════════════════════════════════════╝
```

> **Note:** Step 2 is driven entirely by `connector_spec.yaml`. No credentials screen
> appears since Databricks handles Volume/cloud storage auth natively. OBX will
> typically have far more rows than other tables — one message can produce many
> observation results.

---

## Backlog

### Configurable segment tables via options

Currently `list_tables()` returns a hardcoded list of 9 segment types. A future improvement would allow users to configure which segments they want as tables via a `segment_types` connector option:

```
segment_types = "msh,pid,pv1,obx"   # only ingest these four
```

Implementation:
- In `__init__`, read `options.get("segment_types", ",".join(SEGMENT_TABLES))` and store as `self._tables`
- `list_tables()` returns `self._tables` instead of `list(SEGMENT_TABLES)`
- Add `segment_types` to `external_options_allowlist` in `connector_spec.yaml`

This is low-effort and useful when users want to reduce pipeline scope (e.g. a lab-only pipeline that only needs `obr` and `obx`).

---

## Verification

1. Place sample `.hl7` files in a Unity Catalog Volume
2. Configure `dev_config.json` with `volume_path`
3. Run `pytest tests/unit/sources/hl7/ -v` — all tests pass
4. Verify each table (`msh`, `pid`, `obx`, etc.) returns rows with `message_id`, `message_timestamp`, `source_file` populated
5. Run twice; second run returns no duplicate records (incremental cursor working)

