# Lakeflow HL7 v2 Community Connector

This documentation describes how to configure and use the **HL7 v2** Lakeflow community connector to ingest HL7 v2 message files from a Databricks Unity Catalog Volume into structured Delta tables.

Each HL7 segment type (MSH, PID, PV1, OBR, OBX, IN1, RXA, SCH, TXA, and others) becomes its own streaming table, enabling SQL analytics over clinical, financial, pharmacy, and scheduling data without a separate integration engine. The connector supports **23 named segment types** out of the box plus a **generic fallback** for custom Z-segments and any unrecognized segments.

The connector uses a zero-dependency custom parser and supports HL7 v2.1 through v2.8. New files are picked up incrementally on every pipeline trigger with crash-safe exactly-once delivery via SDP checkpointing.


## Prerequisites

- **Databricks workspace** with Unity Catalog enabled.
- **Unity Catalog Volume** containing `.hl7` (or `.txt`) HL7 v2 message files. Files must be uploaded to the volume before or during pipeline execution.
- **Compute access**: The pipeline cluster must have read access to the volume path.

No external API credentials or authentication tokens are required. Databricks handles Volume access natively through Unity Catalog permissions.


## Setup

### Required Connection Parameters

Provide the following **connection-level** options when configuring the connector. These correspond to the connection options exposed by the connector.

| Name | Type | Required | Description | Example |
|------|------|----------|-------------|---------|
| `volume_path` | string | yes | Path to the Unity Catalog Volume directory containing HL7 v2 message files. Must be accessible to the pipeline compute. | `/Volumes/healthcare/raw/hl7_feed/` |
| `file_pattern` | string | no | Glob pattern used to match HL7 files within the volume path. Defaults to `*.hl7`. Use `**/*.hl7` for recursive directory search or `*.txt` if files use the `.txt` extension. | `**/*.hl7` |
| `externalOptionsAllowList` | string | yes | Comma-separated list of table-specific option names that are allowed to be passed through to the connector. This connector requires table-specific options, so this parameter must be set. | `volume_path,file_pattern,segment_type,max_records_per_batch` |

The full list of supported table-specific options for `externalOptionsAllowList` is:
`volume_path,file_pattern,segment_type,max_records_per_batch`

> **Note**: Table-specific options such as `segment_type` and `max_records_per_batch` are provided per-table via table options in the pipeline specification. These option names must be included in `externalOptionsAllowList` for the connection to allow them.

### Obtaining the Required Parameters

- **Volume path**: Navigate to a Unity Catalog Volume in your workspace that contains (or will contain) the HL7 v2 message files. The path follows the format `/Volumes/<catalog>/<schema>/<volume>/<optional_subdirectory>/`. Verify that your pipeline compute has `READ VOLUME` permission on the volume.
- **File pattern**: The default `*.hl7` matches files with the `.hl7` extension in the top-level directory. If files arrive in date-partitioned subdirectories (e.g. `2024/01/15/feed.hl7`), use `**/*.hl7` for recursive matching. If files use the `.txt` extension, use `*.txt` instead.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `volume_path,file_pattern,segment_type,max_records_per_batch` (required for this connector to pass table-specific options).

The connection can also be created using SQL:

```sql
CREATE CONNECTION hl7_hospital_feed
TYPE LAKEFLOW_COMMUNITY
OPTIONS (
  connector                = 'hl7',
  volume_path              = '/Volumes/healthcare/raw/hl7_feed/',
  file_pattern             = '*.hl7',
  externalOptionsAllowList = 'volume_path,file_pattern,segment_type,max_records_per_batch'
);
```

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The HL7 v2 connector exposes a **static list of 23 segment tables** organized into functional categories. Each HL7 segment type becomes its own table. Additionally, a **generic fallback schema** handles custom Z-segments and any unrecognized segment types.

All tables share four common metadata columns that serve as join keys and traceability fields:

| Column | Source | Description |
|--------|--------|-------------|
| `message_id` | MSH-10 | Unique message control ID; primary join key across all segment tables |
| `message_timestamp` | MSH-7 | Message date/time in raw HL7 DTM format, e.g. `20240115120000` |
| `hl7_version` | MSH-12 | HL7 version string, e.g. `2.5.1` |
| `source_file` | filename | Basename of the source `.hl7` file for traceability |
| `raw_segment` | segment line | Raw pipe-delimited text of the HL7 segment for lossless recovery and debugging |

### Object summary, primary keys, and ingestion mode

The connector defines the ingestion mode and primary key for each table. Segment types that appear at most once per message use `message_id` alone as the primary key. Repeating segment types use the composite key `(message_id, set_id)`.

#### Patient Administration

| Table | Description | Ingestion Type | Primary Key | Rows per Message |
|-------|-------------|----------------|-------------|------------------|
| `msh` | Message Header -- routing info, message type, timestamp, HL7 version | `cdc` | `message_id` | One |
| `evn` | Event Type -- trigger event metadata, event reason, operator | `cdc` | `message_id` | One |
| `pid` | Patient Identification -- demographics, MRN, address, phone | `cdc` | `message_id` | One |
| `pd1` | Patient Additional Demographic -- living will, organ donor, military info | `cdc` | `message_id` | One |
| `pv1` | Patient Visit -- encounter details, bed location, attending physician, admit/discharge times | `cdc` | `message_id` | One |
| `pv2` | Patient Visit Additional -- admit reason, expected dates, mode of arrival, patient condition | `cdc` | `message_id` | One |
| `nk1` | Next of Kin / Associated Parties -- emergency contacts, relationship, phone | `cdc` | `message_id`, `set_id` | Multiple |
| `mrg` | Merge Patient Information -- prior identifiers for merge/link/unlink events | `cdc` | `message_id` | One |

#### Clinical

| Table | Description | Ingestion Type | Primary Key | Rows per Message |
|-------|-------------|----------------|-------------|------------------|
| `al1` | Patient Allergy -- allergen code, type, severity, reaction | `cdc` | `message_id`, `set_id` | Multiple |
| `iam` | Patient Adverse Reaction -- newer replacement for AL1 with action-code support | `cdc` | `message_id`, `set_id` | Multiple |
| `dg1` | Diagnosis -- ICD/SNOMED code, type (admitting/working/final), DRG details | `cdc` | `message_id`, `set_id` | Multiple |
| `pr1` | Procedures -- CPT/ICD procedure code, datetime, duration, anesthesia details | `cdc` | `message_id`, `set_id` | Multiple |

#### Orders and Results

| Table | Description | Ingestion Type | Primary Key | Rows per Message |
|-------|-------------|----------------|-------------|------------------|
| `orc` | Common Order -- order control, placer/filler numbers, status, ordering provider | `cdc` | `message_id`, `set_id` | Multiple |
| `obr` | Observation Request -- ordered test, specimen details, ordering provider, result status | `cdc` | `message_id`, `set_id` | Multiple |
| `obx` | Observation Result -- individual lab value, vital sign, or coded observation with units, range, and abnormal flag | `cdc` | `message_id`, `set_id` | Multiple |
| `nte` | Notes and Comments -- free-text annotations attached to orders or results | `cdc` | `message_id`, `set_id` | Multiple |
| `spm` | Specimen -- specimen type, collection details, handling instructions, condition | `cdc` | `message_id`, `set_id` | Multiple |

#### Financial and Insurance

| Table | Description | Ingestion Type | Primary Key | Rows per Message |
|-------|-------------|----------------|-------------|------------------|
| `in1` | Insurance -- plan, company, group number, policy, coverage dates | `cdc` | `message_id`, `set_id` | Multiple |
| `gt1` | Guarantor -- financially responsible party, address, employer, financial class | `cdc` | `message_id`, `set_id` | Multiple |
| `ft1` | Financial Transaction -- charges, payments, adjustments with codes and amounts | `cdc` | `message_id`, `set_id` | Multiple |

#### Pharmacy

| Table | Description | Ingestion Type | Primary Key | Rows per Message |
|-------|-------------|----------------|-------------|------------------|
| `rxa` | Pharmacy/Treatment Administration -- drug/vaccine code, dose, lot number, completion status | `cdc` | `message_id`, `set_id` | Multiple |

#### Scheduling and Documents

| Table | Description | Ingestion Type | Primary Key | Rows per Message |
|-------|-------------|----------------|-------------|------------------|
| `sch` | Scheduling Activity -- appointment IDs, reason, type, duration, contact information | `cdc` | `message_id` | One |
| `txa` | Transcription Document Header -- document type, number, completion status, authenticator | `cdc` | `message_id` | One |

#### Custom / Z-Segments

Site-specific Z-segments (e.g. `ZPD`, `ZIN`) and any unrecognized segment types are supported via the `segment_type` table option. The connector falls back to a generic schema with `segment_type` + `field_1` through `field_25` columns:

```json
{
  "table": {
    "source_table": "zpd",
    "table_configuration": {
      "segment_type": "ZPD"
    }
  }
}
```

### Incremental ingestion

All tables use an incremental cursor based on **file modification timestamp** (epoch seconds). On each pipeline trigger, only files modified after the last cursor value are read. SDP checkpointing ensures exactly-once delivery even after cluster failures.

| Property | Value |
|----------|-------|
| Ingestion type | `cdc` |
| Cursor field | `message_timestamp` (from MSH-7, used for CDC ordering) |
| File cursor | File modification timestamp (epoch seconds) |
| Deduplication | Files already processed are never re-read |
| Multi-message files | Batch files containing multiple MSH segments are fully supported |
| Batch envelope segments | FHS, BHS, BTS, FTS segments are automatically skipped |


## Table Configurations

### Source and Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|--------|----------|-------------|
| `source_table` | Yes | Table name in the source system -- one of the 23 supported segment names (e.g. `pid`, `obx`, `in1`) or a custom name when using `segment_type` |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|--------|----------|-------------|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

### Source-specific `table_configuration` options

| Option | Default | Description |
|--------|---------|-------------|
| `segment_type` | table name | Override the HL7 segment type to extract. Use this for Z-segments or when the table name differs from the segment type (e.g. source_table `zpd` with segment_type `ZPD`). |
| `max_records_per_batch` | `10000` | Maximum number of records returned per micro-batch trigger. Increase for high-volume feeds or decrease for tighter latency control. |


## Data Type Mapping

HL7 v2 messages are pipe-delimited text. The connector maps wire-format values to Spark types as follows:

| HL7 Wire Format | Example Fields | Spark Type | Notes |
|-----------------|----------------|------------|-------|
| String (most fields) | names, codes, addresses, composite fields | `StringType` | All HL7 field values are natively strings; the connector preserves them as-is. |
| Set ID / sequence number | `set_id`, `sequence_number`, `birth_order`, `outlier_days` | `LongType` | Integer fields guaranteed to be numeric in the HL7 spec. Parsed to long; `null` on parse failure. |
| Date/time (DTM format) | `date_of_birth`, `admit_datetime`, `observation_datetime` | `TimestampType` | HL7 DTM strings (e.g. `20240115120000+0500`) are parsed to Spark timestamps. Supports partial precision (YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHHMMSS) and optional timezone offsets. |
| Composite fields | `patient_name`, `allergen_code`, `ordering_provider` | `StringType` (raw) + decomposed component columns | Composite fields are stored as both the raw pipe-delimited value and individual component columns (e.g. `patient_name` raw alongside `patient_family_name`, `patient_given_name`). |
| Primary key (message_id) | `message_id` | `StringType` (NOT NULL) | Non-nullable to support Unity Catalog PRIMARY KEY constraints. |
| Primary key (set_id) | `set_id` on repeating segments | `LongType` (NOT NULL) | Non-nullable composite key for repeating segment tables. Defaults to 1 when the source segment lacks a set ID. |
| Absent fields | Any field not present in a given message version | `null` | Fields absent from a message are returned as `null`. Schemas are supersets covering HL7 v2.1 through v2.8. |

The connector is designed to:
- Preserve raw composite fields alongside their decomposed components for flexibility.
- Parse HL7 DTM timestamps including partial precision and timezone offsets.
- Treat absent or empty fields as `null` to conform to Lakeflow expectations.
- Retain the original raw segment text in every row via the `raw_segment` column for lossless recovery and debugging.


## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI flow from the **Add Data** page, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure which segment tables to ingest and any per-table options such as `segment_type` for Z-segments or `max_records_per_batch` for batch size control.

Example `pipeline_spec` snippet:

```json
{
  "pipeline_spec": {
    "connection_name": "hl7_hospital_feed",
    "object": [
      {
        "table": {
          "source_table": "msh"
        }
      },
      {
        "table": {
          "source_table": "pid"
        }
      },
      {
        "table": {
          "source_table": "pv1"
        }
      },
      {
        "table": {
          "source_table": "obr"
        }
      },
      {
        "table": {
          "source_table": "obx",
          "table_configuration": {
            "max_records_per_batch": "50000"
          }
        }
      },
      {
        "table": {
          "source_table": "al1"
        }
      },
      {
        "table": {
          "source_table": "dg1"
        }
      },
      {
        "table": {
          "source_table": "in1"
        }
      },
      {
        "table": {
          "source_table": "rxa"
        }
      },
      {
        "table": {
          "source_table": "zpd",
          "table_configuration": {
            "segment_type": "ZPD"
          }
        }
      }
    ]
  }
}
```

- `connection_name` must point to the UC connection configured with your `volume_path` (and optional `file_pattern`).
- For each `table`: `source_table` must be one of the 23 supported segment names or a custom name when using `segment_type`.
- You do not need to ingest all 23 tables. Start with the segments present in your message types.

3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow). The connector is incremental:

- On the **first run**, all matching files in the volume are processed.
- On **subsequent runs**, only files with a modification timestamp after the last cursor are processed.

#### Best Practices

- **Start small**: Begin by syncing a subset of segment tables (e.g. `msh`, `pid`, `pv1`, `obx`) to validate configuration and data shape before adding all 23 tables.
- **Use incremental sync**: The connector automatically uses file modification timestamps as cursors. Avoid clearing pipeline checkpoints unless you need a full reprocess.
- **OBX volume**: A single lab message can produce 10-50 OBX rows. The `obx` table will be significantly larger than others. Set a higher `max_records_per_batch` for this table and consider partitioning by `message_timestamp` for query efficiency.
- **Batch size**: For high-volume feeds (over 100k messages/day), set `max_records_per_batch` to `50000` and use a triggered schedule.
- **File pattern**: Use `**/*.hl7` if files arrive in date-partitioned subdirectories (e.g. `2024/01/15/feed.hl7`).
- **Z-segments**: Add one pipeline table entry per Z-segment type you want to capture, each with the `segment_type` option set. Each gets its own Delta table with the generic 25-field schema.
- **Schema evolution**: The connector includes superset schemas covering HL7 v2.1 through v2.8. When upgrading to a newer HL7 version that adds fields, no schema changes are needed -- new fields will be populated and older records will have `null` for those columns.

#### Joining Segment Tables

All tables share `message_id` (from MSH-10) as a join key. Example queries:

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
FROM healthcare_prod.silver.pid  p
JOIN healthcare_prod.silver.obx  o  USING (message_id)
JOIN healthcare_prod.silver.obr  r  USING (message_id)
WHERE r.result_status = 'F'
  AND o.observation_id = '2951-2'   -- Sodium (LOINC)
ORDER BY o.message_timestamp DESC;
```

```sql
-- Insurance coverage for admitted patients
SELECT
    p.patient_family_name,
    p.patient_given_name,
    v.patient_class,
    v.admit_datetime,
    i.insurance_plan_text,
    i.group_number,
    i.plan_effective_date,
    i.plan_expiration_date
FROM healthcare_prod.silver.pid  p
JOIN healthcare_prod.silver.pv1  v  USING (message_id)
JOIN healthcare_prod.silver.in1  i  USING (message_id)
WHERE v.patient_class = 'I'
ORDER BY v.admit_datetime DESC;
```

#### Troubleshooting

**Common Issues:**

| Symptom | Cause | Fix |
|---------|-------|-----|
| Table returns 0 rows | No matching files in `volume_path` | Verify the path and `file_pattern` match actual file locations. Check Unity Catalog Volume permissions. |
| `evn` / `al1` / `sch` / `txa` tables are empty | Those segments are absent from your messages | Normal -- not all HL7 message types include every segment. ADT messages typically include MSH, EVN, PID, PV1; ORU messages include MSH, PID, OBR, OBX. |
| `message_id` is null | MSH-10 (Message Control ID) missing from source messages | Non-standard HL7 -- check the upstream sending system configuration. |
| Duplicate records after a restart | Pipeline checkpoint was cleared | Expected on full-refresh. Use a triggered schedule to avoid large backlogs. |
| Z-segment columns are all null | `segment_type` table option not set | Add the `segment_type` option with the exact segment name in uppercase (e.g. `ZPD`). |
| `'volume_path' is required` error | Connection missing the `volume_path` parameter | Ensure `volume_path` is set in the Unity Catalog connection options. |
| Files not being picked up incrementally | Files overwritten in place with same mtime | The cursor tracks file modification time. If files are replaced without updating mtime, they will not be re-read. Ensure new files have a newer modification timestamp. |


## References

- Connector implementation: `src/databricks/labs/community_connector/sources/hl7/hl7.py`
- Parser implementation: `src/databricks/labs/community_connector/sources/hl7/hl7_parser.py`
- Schema definitions: `src/databricks/labs/community_connector/sources/hl7/hl7_schemas.py`
- Connector specification: `src/databricks/labs/community_connector/sources/hl7/connector_spec.yaml`
- HL7 International:
  - `https://www.hl7.org/implement/standards/product_brief.cfm?product_id=185` (HL7 v2 standard)
  - `https://hl7-definition.caristix.com/v2/` (Caristix HL7 v2 field reference)
