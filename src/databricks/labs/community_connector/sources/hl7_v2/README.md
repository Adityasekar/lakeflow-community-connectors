# Lakeflow HL7 v2 Community Connector

This documentation provides setup instructions and reference information for the HL7 v2 source connector.

The Lakeflow HL7 v2 Connector parses HL7 v2 pipe-delimited messages and loads each segment type into its own Delta table, enabling structured analytics over clinical data. The connector supports incremental append-only ingestion, automatically tracking new messages by their send time.

## Supported Source Modes

The connector supports two source modes, controlled by the `source_type` connection parameter. In both modes, the same HL7 v2 parsing pipeline is used -- every extractor, schema, and segment table works identically regardless of where the raw messages originate.

| Mode | `source_type` value | Description |
|---|---|---|
| **GCP Healthcare API** | `gcp` (default) | Fetches HL7 v2 messages from a Google Cloud Healthcare API HL7v2 store via REST |
| **Delta Table** | `delta` | Reads pre-loaded HL7 v2 messages from a Bronze Delta table in Databricks |

## Prerequisites

### GCP Mode

- A **Google Cloud Platform** project with the **Cloud Healthcare API** enabled
- An **HL7v2 store** within a Healthcare API dataset, containing HL7 v2 messages
- A **GCP service account** with the following IAM permissions:
  - `healthcare.hl7V2Messages.list`
  - `healthcare.hl7V2Messages.get`
- A JSON key file for the service account

### Delta Mode

- A **Delta table** in Unity Catalog containing raw HL7 v2 messages (see [Delta Table Schema Requirements](#delta-table-schema-requirements) below)
- The table is typically populated by landing HL7 files onto **Databricks Volumes** and loading them into a Bronze table using Autoloader.

## Setup

### Connection Parameters by Source Mode

The `source_type` parameter determines which set of connection parameters is required. If `source_type` is omitted, it defaults to `gcp`.

#### GCP Mode Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `source_type` | string | No | Set to `gcp` or omit entirely | `gcp` |
| `project_id` | string | Yes | GCP project ID containing the Healthcare dataset | `my-gcp-project` |
| `location` | string | Yes | GCP region where the Healthcare dataset resides | `us-central1` |
| `dataset_id` | string | Yes | Google Cloud Healthcare API dataset ID | `My_Clinical_Dataset` |
| `hl7v2_store_id` | string | Yes | HL7v2 store name within the Healthcare dataset | `ehr_messages` |
| `service_account_json` | string | Yes | Full JSON content of a GCP service account key file | `{"type": "service_account", ...}` |

#### Delta Mode Connection Parameters

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `source_type` | string | Yes | Must be set to `delta` | `delta` |
| `delta_table_name` | string | Yes | Fully-qualified Delta table name (three-level namespace) | `my_catalog.bronze.hl7_raw` |
| `databricks_host` | string | Yes | Databricks workspace URL | `https://my-workspace.cloud.databricks.com` |
| `databricks_token` | string | Yes | Databricks personal access token (PAT) or service principal token | `dapi...` |
| `sql_warehouse_id` | string | Yes | ID of a SQL warehouse used to query the Delta table | `01370556fad60fda` |
| `delta_query_mode` | string | No | `preload` (default) loads the entire table into memory at init — fast for small tables. `per_window` issues a live SQL query per micro-batch window — scales to large tables with no memory overhead. | `per_window` |

> **Why are Databricks credentials needed?** The Lakeflow Connect pipeline framework uses the Spark Python Data Source API, which runs connector code in a subprocess where SparkSession is unavailable. To read the Delta table, the connector queries it via the Databricks SQL Statement Execution REST API, which requires explicit workspace credentials -- the same pattern used by the GCP mode with its `service_account_json`.

### Delta Table Schema Requirements

A typical HL7 v2 message is just the raw pipe-delimited text (e.g., `MSH|^~\&|SendingApp|...`). The connector needs that message body plus two pieces of metadata for incremental ingestion and traceability. Your Bronze Delta table must have the following columns:

| Column | Type | Required | Description |
|---|---|---|---|
| `data` | STRING | **Yes** | The raw HL7 v2 message text, stored as-is (pipe-delimited, e.g., `MSH\|^~\\&\|SendingApp\|...`). No encoding required. |
| `sendTime` | STRING or TIMESTAMP | **Yes** | A timestamp (e.g., `2024-01-15T10:30:00Z`). This is used as the **incremental cursor** -- the connector uses a sliding time window over this column to fetch new messages in each micro-batch. Both STRING (RFC 3339) and TIMESTAMP types are supported. Typically this represents when the message was received, sent, or landed in your system. |
| `name` | STRING | No | A source identifier for traceability (e.g., the original file path from Volumes, a message ID, or any label). This value is stored in the `source_file` metadata column of every output table. If omitted, `source_file` will be empty. |

**Why the extra columns beyond the message itself?**

- **`sendTime`**: Without a timestamp column, the connector cannot track which messages have already been processed. This column drives incremental ingestion -- each run reads only messages with `sendTime` after the last checkpoint. The Lakeflow pipeline framework persists this cursor between runs automatically.
- **`name`**: Optional but recommended. When debugging or auditing, it lets you trace a parsed row back to the original source file or system.

**Example: Creating the Bronze table from Volume files**

If your HL7 v2 messages are landing as text files on a Databricks Volume, you can create the required Bronze table like this:

```sql
CREATE TABLE my_catalog.my_schema.hl7_raw AS
SELECT
  value                            AS data,
  _metadata.file_modification_time AS sendTime,
  _metadata.file_name              AS name
FROM read_files(
  '/Volumes/my_catalog/my_schema/hl7_volume/',
  format => 'text'
);
```

For incremental loads into the Bronze table, consider using Auto Loader or a streaming table to continuously pick up new files from the Volume and append them.

### Table-Level Options

The following options can be set per table via `table_configuration`. These apply to **both** source modes:

| Option | Type | Default | Description |
|---|---|---|---|
| `segment_type` | string | *(table name)* | Override the HL7 segment type to extract. Use this to ingest custom Z-segments (e.g., `ZPD`) through any table name. |
| `window_seconds` | string | `86400` | Duration in seconds of the sliding time window used for incremental reads. Smaller values produce smaller, faster batches. |
| `start_timestamp` | string | *(auto-discovered)* | RFC 3339 timestamp to start reading from when no prior offset exists (e.g., `2024-01-01T00:00:00Z`). If omitted, the connector auto-discovers the oldest message. |

#### Understanding `externalOptionsAllowList`

To use the table-level options above, the UC connection must include them in the `externalOptionsAllowList` parameter:

```
segment_type,window_seconds,start_timestamp
```

`externalOptionsAllowList` is a **Unity Catalog connection-level security gate**. It controls which option keys the Lakeflow framework is allowed to pass from your pipeline spec's `table_configuration` through to the connector code. It does **not** restrict which segment types or tables you can ingest.

- If a key (e.g. `window_seconds`) is listed in `externalOptionsAllowList`, the framework passes its value to the connector.
- If a key is **not** listed, the framework **silently drops** it — the connector never receives it and falls back to its default.

The allowlist exists to prevent arbitrary options from being injected into connector code. For this connector, the three allowed keys are `segment_type`, `window_seconds`, and `start_timestamp`.

#### Understanding `window_seconds`

The connector uses a **sliding time window** to bound each micro-batch during incremental ingestion. `window_seconds` controls the size of that window — i.e., how much time each batch covers.

```
window_seconds = 86400 (1 day, default):

  Run 1:  [--Jan 1--]                         → fetches 1 day of messages
  Run 2:             [--Jan 2--]               → next 1-day window
  Run 3:                        [--Jan 3--]    → next 1-day window

window_seconds = 3600 (1 hour):

  Run 1:  [-1hr-]                              → fetches 1 hour of messages
  Run 2:        [-1hr-]                        → next 1-hour window
  ...  (24 runs to cover 1 day)
```

The connector automatically advances the cursor forward after each successful batch and checkpoints between pipeline runs.

**When to adjust:**

| Scenario | Recommended `window_seconds` | Why |
|---|---|---|
| High-volume source (thousands of messages/day) | `3600` (1 hour) | Keeps each batch small to avoid memory or API rate limits |
| Low-volume source (a few messages/day) | `86400` (1 day, default) | Covers a full day per batch, catches up quickly |
| Testing | `60` (1 minute) | Processes tiny batches to verify the pipeline works |

#### Understanding `segment_type`

By default, the `source_table` name maps directly to the HL7 segment type — `source_table: "pid"` extracts `PID` segments, `source_table: "obx"` extracts `OBX` segments, and so on. **For all 21 standard segment types, you never need to set `segment_type`.**

The `segment_type` option exists for **custom Z-segments**. HL7 v2 allows hospitals and vendors to define site-specific segments prefixed with "Z" (e.g. `ZPD`, `ZLB`, `ZIN`). These are not part of the HL7 standard and don't have pre-defined table entries in the connector.

To ingest a Z-segment, choose any table name and use `segment_type` to point it at the custom segment:

```json
{
    "table": {
        "source_table": "custom_patient_demographics",
        "table_configuration": {
            "segment_type": "ZPD"
        }
    }
}
```

This creates a destination table called `custom_patient_demographics` that extracts all `ZPD` segments. Since Z-segments are non-standard, the connector outputs a generic schema with columns `field_1` through `field_25` plus the standard metadata columns (`message_id`, `send_time`, etc.).

| Scenario | `source_table` | `segment_type` needed? |
|---|---|---|
| Standard segment (PID, OBX, MSH, ...) | `pid`, `obx`, `msh` | No — auto-matched from table name |
| Custom Z-segment | Any name you choose | Yes — set to the Z-segment prefix (e.g. `ZPD`) |

### Obtaining Delta Mode Connection Parameters

#### Workspace URL (`databricks_host`)

This is your Databricks workspace URL. You can find it in your browser address bar when logged in (e.g., `https://my-workspace.cloud.databricks.com`). It is also shown in **Settings > Workspace details**.

#### Personal Access Token (`databricks_token`)

1. In Databricks, go to **Settings > Developer > Access tokens**
2. Click **Generate new token**, give it a description (e.g., `hl7-delta-connector`), and set an appropriate lifetime
3. Copy the generated token (starts with `dapi`)

Alternatively, use a **service principal** token for production deployments. The token must have `SELECT` access on the Delta table specified in `delta_table_name`.

#### SQL Warehouse ID (`sql_warehouse_id`)

1. In Databricks, go to **SQL Warehouses**
2. Click on the warehouse you want to use (any warehouse that can access the Delta table)
3. The warehouse ID is shown in the **Connection details** tab, or in the URL: `.../sql/warehouses/<warehouse_id>`

> **Tip**: For cost efficiency, use a serverless SQL warehouse — it scales to zero when idle and starts up quickly when the pipeline triggers a query.

### Obtaining GCP Connection Parameters

#### Project, Location, Dataset, and Store IDs

1. Open the [Google Cloud Console](https://console.cloud.google.com)
2. Navigate to **Healthcare > Datasets**
3. Click into your dataset to find the **dataset ID** and **location** (shown in the breadcrumb, e.g., `Best_Clinics_Dataset (northamerica-northeast1)`)
4. Click into the HL7v2 store to find the **store ID** (shown in the Details section)
5. The **project ID** is visible in the project selector at the top of the console

The full resource path shown in the store details page follows this pattern:
```
projects/{project_id}/locations/{location}/datasets/{dataset_id}/hl7V2Stores/{hl7v2_store_id}
```

#### Service Account Key

1. In GCP Console, go to **IAM & Admin > Service Accounts**
2. Create a new service account or select an existing one
3. Grant it the **Healthcare HL7v2 Message Editor** role (or a custom role with `healthcare.hl7V2Messages.list` and `healthcare.hl7V2Messages.get`)
4. Click on the service account > **Keys** tab > **Add Key** > **Create new key** > **JSON**
5. A `.json` key file will be downloaded. Paste the entire contents of this file as the `service_account_json` parameter.

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:
1. Follow the Lakeflow Community Connector UI flow from the "Add Data" page
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set the `externalOptionsAllowList` to: `segment_type,window_seconds,start_timestamp`

The connection can also be created using the standard Unity Catalog API.


## Supported Objects

The connector exposes one table per HL7 v2 segment type. Schemas follow the HL7 v2.9 specification (the latest version, which is a superset of all prior versions), so every column is present regardless of the message version. Not all messages contain all segment types — the tables you see data in depend on the message types in your HL7v2 store (e.g., ADT, ORU, DFT, SIU).

### Patient Administration

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `msh` | MSH | Message Header — one row per HL7 message | `message_id` |
| `evn` | EVN | Event Type — trigger event metadata | `message_id` |
| `pid` | PID | Patient Identification — demographics and identifiers | `message_id` |
| `pd1` | PD1 | Patient Additional Demographic — living will, organ donor, primary facility | `message_id` |
| `pv1` | PV1 | Patient Visit — encounter/admission data | `message_id` |
| `pv2` | PV2 | Patient Visit Additional — admit reason, expected dates | `message_id` |
| `nk1` | NK1 | Next of Kin / Associated Parties | `message_id`, `set_id` |
| `mrg` | MRG | Merge Patient Information — prior identifiers | `message_id` |

### Clinical

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `al1` | AL1 | Patient Allergy Information | `message_id`, `set_id` |
| `iam` | IAM | Patient Adverse Reaction Information | `message_id`, `set_id` |
| `dg1` | DG1 | Diagnosis — admitting, working, and final diagnoses | `message_id`, `set_id` |
| `pr1` | PR1 | Procedures — surgical, diagnostic, and therapeutic | `message_id`, `set_id` |

### Orders and Results

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `orc` | ORC | Common Order — order control, status, and provider info | `message_id`, `set_id` |
| `obr` | OBR | Observation Request — lab/radiology orders | `message_id`, `set_id` |
| `obx` | OBX | Observation Result — individual test results | `message_id`, `set_id` |
| `nte` | NTE | Notes and Comments — free-text annotations | `message_id`, `set_id` |
| `spm` | SPM | Specimen — type, collection, and handling details | `message_id`, `set_id` |

### Financial and Insurance

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `in1` | IN1 | Insurance — policy coverage and billing | `message_id`, `set_id` |
| `gt1` | GT1 | Guarantor — financially responsible party | `message_id`, `set_id` |
| `ft1` | FT1 | Financial Transaction — charges, payments, adjustments | `message_id`, `set_id` |

### Pharmacy, Scheduling, and Documents

| Table | Segment | Description | Primary Keys |
|---|---|---|---|
| `rxa` | RXA | Pharmacy/Treatment Administration | `message_id`, `set_id` |
| `sch` | SCH | Scheduling Activity Information | `message_id` |
| `txa` | TXA | Transcription Document Header | `message_id` |

### Custom / Z-Segments

Arbitrary Z-segments (site-specific custom segments) can be ingested by setting the `segment_type` table option to the desired segment prefix (e.g., `ZPD`). Z-segments produce a generic schema with columns `field_1` through `field_25` plus the standard metadata columns.

### Incremental Ingestion

All tables use **append-only** incremental ingestion. This reflects how HL7 v2 works as a protocol — it is a messaging standard, not a database protocol. Every message is an immutable event (e.g., "patient admitted", "address changed", "lab result received"). Source systems like the GCP Healthcare API HL7v2 store and integration engines store each message independently and never overwrite prior messages. The connector mirrors this behavior — if the same patient has multiple messages, each one becomes a separate row.

- New messages are tracked by the `sendTime` timestamp (stored as the `send_time` column). In GCP mode this comes from the Healthcare API; in Delta mode it comes from the `sendTime` column of your Bronze table.
- The connector uses a **sliding time window** to bound each micro-batch, advancing the cursor forward with each run.
- Messages already processed are never re-fetched. No change feeds, upserts, or deletes are produced.

### Common Metadata Columns

Every table includes the following columns for traceability and cross-table joins:

| Column | Description |
|---|---|
| `message_id` | Unique message control ID from MSH-10 — the primary join key across all tables |
| `message_timestamp` | Message date/time string from MSH-7 (e.g., `20240115120000`) |
| `hl7_version` | HL7 version (e.g., `2.5.1`) from MSH-12 |
| `source_file` | Source identifier for traceability — the GCP API resource name (GCP mode) or the `name` column value (Delta mode) |
| `send_time` | RFC 3339 timestamp used as the incremental cursor — from the GCP API `sendTime` (GCP mode) or the `sendTime` column (Delta mode) |
| `raw_segment` | The original unparsed HL7 segment text |


## Table Configurations

### Source and Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | Table name in the source system (e.g., `pid`, `obx`, `msh`) |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Source-Specific `table_configuration` options

| Option | Required | Description |
|---|---|---|
| `segment_type` | No | Override the HL7 segment type to extract (e.g., `ZPD` for a custom Z-segment). Defaults to the `source_table` name. |
| `window_seconds` | No | Sliding window duration in seconds. Default: `86400` (1 day). Use smaller values (e.g., `3600`) for higher-volume stores. |
| `start_timestamp` | No | RFC 3339 timestamp to begin reading from (e.g., `2024-01-01T00:00:00Z`). If omitted, the connector auto-discovers the oldest message. |


## Data Type Mapping

All HL7 v2 fields are stored as `STRING` in the Delta tables. The connector extracts specific components from composite HL7 data types and stores them as individual string columns. Downstream SQL queries can cast as needed.

| HL7 Data Type | Examples | Databricks Type | Notes |
|---|---|---|---|
| ST, TX, FT, IS, ID | Plain text, coded values | STRING | Stored as-is |
| NM, SI | Numeric strings | STRING | Cast with `CAST(col AS INT)` or `CAST(col AS DOUBLE)` as needed |
| TS, DT, TM | Timestamps, dates, times | STRING | HL7 DTM format (e.g., `20240115120000`). Datetime fields like `date_of_birth` and `admit_datetime` are parsed to `TIMESTAMP` by the connector. |
| CE, CWE, CNE | Coded elements | STRING | Individual components extracted (e.g., `code`, `text`, `coding_system`) |
| XPN | Person name | STRING | Components extracted: `family_name`, `given_name`, `middle_name` |
| XAD | Address | STRING | Components extracted: `street`, `city`, `state`, `zip`, `country` |
| XCN | Composite ID and name | STRING | Components extracted: `id`, `family_name`, `given_name` |
| CX | Extended composite ID | STRING | Component 1 extracted (ID value) |
| HD, EI | Hierarchic/entity identifiers | STRING | Component 1 extracted (namespace/entity ID) |
| PL | Person location | STRING | Components extracted: `point_of_care`, `room`, `bed`, `facility` |

**Special columns:**
- `set_id` is stored as `BIGINT` — the sequence number for repeating segments within a message.
- Datetime fields parsed from HL7 DTM format (e.g., `date_of_birth`, `admit_datetime`, `observation_datetime`) are stored as `TIMESTAMP`.


## How to Run

### Step 1: Clone/Copy the Source Connector Code
Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline
1. Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`).
2. Configure the tables you want to ingest. Each HL7 segment type is a separate table:
```json
{
  "pipeline_spec": {
      "connection_name": "my_hl7_connection",
      "object": [
        {
            "table": {
                "source_table": "pid",
                "table_configuration": {
                    "window_seconds": "3600"
                }
            }
        },
        {
            "table": {
                "source_table": "obx",
                "table_configuration": {
                    "window_seconds": "3600"
                }
            }
        },
        {
            "table": {
                "source_table": "msh"
            }
        }
      ]
  }
}
```
3. (Optional) Customize the source connector code if needed for special use cases.

### Step 3: Run and Schedule the Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a few key segment types (e.g., `msh`, `pid`, `pv1`, `obx`) before adding the full set.
- **Use Incremental Sync**: The connector automatically tracks progress via the `sendTime` cursor, minimizing redundant reads.
- **Tune the Window Size**: For high-volume sources, reduce `window_seconds` (e.g., `3600` for 1-hour windows) to keep each micro-batch manageable. For low-volume sources, the default of `86400` (1 day) works well.
- **Join Across Tables**: Use the `message_id` column to join data across segment tables (e.g., join `pid` with `obx` to link patient demographics to lab results).
- **Delta Mode -- Keep the Bronze Table Updated**: Use Auto Loader or a streaming table to continuously land new HL7 files from your Volume into the Bronze Delta table, so the connector always has fresh data to parse.

#### Troubleshooting

**Common Issues (both modes):**

- **Missing Segments**: Not all HL7 messages contain all segment types. ADT messages typically include MSH, EVN, PID, PV1 but not OBR/OBX. ORU messages include MSH, PID, ORC, OBR, OBX but may omit EVN, AL1, DG1.
- **Slow Ingestion**: Reduce `window_seconds` to process smaller time ranges per batch.

**GCP Mode Issues:**

- **Authentication Errors**: Verify that `service_account_json` contains the complete JSON key file contents and that the service account has the required Healthcare API permissions.
- **No Data Returned**: Check that the HL7v2 store contains messages and that the `location`, `dataset_id`, and `hl7v2_store_id` are correct. Use the GCP Console to verify.
- **GCP Quotas**: The Google Cloud Healthcare API has per-project quota limits. Check the [GCP quota console](https://console.cloud.google.com/iam-admin/quotas) and adjust pipeline frequency accordingly.

**Delta Mode Issues:**

- **Table Not Found**: Verify that `delta_table_name` is a fully-qualified three-level name (e.g., `my_catalog.bronze.hl7_raw`) and that the pipeline's service principal has `SELECT` permissions on the table.
- **No Data Returned**: Confirm the table has rows with `sendTime` values and that the `data` column contains valid HL7 message text. You can verify with: `SELECT sendTime, substring(data, 1, 50) FROM my_catalog.bronze.hl7_raw LIMIT 5` -- the `data` column should start with `MSH|`.
- **Parse Errors**: Ensure the `data` column contains the raw pipe-delimited HL7 text, not base64-encoded data. If your data is base64-encoded, decode it: `UPDATE my_table SET data = cast(unbase64(data) as STRING)`.
- **Out of Memory / Large Tables**: The default `delta_query_mode=preload` loads the entire table into memory at connector init. If your Bronze table has hundreds of thousands of rows or more, set `delta_query_mode` to `per_window` in the connection parameters. This issues a scoped SQL query per micro-batch instead, with no memory overhead.


## References

- [Google Cloud Healthcare API — HL7v2 Concepts](https://cloud.google.com/healthcare-api/docs/concepts/hl7v2)
- [Healthcare API — List Messages](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.hl7V2Stores.messages/list)
- [Healthcare API — Authentication](https://cloud.google.com/healthcare-api/docs/how-tos/hl7v2-messages)
- [HL7 Version 2.9 Specification](https://www.hl7.eu/HL7v2x/v29/hl7v29.htm)
- [Databricks Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Auto Loader (read_files)](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
