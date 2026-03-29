# Lakeflow HL7 v2 Community Connector

This documentation provides setup instructions and reference information for the HL7 v2 source connector.

The Lakeflow HL7 v2 Connector ingests HL7 v2 messages from a **Google Cloud Healthcare API HL7v2 store** and loads each segment type into its own Delta table. This enables structured analytics over clinical data that was originally transmitted in the HL7 v2 pipe-delimited format. The connector supports incremental append-only ingestion, automatically tracking new messages by their API send time.

## Prerequisites

- A **Google Cloud Platform** project with the **Cloud Healthcare API** enabled
- An **HL7v2 store** within a Healthcare API dataset, containing HL7 v2 messages
- A **GCP service account** with the following IAM permissions:
  - `healthcare.hl7V2Messages.list`
  - `healthcare.hl7V2Messages.get`
- A JSON key file for the service account

## Setup

### Required Connection Parameters

To configure the connector, provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|---|---|---|---|---|
| `project_id` | string | Yes | GCP project ID containing the Healthcare dataset | `my-gcp-project` |
| `location` | string | Yes | GCP region where the Healthcare dataset resides | `us-central1` |
| `dataset_id` | string | Yes | Google Cloud Healthcare API dataset ID | `My_Clinical_Dataset` |
| `hl7v2_store_id` | string | Yes | HL7v2 store name within the Healthcare dataset | `ehr_messages` |
| `service_account_json` | string | Yes | Full JSON content of a GCP service account key file | `{"type": "service_account", ...}` |

### Table-Level Options

The following options can be set per table via `table_configuration`:

| Option | Type | Default | Description |
|---|---|---|---|
| `segment_type` | string | *(table name)* | Override the HL7 segment type to extract. Use this to ingest custom Z-segments (e.g., `ZPD`) through any table name. |
| `window_seconds` | string | `86400` | Duration in seconds of the sliding time window used for incremental reads. Smaller values produce smaller, faster batches. |
| `start_timestamp` | string | *(auto-discovered)* | RFC 3339 timestamp to start reading from when no prior offset exists (e.g., `2024-01-01T00:00:00Z`). If omitted, the connector auto-discovers the oldest message in the store. |

To use table-level options, include them in the `externalOptionsAllowList` connection parameter:

```
segment_type,window_seconds,start_timestamp
```

### Obtaining the Connection Parameters

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

All tables use **append-only** incremental ingestion:

- New messages are tracked by the `sendTime` timestamp from the Google Cloud Healthcare API (stored as the `send_time` column).
- The connector uses a **sliding time window** to bound each micro-batch, advancing the cursor forward with each run.
- Messages already processed are never re-fetched. No change feeds, upserts, or deletes are produced.

### Common Metadata Columns

Every table includes the following columns for traceability and cross-table joins:

| Column | Description |
|---|---|
| `message_id` | Unique message control ID from MSH-10 — the primary join key across all tables |
| `message_timestamp` | Message date/time string from MSH-7 (e.g., `20240115120000`) |
| `hl7_version` | HL7 version (e.g., `2.5.1`) from MSH-12 |
| `source_file` | API resource name of the source HL7 message for traceability |
| `send_time` | Message send time from the GCP Healthcare API (RFC 3339); used as the incremental cursor |
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

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. Only applicable to tables with CDC or SNAPSHOT ingestion mode; APPEND_ONLY tables do not support this option. |
| `primary_keys` | No | List of columns to override the connector's default primary keys |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking |

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
- **Use Incremental Sync**: The connector automatically tracks progress via the `sendTime` cursor, minimizing redundant API calls.
- **Tune the Window Size**: For high-volume stores, reduce `window_seconds` (e.g., `3600` for 1-hour windows) to keep each micro-batch manageable. For low-volume stores, the default of `86400` (1 day) works well.
- **Monitor GCP Quotas**: The Google Cloud Healthcare API has per-project quota limits. Check the [GCP quota console](https://console.cloud.google.com/iam-admin/quotas) and adjust pipeline frequency accordingly.
- **Join Across Tables**: Use the `message_id` column to join data across segment tables (e.g., join `pid` with `obx` to link patient demographics to lab results).

#### Troubleshooting

**Common Issues:**

- **Authentication Errors**: Verify that `service_account_json` contains the complete JSON key file contents and that the service account has the required Healthcare API permissions.
- **No Data Returned**: Check that the HL7v2 store contains messages and that the `location`, `dataset_id`, and `hl7v2_store_id` are correct. Use the GCP Console to verify.
- **Slow Ingestion**: Reduce `window_seconds` to process smaller time ranges per batch. Ensure the HL7v2 store is in a region with low latency to your Databricks workspace.
- **Missing Segments**: Not all HL7 messages contain all segment types. ADT messages typically include MSH, EVN, PID, PV1 but not OBR/OBX. ORU messages include MSH, PID, ORC, OBR, OBX but may omit EVN, AL1, DG1.


## References

- [Google Cloud Healthcare API — HL7v2 Concepts](https://cloud.google.com/healthcare-api/docs/concepts/hl7v2)
- [Healthcare API — List Messages](https://cloud.google.com/healthcare-api/docs/reference/rest/v1/projects.locations.datasets.hl7V2Stores.messages/list)
- [Healthcare API — Authentication](https://cloud.google.com/healthcare-api/docs/how-tos/hl7v2-messages)
- [HL7 Version 2.9 Specification](https://www.hl7.eu/HL7v2x/v29/hl7v29.htm)
