# HL7 v2 Connector — UI Setup Guide

This guide walks through creating HL7 v2 ingestion pipelines via the Databricks UI using the **Custom Connector** flow. Since the HL7 v2 connector is not yet published to the main repository, you point the UI at your own fork.

Both GCP and Delta modes use the same source code and Git repo — the only difference is which connection (and credentials) the pipeline uses.

---

## Prerequisite: Code Is Pushed to Your Fork

Your HL7 v2 connector code must be pushed to your fork:

```
https://github.com/Adityasekar/lakeflow-community-connectors
```

The source name is `hl7_v2` (matching the directory name under `sources/`).

---

## Creating Pipeline #1: GCP Mode

### Step 1 — Add the Custom Connector

1. In your Databricks workspace, click **Jobs & Pipelines** in the left sidebar
2. Click **Create new** dropdown > **Ingestion pipeline** ("Ingest data from apps, databases and files")
3. In the "Add data" screen, scroll down to the **Community connectors** section
4. Click **Custom Connector**
5. In the "Add custom connector" dialog:
   - **Source name**: `hl7_v2`
   - **Git Repository URL**: `https://github.com/Adityasekar/lakeflow-community-connectors`
6. Click **Add Connector**
7. The HL7 v2 connector now appears in the Community connectors list

### Step 2 — Select the Connector and Configure Connection

1. Click on your **hl7_v2** connector in the Community connectors section
2. This opens the connection setup wizard — "Ingest data from hl7_v2"
3. Under "Connection to the source", click **Create connection**
4. Give it a name, e.g. `hl7_v2_gcp_connection`
5. The UI should show the structured form from your `connector_spec.yaml`. Fill in the parameters below.

#### Where to find GCP connection parameters

| Parameter | Value to enter | Where to find it |
|---|---|---|
| `source_type` | `gcp` (or leave blank — defaults to `gcp`) | N/A |
| `project_id` | Your GCP project ID (e.g. `my-gcp-project`) | **GCP Console** — visible in the project selector dropdown at the top of the page |
| `location` | GCP region (e.g. `us-central1`, `northamerica-northeast1`) | **GCP Console** → **Healthcare > Datasets** → click your dataset — the location is shown in the breadcrumb next to the dataset name |
| `dataset_id` | Healthcare API dataset ID (e.g. `My_Clinical_Dataset`) | **GCP Console** → **Healthcare > Datasets** → the dataset name listed on the page |
| `hl7v2_store_id` | HL7v2 store name (e.g. `ehr_messages`) | **GCP Console** → **Healthcare > Datasets** → click your dataset → click the HL7v2 store — the store ID is in the Details section |
| `service_account_json` | Paste the **entire contents** of your GCP service account JSON key file | **GCP Console** → **IAM & Admin > Service Accounts** → select or create a service account → **Keys** tab → **Add Key** → **Create new key** → **JSON**. A `.json` file downloads — paste its full contents. |

> **Service account permissions required**: The service account must have `healthcare.hl7V2Messages.list` and `healthcare.hl7V2Messages.get`. The easiest way is to grant the **Healthcare HL7v2 Message Editor** role.

> **Tip**: The full resource path for your HL7v2 store follows this pattern — you can find all four values from this single string on the store details page:
> ```
> projects/{project_id}/locations/{location}/datasets/{dataset_id}/hl7V2Stores/{hl7v2_store_id}
> ```

#### When to add `externalOptionsAllowList`

The `externalOptionsAllowList` lets you pass per-table configuration options (like `window_seconds`, `segment_type`, `start_timestamp`) through the pipeline spec to the connector. Without it, those options are silently ignored.

- **If the UI shows the structured form** (with labeled fields from `connector_spec.yaml`): The `externalOptionsAllowList` is typically handled automatically by the framework. You should **not** need to add it manually.
- **If the UI shows only generic key-value pairs** (i.e., it couldn't load the spec from your repo — this can happen if the branch name doesn't match, or the repo is private): You must add it as an explicit key-value entry:
  - **Key**: `externalOptionsAllowList`
  - **Value**: `segment_type,window_seconds,start_timestamp`

> **Rule of thumb**: If you plan to use `window_seconds`, `start_timestamp`, or `segment_type` in any of your table configurations, make sure `externalOptionsAllowList` is set on the connection. If in doubt, add it — it does no harm.

6. Click **Next** to create the connection

### Step 3 — Configure Ingestion Setup

1. **Pipeline name**: Enter a descriptive name (e.g. `hl7_v2_gcp_pipeline`)
2. **Event log location**:
   - **Catalog**: Select a catalog (e.g. `main` or your user catalog)
   - **Schema**: Select or create a schema (e.g. `hl7_gcp`)
3. **Root path**: Enter the workspace path where the pipeline will store its source code, metadata, and checkpoint information
   - Example: `/Users/your.email@company.com/hl7_v2_gcp_pipeline`
   - This is a workspace path (not a DBFS or Volume path)
4. Click **Create** to create the ingestion pipeline

### Step 4 — Configure Pipeline Source Code

Once created, you'll be redirected to the pipeline details page.

1. Click **Open in Editor** to edit the auto-generated `ingest.py`
2. The auto-generated skeleton won't have your table configuration — **you must replace it** with the actual pipeline spec
3. Paste the following, adjusting `connection_name`, catalog, schema, and tables as needed:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

source_name = "hl7_v2"
connection_name = "hl7_v2_gcp_connection"

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")
register(spark, source_name)

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "msh",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "pid",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "pv1",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "obx",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "obr",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
    ],
}

ingest(spark, pipeline_spec)
```

4. Save your changes

### Step 5 — Run the Pipeline

1. Return to the pipeline details page
2. Click **Start** to kick off the first ingestion run
3. After the first run, you'll see a visual DAG showing each segment table

---

## Creating Pipeline #2: Delta Mode

### Step 1 — Add the Custom Connector (if not already added)

If you already added the custom connector during the GCP setup, it persists — you don't need to add it again. Just create a new ingestion pipeline:

1. **Jobs & Pipelines** > **Create new** > **Ingestion pipeline**
2. Find **hl7_v2** in the Community connectors list and click on it

If you haven't added it yet, follow Step 1 from the GCP section above (same source name and Git repo URL).

### Step 2 — Create the Delta Connection

1. Under "Connection to the source", click **Create connection**
2. Name it, e.g. `hl7_v2_delta_connection`
3. Fill in the parameters:

| Parameter | Value to enter | Where to find it |
|---|---|---|
| `source_type` | `delta` (**required** — this switches the connector to Delta mode) | N/A — just type `delta` |
| `delta_table_name` | Fully-qualified 3-level name (e.g. `my_catalog.bronze.hl7_raw`) | The catalog, schema, and table name of your Bronze table in Unity Catalog |
| `databricks_host` | Your workspace URL (e.g. `https://my-workspace.cloud.databricks.com`) | Your browser address bar when logged into the workspace, or **Settings > Workspace details** |
| `databricks_token` | A Databricks personal access token (starts with `dapi...`) | **Settings > Developer > Access tokens** → **Generate new token** → copy the generated token |
| `sql_warehouse_id` | SQL warehouse ID (e.g. `01370556fad60fda`) | **SQL Warehouses** → click your warehouse → **Connection details** tab, or from the URL: `.../sql/warehouses/<warehouse_id>` |

> **Why are Databricks credentials needed?** The Lakeflow Connect pipeline framework runs connector code in a subprocess where SparkSession is unavailable. To read the Delta table, the connector queries it via the SQL Statement Execution REST API, which requires explicit workspace credentials.

> **Tip**: Use a **serverless SQL warehouse** for cost efficiency — it scales to zero when idle.

#### When to add `externalOptionsAllowList`

Same rule as GCP mode:
- If the UI shows only generic key-value pairs, add: Key = `externalOptionsAllowList`, Value = `segment_type,window_seconds,start_timestamp`
- If the structured form loaded from `connector_spec.yaml`, it's handled automatically

4. Click **Next** to create the connection

### Step 3 — Configure Ingestion Setup

1. **Pipeline name**: e.g. `hl7_v2_delta_pipeline`
2. **Event log location**:
   - **Catalog**: Select your catalog
   - **Schema**: e.g. `hl7_delta`
3. **Root path**: Workspace path for pipeline assets
   - Example: `/Users/your.email@company.com/hl7_v2_delta_pipeline`
4. Click **Create**

### Step 4 — Configure Pipeline Source Code

1. Click **Open in Editor**
2. Replace the auto-generated code with:

```python
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

source_name = "hl7_v2"
connection_name = "hl7_v2_delta_connection"

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")
register(spark, source_name)

pipeline_spec = {
    "connection_name": connection_name,
    "objects": [
        {
            "table": {
                "source_table": "msh",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "pid",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "pv1",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "obx",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
        {
            "table": {
                "source_table": "obr",
                "table_configuration": {"window_seconds": "86400"}
            }
        },
    ],
}

ingest(spark, pipeline_spec)
```

3. Save your changes

### Step 5 — Run the Pipeline

Click **Start** on the pipeline page.

---

## Delta Mode Prerequisite: Bronze Table

Before creating the Delta pipeline, your Bronze Delta table must exist with these columns:

| Column | Type | Required | Description |
|---|---|---|---|
| `data` | STRING | Yes | Raw HL7 v2 pipe-delimited message text |
| `sendTime` | STRING or TIMESTAMP | Yes | Timestamp used as the incremental cursor |
| `name` | STRING | No | Source identifier for traceability |

If loading from files on a Databricks Volume:

```sql
CREATE TABLE my_catalog.bronze.hl7_raw AS
SELECT
  string(content)      AS data,
  modificationTime     AS sendTime,
  path                 AS name
FROM read_files(
  '/Volumes/my_catalog/my_schema/hl7_volume/',
  format => 'binaryFile'
);
```

---

## Supported Tables (Segment Types)

Both modes support the same tables. Start small with a few key segments:

| Table | Segment | Description |
|---|---|---|
| `msh` | MSH | Message Header — one row per HL7 message |
| `pid` | PID | Patient Identification — demographics |
| `pv1` | PV1 | Patient Visit — encounter/admission data |
| `obx` | OBX | Observation Result — lab results |
| `obr` | OBR | Observation Request — lab/radiology orders |

See the [main README](README.md) for the full list of 21+ segment types.

---

## Per-Table Configuration Options

These go inside `table_configuration` for each table in the pipeline spec:

| Option | Default | Description |
|---|---|---|
| `window_seconds` | `86400` (1 day) | Sliding window duration in seconds. Use `3600` for high-volume, `60` for testing. |
| `start_timestamp` | Auto-discovered | RFC 3339 timestamp to start reading from (e.g. `2024-01-01T00:00:00Z`). If omitted, auto-discovers the oldest message. |
| `segment_type` | Same as table name | Override to ingest custom Z-segments (e.g. set to `ZPD`). |

---

## Key Points

- **Both pipelines use the same Custom Connector** (same Git repo and source name `hl7_v2`). The connection credentials determine GCP vs Delta mode.
- **The Custom Connector only needs to be added once** — after that, `hl7_v2` stays in your Community connectors list for future pipelines.
- **The `ingest.py` replacement is critical** — the auto-generated skeleton won't work without your pipeline spec.
- **Start small** with `msh`, `pid`, `pv1`, `obx`, `obr` and add more once you confirm data flows correctly.
- **Join across tables** using the `message_id` column to link patients to lab results, diagnoses, etc.
