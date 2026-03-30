# HL7 v2 Connector — UI Setup Guide

This guide walks through creating HL7 v2 ingestion pipelines via the Databricks UI using the **Custom Connector** flow. Since the HL7 v2 connector is not yet published to the main repository, you point the UI to the fork mentioned below.

Both GCP and Delta modes use the same source code and Git repo — the only difference is which connection (and credentials) the pipeline uses.

For prerequisites, supported tables, per-table configuration options, and connection parameter details, see the [HL7 v2 Connector README](README.md).

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

If the UI shows only generic key-value pairs (i.e., it couldn't load the spec from your repo), you must add `externalOptionsAllowList` as an explicit key-value entry:
- **Key**: `externalOptionsAllowList`
- **Value**: `segment_type,window_seconds,start_timestamp`

If the UI shows the structured form (with labeled fields from `connector_spec.yaml`), it is typically handled automatically.

> For a detailed explanation of what `externalOptionsAllowList` does and how each table-level option works, see the [HL7 v2 Connector README — Table-Level Options](README.md#table-level-options).

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
3. Paste the following, adjusting `connection_name` and tables as needed (see [HL7 v2 Connector README — Supported Objects](README.md#supported-objects) for the full list of segment types):

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
3. Fill in the parameters (see [HL7 v2 Connector README — Delta Mode Connection Parameters](README.md#delta-mode-connection-parameters) for where to find each value):

| Parameter | Value to enter |
|---|---|
| `source_type` | `delta` (**required** — this switches the connector to Delta mode) |
| `delta_table_name` | Fully-qualified 3-level name (e.g. `my_catalog.bronze.hl7_raw`) |
| `databricks_host` | Your workspace URL (e.g. `https://my-workspace.cloud.databricks.com`) |
| `databricks_token` | A Databricks personal access token (starts with `dapi...`) |
| `sql_warehouse_id` | SQL warehouse ID (e.g. `01370556fad60fda`) |

> If the UI shows only generic key-value pairs, also add `externalOptionsAllowList` = `segment_type,window_seconds,start_timestamp`. See [above](#when-to-add-externaloptionsallowlist).

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

## Key Points

- **Both pipelines use the same Custom Connector** (same Git repo and source name `hl7_v2`). The connection credentials determine GCP vs Delta mode.
- **The Custom Connector only needs to be added once** — after that, `hl7_v2` stays in your Community connectors list for future pipelines.
- **The `ingest.py` replacement is critical** — the auto-generated skeleton won't work without your pipeline spec.
- For supported tables, per-table configuration options, prerequisites, and troubleshooting, see the [HL7 v2 Connector README](README.md).
