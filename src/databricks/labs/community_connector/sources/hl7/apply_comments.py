# Databricks notebook source
from __future__ import annotations
import sys

from databricks.labs.community_connector.sources.hl7.hl7_schemas import (
    SEGMENT_SCHEMAS,
    SEGMENT_TABLES,
    TABLE_DESCRIPTIONS,
)

# MAGIC %md
# MAGIC # HL7 — Apply Column & Table Comments to Unity Catalog
# MAGIC
# MAGIC Run this notebook **after** the LakeflowConnect pipeline has finished creating the HL7 streaming tables.
# MAGIC The column comments and table descriptions are defined in `hl7_schemas.py` but the LakeflowConnect
# MAGIC framework does not propagate `StructField` metadata to UC column descriptions automatically.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Fill in `catalog`, `schema`, and optionally `table_prefix` in the Parameters cell below.
# MAGIC 2. Run **Dry Run** to preview the SQL that will be executed.
# MAGIC 3. Run **Apply** to execute the DDL and populate the comments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# Edit these values before running
catalog = "my_catalog"       # UC catalog name
schema  = "my_schema"        # UC schema (database) name
table_prefix = ""            # Optional prefix prepended to each segment table name (e.g. "hl7_")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions

# COMMAND ----------

def _esc(s: str) -> str:
    """Escape single quotes for use inside SQL string literals."""
    return s.replace("'", "\\'")


def _run(sql: str, dry_run: bool) -> None:
    if dry_run:
        print(f"  SQL: {sql}")
    else:
        spark.sql(sql)  # noqa: F821 — spark is always defined in Databricks notebooks


def apply_hl7_comments(
    catalog: str,
    schema: str,
    table_prefix: str = "",
    dry_run: bool = False,
) -> None:
    """Apply column comments and table descriptions to all HL7 streaming tables.

    Args:
        catalog:      Unity Catalog catalog name.
        schema:       Schema (database) name.
        table_prefix: Optional prefix prepended to each table name (default: "").
        dry_run:      If True, print the SQL statements without executing them.
    """
    total_cols = 0
    total_tables = 0

    for seg in SEGMENT_TABLES:
        table_name = f"{table_prefix}{seg}"
        full_name = f"`{catalog}`.`{schema}`.`{table_name}`"

        struct_type = SEGMENT_SCHEMAS.get(seg)
        if struct_type is None:
            print(f"[SKIP] No schema found for segment '{seg}'", file=sys.stderr)
            continue

        # --- table description ---
        description = TABLE_DESCRIPTIONS.get(seg, "")
        if description:
            sql = f"COMMENT ON TABLE {full_name} IS '{_esc(description)}'"
            _run(sql, dry_run)

        # --- column comments ---
        col_count = 0
        for field in struct_type.fields:
            comment = field.metadata.get("comment", "")
            if not comment:
                continue
            sql = (
                f"ALTER TABLE {full_name} "
                f"ALTER COLUMN `{field.name}` "
                f"COMMENT '{_esc(comment)}'"
            )
            _run(sql, dry_run)
            col_count += 1

        print(
            f"[{'DRY RUN' if dry_run else 'OK'}] {full_name}: "
            f"{'would apply' if dry_run else 'applied'} {col_count} column comment(s)"
        )
        total_cols += col_count
        total_tables += 1

    print(
        f"\n{'[DRY RUN] Would apply' if dry_run else 'Applied'} comments to "
        f"{total_cols} columns across {total_tables} tables."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dry Run — preview SQL without executing

# COMMAND ----------

apply_hl7_comments(catalog=catalog, schema=schema, table_prefix=table_prefix, dry_run=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply — execute DDL to populate UC comments
# MAGIC
# MAGIC Uncomment and run the cell below when you are ready to apply.

# COMMAND ----------

# apply_hl7_comments(catalog=catalog, schema=schema, table_prefix=table_prefix, dry_run=False)
