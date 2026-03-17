"""Tests for the HL7 v2 community connector.

Runs against the bundled sample .hl7 files in samples/ by default so no
external Databricks Volume is required.

To test against a real volume, update configs/dev_config.json and remove
the cls.config override in setup_class below.
"""

from pathlib import Path

import pytest

from databricks.labs.community_connector.sources.hl7.hl7 import HL7LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests

_SAMPLES_DIR = Path(__file__).parent / "samples"


class TestHL7Connector(LakeflowConnectTests):
    connector_class = HL7LakeflowConnect

    @classmethod
    def setup_class(cls):
        # Use the bundled sample files so the suite runs without any
        # external setup.  To test against a real Databricks Volume,
        # update configs/dev_config.json and comment out these two lines.
        cls.config = {"volume_path": str(_SAMPLES_DIR)}
        super().setup_class()

    # ------------------------------------------------------------------
    # HL7-specific tests
    # ------------------------------------------------------------------

    def test_message_id_populated(self):
        """Every row has a non-null message_id (from MSH-10)."""
        errors = []
        for table in self.connector.list_tables():
            records, _ = self.connector.read_table(table, {}, {})
            for i, rec in enumerate(records):
                if rec.get("message_id") is None:
                    errors.append(f"[{table}] record {i}: message_id is None")
                    break
        if errors:
            pytest.fail("\n".join(errors))

    def test_source_file_populated(self):
        """Every row has a non-null source_file."""
        errors = []
        for table in self.connector.list_tables():
            records, _ = self.connector.read_table(table, {}, {})
            for i, rec in enumerate(records):
                if rec.get("source_file") is None:
                    errors.append(f"[{table}] record {i}: source_file is None")
                    break
        if errors:
            pytest.fail("\n".join(errors))

    def test_obx_multiple_per_message(self):
        """OBX table returns multiple rows (one per OBX segment per message)."""
        records, _ = self.connector.read_table("obx", {}, {})
        obx_list = list(records)
        assert len(obx_list) >= 3, (
            f"Expected >= 3 OBX rows (5 in sample_oru.hl7), got {len(obx_list)}.\n"
            "  Fix: read_table('obx') must yield one row per OBX segment."
        )

    def test_incremental_cursor_no_duplicates(self):
        """Second read with the returned offset yields no new records (static files)."""
        # First run — read everything
        records1, offset1 = self.connector.read_table("pid", {}, {})
        list(records1)  # consume iterator

        assert offset1 is not None, "First offset must not be None for append tables."
        assert "cursor" in offset1, f"Expected 'cursor' key in offset, got {offset1}"

        # Second run — nothing is new
        fresh = HL7LakeflowConnect(self.config)
        records2, offset2 = fresh.read_table("pid", offset1, {})
        rows2 = list(records2)

        assert len(rows2) == 0, (
            f"Expected 0 records on second run (no new files), got {len(rows2)}.\n"
            "  Fix: Files already seen should not be re-read."
        )

    def test_z_segment_via_table_options(self):
        """A custom segment type can be read via segment_type table_option."""
        # Use "msh" as the table name but override segment_type — schema should
        # fall back to GENERIC_SEGMENT_SCHEMA for an unknown segment type.
        from databricks.labs.community_connector.sources.hl7.hl7_schemas import (
            GENERIC_SEGMENT_SCHEMA,
        )

        schema = self.connector.get_table_schema("msh", {"segment_type": "ZPD"})
        assert schema == GENERIC_SEGMENT_SCHEMA, (
            "Expected GENERIC_SEGMENT_SCHEMA for unknown segment type 'ZPD'."
        )

    def test_pid_field_extraction(self):
        """PID rows contain correctly parsed demographic fields."""
        records, _ = self.connector.read_table("pid", {}, {})
        rows = list(records)
        assert rows, "Expected at least one PID row."
        row = rows[0]
        assert row.get("patient_family_name") == "Doe", (
            f"Expected patient_family_name='Doe', got {row.get('patient_family_name')!r}"
        )
        assert row.get("patient_given_name") == "John", (
            f"Expected patient_given_name='John', got {row.get('patient_given_name')!r}"
        )
        assert row.get("date_of_birth") == "19700315", (
            f"Expected date_of_birth='19700315', got {row.get('date_of_birth')!r}"
        )
        assert row.get("administrative_sex") == "M", (
            f"Expected administrative_sex='M', got {row.get('administrative_sex')!r}"
        )

    def test_msh_field_extraction(self):
        """MSH rows contain correctly parsed header fields."""
        records, _ = self.connector.read_table("msh", {}, {})
        rows = list(records)
        assert rows, "Expected at least one MSH row."
        # Find the ADT message
        adt = next((r for r in rows if r.get("message_code") == "ADT"), None)
        assert adt is not None, "Expected an ADT message in MSH rows."
        assert adt.get("trigger_event") == "A01"
        assert adt.get("sending_application") == "HIS"
        assert adt.get("hl7_version") == "2.5.1"

    def test_obr_field_extraction(self):
        """OBR rows contain correctly parsed order fields."""
        records, _ = self.connector.read_table("obr", {}, {})
        rows = list(records)
        assert rows, "Expected at least one OBR row."
        row = rows[0]
        assert row.get("service_id") == "80048", (
            f"Expected service_id='80048', got {row.get('service_id')!r}"
        )
        assert row.get("result_status") == "F", (
            f"Expected result_status='F', got {row.get('result_status')!r}"
        )

    def test_obx_field_extraction(self):
        """OBX rows contain correctly parsed observation fields."""
        records, _ = self.connector.read_table("obx", {}, {})
        rows = list(records)
        assert rows, "Expected at least one OBX row."
        sodium = next((r for r in rows if r.get("observation_id") == "2951-2"), None)
        assert sodium is not None, "Expected OBX row for Sodium (LOINC 2951-2)."
        assert sodium.get("observation_value") == "138"
        assert sodium.get("units_code") == "mEq/L"
        assert sodium.get("observation_result_status") == "F"
