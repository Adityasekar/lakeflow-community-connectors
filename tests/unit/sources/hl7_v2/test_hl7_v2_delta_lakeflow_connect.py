"""Tests for the HL7 v2 community connector (Delta table source mode).

Runs the standard LakeflowConnectTests suite against a Delta table containing
raw HL7 v2 messages.

Update configs/dev_config_delta.json with your Delta table name and
configs/dev_table_config_delta.json with appropriate table options before running.
"""

import json

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import HL7V2LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestHL7V2DeltaConnector(LakeflowConnectTests):
    connector_class = HL7V2LakeflowConnect
    sample_records = 5

    @classmethod
    def _load_config(cls) -> dict:
        path = cls._config_dir() / "dev_config_delta.json"
        assert path.exists(), (
            f"Config file not found: {path}\n"
            "  Fix: Create dev_config_delta.json with Delta table connection options."
        )
        with open(path, "r") as f:
            return json.load(f)

    @classmethod
    def _load_table_configs(cls):
        path = cls._config_dir() / "dev_table_config_delta.json"
        if not path.exists():
            return {}
        with open(path, "r") as f:
            return json.load(f)
