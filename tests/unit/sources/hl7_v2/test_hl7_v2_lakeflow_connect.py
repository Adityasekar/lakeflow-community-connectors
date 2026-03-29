"""Tests for the HL7 v2 community connector (GCP Healthcare API).

Runs the standard LakeflowConnectTests suite against a live GCP Healthcare
API HL7v2 store.

Update configs/dev_config_gcp.json with your GCP credentials and
configs/dev_table_config.json with appropriate table options before running.
"""

import json

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import HL7V2LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestHL7V2Connector(LakeflowConnectTests):
    connector_class = HL7V2LakeflowConnect
    sample_records = 5

    @classmethod
    def _load_config(cls) -> dict:
        path = cls._config_dir() / "dev_config_gcp.json"
        assert path.exists(), (
            f"Config file not found: {path}\n"
            "  Fix: Create dev_config_gcp.json with GCP connection options."
        )
        with open(path, "r") as f:
            return json.load(f)
