"""Tests for the HL7 v2 community connector (GCP Healthcare API).

Runs the standard LakeflowConnectTests suite against a live GCP Healthcare
API HL7v2 store.

Update configs/dev_config.json with your GCP credentials and
configs/dev_table_config.json with appropriate table options before running.
"""

from databricks.labs.community_connector.sources.hl7.hl7 import HL7LakeflowConnect
from tests.unit.sources.test_suite import LakeflowConnectTests


class TestHL7Connector(LakeflowConnectTests):
    connector_class = HL7LakeflowConnect
    sample_records = 5
