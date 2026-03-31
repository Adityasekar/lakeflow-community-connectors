"""Tests for PR1 (Procedures) segment extraction.

PR1 contains procedure data: code, description, datetime, functional type,
anesthesiologist, duration. Multiple PR1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_pr1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestPR1Extraction:
    def test_comprehensive_pr1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "PR1", _extract_pr1)
        assert row["procedure_id"] == "02703ZZ"
        assert row["procedure_text"] == "Dilation of Coronary Artery, Percutaneous Approach"
        assert row["procedure_functional_type"] == "A"
        assert row["procedure_minutes"] == 45
        assert row["anesthesiologist"] == "ANE001"
        assert row["procedure_coding_method"] == "ICD10PCS"


class TestPR1MissingFields:
    def test_minimal_pr1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PR1|1"
        )
        row = _extract_pr1(msg.get_segment("PR1"))
        assert row["set_id"] == 1
        assert row["procedure_coding_method"] is None
        assert row["procedure_id"] is None
        assert row["procedure_text"] is None
        assert row["procedure_functional_type"] is None
        assert row["procedure_minutes"] is None
        assert row["anesthesiologist"] is None

    def test_pr1_code_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "PR1|1|CPT|99213^Office Visit^CPT4"
        )
        row = _extract_pr1(msg.get_segment("PR1"))
        assert row["procedure_coding_method"] == "CPT"
        assert row["procedure_id"] == "99213"
        assert row["procedure_text"] == "Office Visit"
        assert row["procedure_coding_system"] == "CPT4"
