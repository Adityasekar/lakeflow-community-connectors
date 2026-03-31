"""Tests for DG1 (Diagnosis) segment extraction.

DG1 contains diagnosis data: coding method, ICD code, text, type, priority.
Multiple DG1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_dg1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestDG1Extraction:
    def test_adt_dg1(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "DG1", _extract_dg1)
        assert row["diagnosis_coding_method"] == "ICD10"
        assert row["diagnosis_id"] == "J18.9"
        assert row["diagnosis_text"] == "Pneumonia unspecified"
        assert row["diagnosis_type"] == "A"

    def test_comprehensive_multiple_dg1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        segs = segments_of_type(msg, "DG1")
        assert len(segs) == 2
        row1 = _extract_dg1(segs[0])
        assert row1["diagnosis_id"] == "I21.0"
        assert row1["diagnosis_priority"] == 1
        row2 = _extract_dg1(segs[1])
        assert row2["diagnosis_id"] == "I50.9"
        assert row2["diagnosis_type"] == "W"
        assert row2["diagnosis_text"] == "Heart failure unspecified"


class TestDG1MissingFields:
    def test_minimal_dg1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "DG1|1"
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["set_id"] == 1
        assert row["diagnosis_coding_method"] is None
        assert row["diagnosis_id"] is None
        assert row["diagnosis_text"] is None
        assert row["diagnosis_type"] is None
        assert row["diagnosis_priority"] is None

    def test_dg1_code_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "DG1|1|ICD10|E11.9^Type 2 diabetes^I10"
        )
        row = _extract_dg1(msg.get_segment("DG1"))
        assert row["diagnosis_coding_method"] == "ICD10"
        assert row["diagnosis_id"] == "E11.9"
        assert row["diagnosis_text"] == "Type 2 diabetes"
        assert row["diagnosis_coding_system"] == "I10"
