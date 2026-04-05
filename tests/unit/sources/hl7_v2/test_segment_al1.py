"""Tests for AL1 (Patient Allergy Information) segment extraction.

AL1 contains allergy data: allergen type, code, severity, reaction.
Multiple AL1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_al1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestAL1Extraction:
    def test_adt_al1(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "AL1", _extract_al1)
        assert row["allergen_type_code"] == "DA"
        assert row["allergen_code"] == "PENICILLIN"
        assert row["allergy_reaction"] == "HIVES"

    def test_comprehensive_multiple_al1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        segs = segments_of_type(msg, "AL1")
        assert len(segs) == 2
        row1 = _extract_al1(segs[0])
        assert row1["allergen_code"] == "ASPIRIN"
        assert row1["allergy_severity_code"] == "SV"
        assert row1["allergy_reaction"] == "ANAPHYLAXIS"
        row2 = _extract_al1(segs[1])
        assert row2["allergen_type_code"] == "FA"
        assert row2["allergen_code"] == "SHELLFISH"


class TestAL1MissingFields:
    def test_minimal_al1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "AL1|1"
        )
        row = _extract_al1(msg.get_segment("AL1"))
        assert row["set_id"] == 1
        assert row["allergen_type_code"] is None
        assert row["allergen_code"] is None
        assert row["allergy_severity_code"] is None
        assert row["allergy_reaction"] is None
        assert row["identification_date"] is None

    def test_al1_type_and_code_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5\r"
            "AL1|1|DA^Drug Allergy|CODEINE^Codeine"
        )
        row = _extract_al1(msg.get_segment("AL1"))
        assert row["allergen_type_code"] == "DA"
        assert row["allergen_code"] == "CODEINE"
        assert row["allergen_code_text"] == "Codeine"
