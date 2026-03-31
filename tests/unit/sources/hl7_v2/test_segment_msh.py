"""Tests for MSH (Message Header) segment extraction.

Validated against HL7 v2.5–v2.9 specification.
MSH is present in every HL7 message — one row per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_msh
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


# ── Extraction: happy path ──────────────────────────────────────────────────

class TestMSHExtraction:
    def test_adt_msh(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["sending_application"] == "HIS"
        assert row["sending_facility"] == "GENERAL_HOSPITAL"
        assert row["receiving_application"] == "LAB"
        assert row["message_code"] == "ADT"
        assert row["trigger_event"] == "A01"
        assert row["message_structure"] == "ADT_A01"
        assert row["version_id"] == "2.5.1"
        assert row["processing_id"] == "P"

    def test_oru_covid_msh(self):
        msg = parse_first(load_sample("sample_oru_covid.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["message_code"] == "ORU"
        assert row["trigger_event"] == "R01"
        assert row["version_id"] == "2.5"

    def test_celr_msh_with_timezone(self):
        msg = parse_first(load_sample("sample_oru_lab_celr.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["message_code"] == "ORU"
        assert row["version_id"] == "2.5.1"
        assert row["message_datetime"] is not None

    def test_flu_ar_msh_security(self):
        msg = parse_first(load_sample("sample_oru_flu_ar.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["security"] == "36225"


# ── Extraction: field details ───────────────────────────────────────────────

class TestMSHFieldDetails:
    def test_field_separator_is_pipe(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["field_separator"] == "|"

    def test_encoding_characters(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["encoding_characters"] == "^~\\&"

    def test_message_control_id_present(self):
        msg = parse_first(load_sample("sample_adt.hl7"))
        row = extract_segment(msg, "MSH", _extract_msh)
        assert row["message_control_id"] is not None


# ── Extraction: missing fields ──────────────────────────────────────────────

class TestMSHMissingFields:
    def test_minimal_msh(self):
        msg = parse_message("MSH|^~\\&|SYS|FAC|RCV|FAC|20240101||ADT^A01|1|P|2.5")
        msh = msg.get_segment("MSH")
        row = _extract_msh(msh)
        assert row["security"] is None
        assert row["sequence_number"] is None
        assert row["country_code"] is None
        assert row["character_set"] is None

    def test_all_output_keys_present(self):
        msg = parse_message("MSH|^~\\&|A|B|C|D|20240101||ADT^A01|1|P|2.5")
        row = _extract_msh(msg.get_segment("MSH"))
        expected_keys = {
            "field_separator", "encoding_characters", "sending_application",
            "sending_facility", "receiving_application", "receiving_facility",
            "message_datetime", "security", "message_code", "trigger_event",
            "message_structure", "message_control_id", "processing_id",
            "version_id", "sequence_number", "continuation_pointer",
            "accept_acknowledgment_type", "application_acknowledgment_type",
            "country_code", "character_set", "principal_language",
            "alt_character_set_handling", "message_profile_identifier",
            "message_profile_namespace_id", "message_profile_universal_id",
            "message_profile_universal_id_type", "sending_responsible_org",
            "receiving_responsible_org", "sending_network_address",
            "receiving_network_address", "security_classification_tag",
            "security_handling_instructions", "special_access_restriction",
        }
        assert set(row.keys()) == expected_keys
