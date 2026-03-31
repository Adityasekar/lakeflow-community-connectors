"""Tests for FT1 (Financial Transaction) segment extraction.

FT1 contains charge/billing data: transaction type, code, description,
quantity, batch ID. Multiple FT1 segments can appear per message.
"""
from __future__ import annotations

from tests.unit.sources.hl7_v2._helpers import extract_segment, load_sample, parse_first, segments_of_type

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import _extract_ft1
from databricks.labs.community_connector.sources.hl7_v2.hl7_v2_parser import (
    parse_message,
)


class TestFT1Extraction:
    def test_comprehensive_ft1(self):
        msg = parse_first(load_sample("sample_adt_comprehensive.hl7"))
        row = extract_segment(msg, "FT1", _extract_ft1)
        assert row["transaction_type"] == "CG"
        assert row["transaction_code_id"] == "99285"
        assert row["transaction_code_text"] == "Emergency dept visit, high severity"
        assert row["transaction_quantity"] == 1
        assert row["transaction_description"] == "Emergency Department Visit"

    def test_dft_multiple_ft1(self):
        msg = parse_first(load_sample("sample_dft_financial.hl7"))
        segs = segments_of_type(msg, "FT1")
        assert len(segs) == 2
        row1 = _extract_ft1(segs[0])
        assert row1["transaction_code_id"] == "27447"
        assert row1["transaction_batch_id"] == "BATCH001"
        row2 = _extract_ft1(segs[1])
        assert row2["transaction_code_id"] == "01402"
        assert row2["transaction_description"] == "Anesthesia for Knee Surgery"


class TestFT1MissingFields:
    def test_minimal_ft1(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||DFT^P03|1|P|2.5\r"
            "FT1|1"
        )
        row = _extract_ft1(msg.get_segment("FT1"))
        assert row["set_id"] == 1
        assert row["transaction_id"] is None
        assert row["transaction_batch_id"] is None
        assert row["transaction_type"] is None
        assert row["transaction_code_id"] is None
        assert row["transaction_description"] is None
        assert row["transaction_quantity"] is None

    def test_ft1_charge_only(self):
        msg = parse_message(
            "MSH|^~\\&|A|B|C|D|20240101||DFT^P03|1|P|2.5\r"
            "FT1|1||BATCH99|||CG|99213^Office Visit^CPT4|Office visit"
        )
        row = _extract_ft1(msg.get_segment("FT1"))
        assert row["transaction_batch_id"] == "BATCH99"
        assert row["transaction_type"] == "CG"
        assert row["transaction_code_id"] == "99213"
        assert row["transaction_code_text"] == "Office Visit"
        assert row["transaction_description"] == "Office visit"
