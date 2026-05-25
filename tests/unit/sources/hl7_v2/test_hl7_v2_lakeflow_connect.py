"""Tests for the HL7 v2 community connector (GCP Healthcare API).

Runs the standard LakeflowConnectTests suite against the simulator by
default (offline, deterministic, no creds required). To exercise a real
GCP HL7v2 store, set ``CONNECTOR_TEST_MODE=live`` and provide credentials
via ``CONNECTOR_TEST_CONFIG_JSON`` or ``CONNECTOR_TEST_CONFIG_PATH``.
"""

import json

from databricks.labs.community_connector.sources.hl7_v2.hl7_v2 import HL7V2LakeflowConnect
from tests.unit.sources.hl7_v2._hl7v2_null_cols import allow_null_columns as _HL7V2_NULL_COLS
from tests.unit.sources.test_suite import LakeflowConnectTests


def _generate_pem_key() -> str:
    """Build a real RSA key in PKCS#8 PEM form.

    The connector calls ``google.oauth2.service_account.Credentials.
    from_service_account_info`` in its ``__init__``, which deserializes
    the PEM eagerly. The simulator intercepts every outbound HTTP call,
    so the key is never used for actual signing — but it must parse.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return pem.decode("ascii")


class TestHL7V2Connector(LakeflowConnectTests):
    connector_class = HL7V2LakeflowConnect
    simulator_source = "hl7_v2"
    sample_records = 5
    allow_null_columns = _HL7V2_NULL_COLS

    @classmethod
    def _replay_config(cls):
        return {
            "source_type": "gcp",
            "project_id": "sim-project",
            "location": "us-central1",
            "dataset_id": "sim-dataset",
            "hl7v2_store_id": "sim-store",
            "service_account_json": json.dumps({
                "type": "service_account",
                "project_id": "sim-project",
                "private_key_id": "sim-key-id",
                "private_key": _generate_pem_key(),
                "client_email": "sim@sim-project.iam.gserviceaccount.com",
                "client_id": "0",
                "token_uri": "https://oauth2.googleapis.com/token",
            }),
        }

    # ------------------------------------------------------------------
    # Z-segment / custom-segment coverage
    #
    # The base ``LakeflowConnectTests`` iterates ``connector.list_tables()``
    # which only exposes the typed segment names (MSH, PID, OBX, ...).
    # Custom Z-segments are opted into per-table via the ``segment_type``
    # table option (see the connector README), so they are not reached by
    # any of the auto-generated read / schema / metadata checks. These
    # tests fill that gap by driving the full connector path (schema →
    # metadata → read) for a Z-segment table backed by the simulator
    # corpus message ``sample_adt_zsegments`` (which carries two ZPI
    # segments and one ZIN segment).
    # ------------------------------------------------------------------

    _Z_TABLE = "custom_zpi"
    _Z_OPTS = {"segment_type": "ZPI", "window_seconds": "31536000"}

    def test_z_segment_schema_falls_back_to_generic(self):
        """A Z-segment table resolves to the 25-field generic schema."""
        schema = self.connector.get_table_schema(self._Z_TABLE, self._Z_OPTS)
        names = schema.fieldNames()
        assert "segment_type" in names, (
            "GENERIC_SEGMENT_SCHEMA must declare the segment_type column so "
            "downstream consumers can distinguish multiplexed Z-segments."
        )
        for i in range(1, 26):
            assert f"field_{i}" in names, f"missing field_{i} in generic schema"
        for meta_col in ("message_id", "message_timestamp", "create_time", "raw_segment"):
            assert meta_col in names, f"missing metadata column {meta_col!r}"

    def test_z_segment_metadata_uses_message_id_only(self):
        """Unknown / Z-segments fall back to ``primary_keys=['message_id']``.

        The composite ``(message_id, set_id)`` PK is only used for typed
        multi-segment tables — the generic schema doesn't declare set_id,
        so the connector intentionally narrows the PK here.
        """
        meta = self.connector.read_table_metadata(self._Z_TABLE, self._Z_OPTS)
        assert meta["ingestion_type"] == "append"
        assert meta["cursor_field"] == "create_time"
        assert meta["primary_keys"] == ["message_id"]

    def test_z_segment_read_extracts_rows_from_corpus(self):
        """End-to-end: read_table for a Z-segment yields generic-schema rows.

        Exercises the full pipeline: simulator → list/get message →
        base64-decode → parse HL7 → fallback to ``_extract_generic`` for
        an unknown segment → propagate metadata / raw_segment / segment_type.
        """
        iterator, offset = self.connector.read_table(
            self._Z_TABLE, {}, self._Z_OPTS
        )
        rows = list(iterator)
        assert rows, (
            "Expected at least one ZPI row from the simulator corpus "
            "(sample_adt_zsegments carries two ZPI segments)."
        )
        assert isinstance(offset, dict) and offset.get("cursor"), (
            "read_table must advance the cursor for a Z-segment table just "
            "like any other incremental table."
        )

        for row in rows:
            assert row["segment_type"] == "ZPI", (
                "Generic extractor must stamp the segment_type column with "
                "the upper-cased segment identifier."
            )
            assert row["message_id"], "metadata.message_id must be populated"
            assert row["raw_segment"].startswith("ZPI|"), (
                "raw_segment must carry the original pipe-delimited line for "
                "lossless recovery."
            )
            # field_1 is the set ID baked into the segment payload; field_2..
            # carry the custom Z-segment columns. Verify both shapes work.
            assert row["field_1"] in {"1", "2"}, (
                f"Unexpected ZPI.field_1 value: {row['field_1']!r}"
            )

        # Multi-segment fan-out: the corpus message has two ZPI segments,
        # so a single source message must produce two rows.
        by_msg: dict[str, list] = {}
        for r in rows:
            by_msg.setdefault(r["message_id"], []).append(r)
        assert any(len(rs) >= 2 for rs in by_msg.values()), (
            "Expected at least one message to fan out into multiple ZPI rows "
            f"but got: {[(k, len(v)) for k, v in by_msg.items()]}"
        )

    def test_z_segment_distinct_custom_segment(self):
        """A second Z-segment type (ZIN) on the same corpus reads independently.

        Confirms the ``segment_type`` table-option is the only thing that
        controls which segments are emitted — switching it from ZPI to ZIN
        on the same connector instance yields a different (non-overlapping)
        row set.
        """
        zin_opts = {"segment_type": "ZIN", "window_seconds": "31536000"}
        iterator, _ = self.connector.read_table("custom_zin", {}, zin_opts)
        zin_rows = list(iterator)
        assert zin_rows, "Expected at least one ZIN row from the corpus"
        for row in zin_rows:
            assert row["segment_type"] == "ZIN"
            assert row["raw_segment"].startswith("ZIN|")
            assert row["field_2"] == "PRIMARY"
            assert row["field_3"] == "MEMBER_Z001"
