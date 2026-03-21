"""HL7 v2 community connector — ingests HL7 message files from a volume path.

Files (.hl7 / .txt) are read from a Databricks Unity Catalog Volume path.
Each HL7 segment type becomes its own table (msh, pid, pv1, obr, obx, …).

Incremental cursor: file modification timestamp (epoch seconds).
Every micro-batch picks up only files modified after the last cursor value.
"""

from __future__ import annotations

import re
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from pyspark.sql.types import StructType

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.hl7.hl7_parser import (
    HL7Message,
    HL7Segment,
    parse_message,
)
from databricks.labs.community_connector.sources.hl7.hl7_schemas import (
    SEGMENT_TABLES,
    TABLE_DESCRIPTIONS,
    get_schema,
)

_DEFAULT_FILE_PATTERN = "*.hl7"
_DEFAULT_MAX_RECORDS = 10_000

# Segment types that appear at most once per message; PK = message_id alone.
# All other known segment types (obx, obr, al1, dg1, nk1) repeat and require
# (message_id, set_id) as a composite PK.
_SINGLE_SEGMENT_TABLES = frozenset({"msh", "evn", "pid", "pv1"})


# ---------------------------------------------------------------------------
# Null-safe helpers
# ---------------------------------------------------------------------------


def _v(s: str) -> str | None:
    """Return *s* if non-empty, else None."""
    return s if s else None


def _i(s: str) -> int | None:
    """Parse *s* as int; return None on failure."""
    if not s:
        return None
    try:
        return int(s.strip())
    except ValueError:
        return None


_DTM_RE = re.compile(
    r"^(\d{4})(\d{2})?(\d{2})?(\d{2})?(\d{2})?(\d{2})?(?:\.\d+)?([+-]\d{4})?$"
)


def _parse_dtm(s: str) -> datetime | None:
    """Parse an HL7 DTM string to a Python datetime.

    Handles partial precision (YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHHMMSS)
    and optional timezone offset (e.g. +0500, -0800).
    Returns None for empty or unparseable input.
    """
    if not s:
        return None
    m = _DTM_RE.match(s.strip())
    if not m:
        return None
    y, mo, d, h, mi, sec, tz = m.groups()
    try:
        dt = datetime(
            int(y),
            int(mo or 1),
            int(d or 1),
            int(h or 0),
            int(mi or 0),
            int(sec or 0),
        )
        if tz:
            sign = 1 if tz[0] == "+" else -1
            offset = timedelta(hours=int(tz[1:3]), minutes=int(tz[3:5]))
            dt = dt.replace(tzinfo=timezone(sign * offset))
        return dt
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Metadata builder (from MSH segment, added to every row)
# ---------------------------------------------------------------------------


def _metadata(msh: HL7Segment | None, source_file: str) -> dict:
    if msh is None:
        return {
            "message_id": None,
            "message_timestamp": None,
            "hl7_version": None,
            "source_file": source_file,
        }
    return {
        "message_id": _v(msh.get_field(10)),
        "message_timestamp": _v(msh.get_field(7)),
        "hl7_version": _v(msh.get_field(12)),
        "source_file": source_file,
    }


# ---------------------------------------------------------------------------
# Per-segment field extractors
# Note: after the MSH index fix in the parser, get_field(N) == MSH-N for all N.
# ---------------------------------------------------------------------------


def _extract_msh(seg: HL7Segment) -> dict:
    return {
        "field_separator": _v(seg.get_field(1)),               # MSH-1  "|"
        "encoding_characters": _v(seg.get_field(2)),           # MSH-2  "^~\&"
        "sending_application": _v(seg.get_component(3, 1)),    # MSH-3.1
        "sending_facility": _v(seg.get_component(4, 1)),       # MSH-4.1
        "receiving_application": _v(seg.get_component(5, 1)),  # MSH-5.1
        "receiving_facility": _v(seg.get_component(6, 1)),     # MSH-6.1
        "message_datetime": _parse_dtm(seg.get_field(7)),         # MSH-7
        "security": _v(seg.get_field(8)),                      # MSH-8
        "message_code": _v(seg.get_component(9, 1)),           # MSH-9.1 e.g. "ADT"
        "trigger_event": _v(seg.get_component(9, 2)),          # MSH-9.2 e.g. "A01"
        "message_structure": _v(seg.get_component(9, 3)),      # MSH-9.3
        "message_control_id": _v(seg.get_field(10)),           # MSH-10
        "processing_id": _v(seg.get_component(11, 1)),         # MSH-11.1
        "version_id": _v(seg.get_field(12)),                   # MSH-12
        "sequence_number": _i(seg.get_field(13)),              # MSH-13
        "continuation_pointer": _v(seg.get_field(14)),         # MSH-14
        "accept_acknowledgment_type": _v(seg.get_field(15)),   # MSH-15
        "application_acknowledgment_type": _v(seg.get_field(16)),  # MSH-16
        "country_code": _v(seg.get_field(17)),                 # MSH-17
        "character_set": _v(seg.get_field(18)),                # MSH-18
        "principal_language": _v(seg.get_component(19, 1)),    # MSH-19.1
        "alt_character_set_handling": _v(seg.get_field(20)),   # MSH-20
        "message_profile_identifier": _v(seg.get_component(21, 1)),    # MSH-21.1
        "sending_responsible_org": _v(seg.get_component(22, 1)),       # MSH-22.1 v2.7+
        "receiving_responsible_org": _v(seg.get_component(23, 1)),     # MSH-23.1 v2.7+
        "sending_network_address": _v(seg.get_component(24, 1)),       # MSH-24.1 v2.7+
        "receiving_network_address": _v(seg.get_component(25, 1)),     # MSH-25.1 v2.7+
    }


def _extract_pid(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)),                                  # PID-1
        "patient_id": _v(seg.get_field(2)),                              # PID-2
        "patient_identifier_list": _v(seg.get_first_repetition(3)),      # PID-3 raw
        "patient_id_value": _v(seg.get_rep_component(3, 1, 1)),          # PID-3.1
        "patient_id_check_digit": _v(seg.get_rep_component(3, 1, 2)),    # PID-3.2
        "patient_id_check_digit_scheme": _v(seg.get_rep_component(3, 1, 3)),  # PID-3.3
        "patient_id_assigning_authority": _v(seg.get_rep_component(3, 1, 4)), # PID-3.4
        "patient_id_type_code": _v(seg.get_rep_component(3, 1, 5)),      # PID-3.5
        "alternate_patient_id": _v(seg.get_first_repetition(4)),         # PID-4
        "patient_name": _v(seg.get_first_repetition(5)),                 # PID-5 raw
        "patient_family_name": _v(seg.get_rep_component(5, 1, 1)),       # PID-5.1
        "patient_given_name": _v(seg.get_rep_component(5, 1, 2)),        # PID-5.2
        "patient_middle_name": _v(seg.get_rep_component(5, 1, 3)),       # PID-5.3
        "patient_name_suffix": _v(seg.get_rep_component(5, 1, 5)),       # PID-5.5
        "patient_name_prefix": _v(seg.get_rep_component(5, 1, 6)),       # PID-5.6
        "mothers_maiden_name": _v(seg.get_rep_component(6, 1, 1)),       # PID-6.1
        "date_of_birth": _parse_dtm(seg.get_field(7)),                    # PID-7
        "administrative_sex": _v(seg.get_field(8)),                      # PID-8
        "patient_alias": _v(seg.get_first_repetition(9)),                # PID-9
        "race": _v(seg.get_rep_component(10, 1, 1)),                     # PID-10.1
        "patient_address": _v(seg.get_first_repetition(11)),             # PID-11 raw
        "address_street": _v(seg.get_rep_component(11, 1, 1)),           # PID-11.1
        "address_other_designation": _v(seg.get_rep_component(11, 1, 2)),# PID-11.2
        "address_city": _v(seg.get_rep_component(11, 1, 3)),             # PID-11.3
        "address_state": _v(seg.get_rep_component(11, 1, 4)),            # PID-11.4
        "address_zip": _v(seg.get_rep_component(11, 1, 5)),              # PID-11.5
        "address_country": _v(seg.get_rep_component(11, 1, 6)),          # PID-11.6
        "address_type": _v(seg.get_rep_component(11, 1, 7)),             # PID-11.7
        "county_code": _v(seg.get_field(12)),                            # PID-12
        "home_phone": _v(seg.get_first_repetition(13)),                  # PID-13
        "business_phone": _v(seg.get_first_repetition(14)),              # PID-14
        "primary_language": _v(seg.get_component(15, 1)),                # PID-15.1
        "marital_status": _v(seg.get_component(16, 1)),                  # PID-16.1
        "religion": _v(seg.get_component(17, 1)),                        # PID-17.1
        "patient_account_number": _v(seg.get_component(18, 1)),          # PID-18.1
        "ssn": _v(seg.get_field(19)),                                    # PID-19
        "drivers_license": _v(seg.get_field(20)),                        # PID-20
        "mothers_identifier": _v(seg.get_rep_component(21, 1, 1)),       # PID-21.1
        "ethnic_group": _v(seg.get_rep_component(22, 1, 1)),             # PID-22.1
        "birth_place": _v(seg.get_field(23)),                            # PID-23
        "multiple_birth_indicator": _v(seg.get_field(24)),               # PID-24
        "birth_order": _i(seg.get_field(25)),                            # PID-25
        "citizenship": _v(seg.get_rep_component(26, 1, 1)),              # PID-26.1
        "veterans_military_status": _v(seg.get_component(27, 1)),        # PID-27.1
        "nationality": _v(seg.get_component(28, 1)),                     # PID-28.1
        "patient_death_datetime": _parse_dtm(seg.get_field(29)),          # PID-29
        "patient_death_indicator": _v(seg.get_field(30)),                # PID-30
        "identity_unknown_indicator": _v(seg.get_field(31)),             # PID-31
        "identity_reliability_code": _v(seg.get_first_repetition(32)),  # PID-32
        "last_update_datetime": _parse_dtm(seg.get_field(33)),            # PID-33
        "last_update_facility": _v(seg.get_component(34, 1)),            # PID-34.1
        "species_code": _v(seg.get_component(35, 1)),                    # PID-35.1
        "breed_code": _v(seg.get_component(36, 1)),                      # PID-36.1
        "strain": _v(seg.get_field(37)),                                 # PID-37
        "production_class_code": _v(seg.get_component(38, 1)),           # PID-38.1
        "tribal_citizenship": _v(seg.get_component(39, 1)),              # PID-39.1
        "patient_telecommunication": _v(seg.get_first_repetition(40)),   # PID-40
    }


def _extract_pv1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)),                                  # PV1-1
        "patient_class": _v(seg.get_field(2)),                           # PV1-2
        "assigned_patient_location": _v(seg.get_field(3)),               # PV1-3 raw
        "location_point_of_care": _v(seg.get_component(3, 1)),           # PV1-3.1
        "location_room": _v(seg.get_component(3, 2)),                    # PV1-3.2
        "location_bed": _v(seg.get_component(3, 3)),                     # PV1-3.3
        "location_facility": _v(seg.get_component(3, 4)),                # PV1-3.4
        "location_status": _v(seg.get_component(3, 5)),                  # PV1-3.5
        "location_type": _v(seg.get_component(3, 9)),                    # PV1-3.9
        "admission_type": _v(seg.get_field(4)),                          # PV1-4
        "preadmit_number": _v(seg.get_component(5, 1)),                  # PV1-5.1
        "prior_patient_location": _v(seg.get_field(6)),                  # PV1-6 raw
        "attending_doctor": _v(seg.get_first_repetition(7)),             # PV1-7 raw
        "attending_doctor_id": _v(seg.get_rep_component(7, 1, 1)),       # PV1-7.1
        "attending_doctor_family_name": _v(seg.get_rep_component(7, 1, 2)),  # PV1-7.2
        "attending_doctor_given_name": _v(seg.get_rep_component(7, 1, 3)),   # PV1-7.3
        "attending_doctor_prefix": _v(seg.get_rep_component(7, 1, 6)),   # PV1-7.6
        "referring_doctor": _v(seg.get_first_repetition(8)),             # PV1-8 raw
        "referring_doctor_id": _v(seg.get_rep_component(8, 1, 1)),       # PV1-8.1
        "referring_doctor_family_name": _v(seg.get_rep_component(8, 1, 2)),  # PV1-8.2
        "referring_doctor_given_name": _v(seg.get_rep_component(8, 1, 3)),   # PV1-8.3
        "consulting_doctor": _v(seg.get_first_repetition(9)),            # PV1-9 raw
        "hospital_service": _v(seg.get_field(10)),                       # PV1-10
        "temporary_location": _v(seg.get_field(11)),                     # PV1-11
        "preadmit_test_indicator": _v(seg.get_field(12)),                # PV1-12
        "readmission_indicator": _v(seg.get_field(13)),                  # PV1-13
        "admit_source": _v(seg.get_field(14)),                           # PV1-14
        "ambulatory_status": _v(seg.get_first_repetition(15)),           # PV1-15
        "vip_indicator": _v(seg.get_field(16)),                          # PV1-16
        "admitting_doctor": _v(seg.get_first_repetition(17)),            # PV1-17 raw
        "admitting_doctor_id": _v(seg.get_rep_component(17, 1, 1)),      # PV1-17.1
        "admitting_doctor_family_name": _v(seg.get_rep_component(17, 1, 2)), # PV1-17.2
        "admitting_doctor_given_name": _v(seg.get_rep_component(17, 1, 3)),  # PV1-17.3
        "patient_type": _v(seg.get_field(18)),                           # PV1-18
        "visit_number": _v(seg.get_component(19, 1)),                    # PV1-19.1
        "financial_class": _v(seg.get_rep_component(20, 1, 1)),          # PV1-20.1
        "charge_price_indicator": _v(seg.get_field(21)),                 # PV1-21
        "courtesy_code": _v(seg.get_field(22)),                          # PV1-22
        "credit_rating": _v(seg.get_field(23)),                          # PV1-23
        "contract_code": _v(seg.get_first_repetition(24)),               # PV1-24
        "contract_effective_date": _v(seg.get_first_repetition(25)),     # PV1-25
        "contract_amount": _v(seg.get_first_repetition(26)),             # PV1-26
        "contract_period": _v(seg.get_first_repetition(27)),             # PV1-27
        "interest_code": _v(seg.get_field(28)),                          # PV1-28
        "transfer_to_bad_debt_code": _v(seg.get_field(29)),              # PV1-29
        "transfer_to_bad_debt_date": _v(seg.get_field(30)),              # PV1-30
        "bad_debt_agency_code": _v(seg.get_field(31)),                   # PV1-31
        "bad_debt_transfer_amount": _v(seg.get_field(32)),               # PV1-32
        "bad_debt_recovery_amount": _v(seg.get_field(33)),               # PV1-33
        "delete_account_indicator": _v(seg.get_field(34)),               # PV1-34
        "delete_account_date": _v(seg.get_field(35)),                    # PV1-35
        "discharge_disposition": _v(seg.get_field(36)),                  # PV1-36
        "discharged_to_location": _v(seg.get_component(37, 1)),          # PV1-37.1
        "diet_type": _v(seg.get_component(38, 1)),                       # PV1-38.1
        "servicing_facility": _v(seg.get_field(39)),                     # PV1-39
        "bed_status": _v(seg.get_field(40)),                             # PV1-40
        "account_status": _v(seg.get_field(41)),                         # PV1-41
        "pending_location": _v(seg.get_field(42)),                       # PV1-42
        "prior_temporary_location": _v(seg.get_field(43)),               # PV1-43
        "admit_datetime": _parse_dtm(seg.get_first_repetition(44)),       # PV1-44
        "discharge_datetime": _parse_dtm(seg.get_first_repetition(45)), # PV1-45
        "current_patient_balance": _v(seg.get_field(46)),                # PV1-46
        "total_charges": _v(seg.get_field(47)),                          # PV1-47
        "total_adjustments": _v(seg.get_field(48)),                      # PV1-48
        "total_payments": _v(seg.get_field(49)),                         # PV1-49
        "alternate_visit_id": _v(seg.get_component(50, 1)),              # PV1-50.1
        "visit_indicator": _v(seg.get_field(51)),                        # PV1-51
        "other_healthcare_provider": _v(seg.get_first_repetition(52)),   # PV1-52
    }


def _extract_obr(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,                             # OBR-1 (NOT NULL PK; default 1)
        "placer_order_number": _v(seg.get_component(2, 1)),              # OBR-2.1
        "filler_order_number": _v(seg.get_component(3, 1)),              # OBR-3.1
        "universal_service_identifier": _v(seg.get_field(4)),            # OBR-4 raw
        "service_id": _v(seg.get_component(4, 1)),                       # OBR-4.1
        "service_text": _v(seg.get_component(4, 2)),                     # OBR-4.2
        "service_coding_system": _v(seg.get_component(4, 3)),            # OBR-4.3
        "priority": _v(seg.get_field(5)),                                # OBR-5
        "requested_datetime": _parse_dtm(seg.get_field(6)),               # OBR-6
        "observation_datetime": _parse_dtm(seg.get_field(7)),            # OBR-7
        "observation_end_datetime": _parse_dtm(seg.get_field(8)),        # OBR-8
        "collection_volume": _v(seg.get_component(9, 1)),                # OBR-9.1
        "collector_identifier": _v(seg.get_rep_component(10, 1, 1)),     # OBR-10.1
        "specimen_action_code": _v(seg.get_field(11)),                   # OBR-11
        "danger_code": _v(seg.get_component(12, 1)),                     # OBR-12.1
        "relevant_clinical_information": _v(seg.get_field(13)),          # OBR-13
        "specimen_received_datetime": _parse_dtm(seg.get_field(14)),      # OBR-14
        "specimen_source": _v(seg.get_field(15)),                        # OBR-15
        "ordering_provider": _v(seg.get_first_repetition(16)),           # OBR-16 raw
        "ordering_provider_id": _v(seg.get_rep_component(16, 1, 1)),     # OBR-16.1
        "ordering_provider_family_name": _v(seg.get_rep_component(16, 1, 2)),  # OBR-16.2
        "ordering_provider_given_name": _v(seg.get_rep_component(16, 1, 3)),   # OBR-16.3
        "order_callback_phone": _v(seg.get_first_repetition(17)),        # OBR-17
        "placer_field_1": _v(seg.get_field(18)),                         # OBR-18
        "placer_field_2": _v(seg.get_field(19)),                         # OBR-19
        "filler_field_1": _v(seg.get_field(20)),                         # OBR-20
        "filler_field_2": _v(seg.get_field(21)),                         # OBR-21
        "results_rpt_status_chng_datetime": _parse_dtm(seg.get_field(22)), # OBR-22
        "charge_to_practice": _v(seg.get_field(23)),                     # OBR-23
        "diagnostic_service_section_id": _v(seg.get_field(24)),          # OBR-24
        "result_status": _v(seg.get_field(25)),                          # OBR-25
        "parent_result": _v(seg.get_field(26)),                          # OBR-26
        "quantity_timing": _v(seg.get_first_repetition(27)),             # OBR-27
        "result_copies_to": _v(seg.get_rep_component(28, 1, 1)),         # OBR-28.1
        "parent_placer_order_number": _v(seg.get_component(29, 1)),      # OBR-29.1
        "transportation_mode": _v(seg.get_field(30)),                    # OBR-30
        "reason_for_study": _v(seg.get_rep_component(31, 1, 1)),         # OBR-31.1
        "principal_result_interpreter": _v(seg.get_component(32, 1)),    # OBR-32.1
        "assistant_result_interpreter": _v(seg.get_rep_component(33, 1, 1)),  # OBR-33.1
        "technician": _v(seg.get_rep_component(34, 1, 1)),               # OBR-34.1
        "transcriptionist": _v(seg.get_rep_component(35, 1, 1)),         # OBR-35.1
        "scheduled_datetime": _parse_dtm(seg.get_field(36)),              # OBR-36
        "number_of_sample_containers": _i(seg.get_field(37)),            # OBR-37
        "transport_logistics": _v(seg.get_rep_component(38, 1, 1)),      # OBR-38.1
        "collectors_comment": _v(seg.get_rep_component(39, 1, 1)),       # OBR-39.1
        "transport_arrangement_responsibility": _v(seg.get_component(40, 1)),  # OBR-40.1
        "transport_arranged": _v(seg.get_field(41)),                     # OBR-41
        "escort_required": _v(seg.get_field(42)),                        # OBR-42
        "planned_patient_transport_comment": _v(seg.get_rep_component(43, 1, 1)),  # OBR-43.1
        "procedure_code": _v(seg.get_component(44, 1)),                  # OBR-44.1
        "procedure_code_modifier": _v(seg.get_rep_component(45, 1, 1)),  # OBR-45.1
        "placer_supplemental_service_info": _v(seg.get_rep_component(46, 1, 1)),   # OBR-46.1
        "filler_supplemental_service_info": _v(seg.get_rep_component(47, 1, 1)),   # OBR-47.1
        "medically_necessary_dup_proc_reason": _v(seg.get_component(48, 1)),  # OBR-48.1
        "result_handling": _v(seg.get_component(49, 1)),                 # OBR-49.1
        "parent_universal_service_id": _v(seg.get_component(50, 1)),     # OBR-50.1
    }


def _extract_obx(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,                             # OBX-1 (NOT NULL PK; default 1)
        "value_type": _v(seg.get_field(2)),                              # OBX-2 (NM/ST/TX/CWE…)
        "observation_identifier": _v(seg.get_field(3)),                  # OBX-3 raw
        "observation_id": _v(seg.get_component(3, 1)),                   # OBX-3.1 (LOINC)
        "observation_text": _v(seg.get_component(3, 2)),                 # OBX-3.2
        "observation_coding_system": _v(seg.get_component(3, 3)),        # OBX-3.3
        "observation_alt_id": _v(seg.get_component(3, 4)),               # OBX-3.4
        "observation_alt_text": _v(seg.get_component(3, 5)),             # OBX-3.5
        "observation_alt_coding_system": _v(seg.get_component(3, 6)),    # OBX-3.6
        "observation_sub_id": _v(seg.get_field(4)),                      # OBX-4
        "observation_value": _v(seg.get_first_repetition(5)),            # OBX-5 (varies)
        "units": _v(seg.get_field(6)),                                   # OBX-6 raw
        "units_code": _v(seg.get_component(6, 1)),                       # OBX-6.1
        "units_text": _v(seg.get_component(6, 2)),                       # OBX-6.2
        "units_coding_system": _v(seg.get_component(6, 3)),              # OBX-6.3
        "references_range": _v(seg.get_field(7)),                        # OBX-7
        "interpretation_codes": _v(seg.get_first_repetition(8)),         # OBX-8
        "probability": _v(seg.get_field(9)),                             # OBX-9
        "nature_of_abnormal_test": _v(seg.get_first_repetition(10)),     # OBX-10
        "observation_result_status": _v(seg.get_field(11)),              # OBX-11 (F/P/C…)
        "effective_date_of_ref_range": _parse_dtm(seg.get_field(12)),     # OBX-12
        "user_defined_access_checks": _v(seg.get_field(13)),             # OBX-13
        "datetime_of_observation": _parse_dtm(seg.get_field(14)),         # OBX-14
        "producers_id": _v(seg.get_component(15, 1)),                    # OBX-15.1
        "responsible_observer": _v(seg.get_rep_component(16, 1, 1)),     # OBX-16.1
        "observation_method": _v(seg.get_rep_component(17, 1, 1)),       # OBX-17.1
        "equipment_instance_identifier": _v(seg.get_rep_component(18, 1, 1)),  # OBX-18.1
        "datetime_of_analysis": _parse_dtm(seg.get_field(19)),            # OBX-19
        "observation_site": _v(seg.get_rep_component(20, 1, 1)),         # OBX-20.1
        "observation_instance_identifier": _v(seg.get_component(21, 1)), # OBX-21.1
        "mood_code": _v(seg.get_component(22, 1)),                       # OBX-22.1
        "performing_organization_name": _v(seg.get_component(23, 1)),    # OBX-23.1
        "performing_organization_address": _v(seg.get_field(24)),        # OBX-24
        "performing_org_medical_director": _v(seg.get_rep_component(25, 1, 1)),  # OBX-25.1
        "patient_results_release_category": _v(seg.get_field(26)),       # OBX-26
        "root_cause": _v(seg.get_component(27, 1)),                      # OBX-27.1
        "local_process_control": _v(seg.get_rep_component(28, 1, 1)),    # OBX-28.1
    }


def _extract_al1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,                             # AL1-1 (NOT NULL PK; default 1)
        "allergen_type_code": _v(seg.get_component(2, 1)),               # AL1-2.1
        "allergen_code": _v(seg.get_field(3)),                           # AL1-3 raw
        "allergen_id": _v(seg.get_component(3, 1)),                      # AL1-3.1
        "allergen_text": _v(seg.get_component(3, 2)),                    # AL1-3.2
        "allergen_coding_system": _v(seg.get_component(3, 3)),           # AL1-3.3
        "allergy_severity_code": _v(seg.get_component(4, 1)),            # AL1-4.1
        "allergy_reaction_code": _v(seg.get_first_repetition(5)),        # AL1-5
        "identification_date": _parse_dtm(seg.get_field(6)),              # AL1-6
    }


def _extract_dg1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,                             # DG1-1 (NOT NULL PK; default 1)
        "diagnosis_coding_method": _v(seg.get_field(2)),                 # DG1-2
        "diagnosis_code": _v(seg.get_field(3)),                          # DG1-3 raw
        "diagnosis_id": _v(seg.get_component(3, 1)),                     # DG1-3.1
        "diagnosis_text": _v(seg.get_component(3, 2)),                   # DG1-3.2
        "diagnosis_coding_system": _v(seg.get_component(3, 3)),          # DG1-3.3
        "diagnosis_description": _v(seg.get_field(4)),                   # DG1-4
        "diagnosis_datetime": _parse_dtm(seg.get_field(5)),               # DG1-5
        "diagnosis_type": _v(seg.get_field(6)),                          # DG1-6
        "major_diagnostic_category": _v(seg.get_component(7, 1)),        # DG1-7.1
        "diagnostic_related_group": _v(seg.get_component(8, 1)),         # DG1-8.1
        "drg_approval_indicator": _v(seg.get_field(9)),                  # DG1-9
        "drg_grouper_review_code": _v(seg.get_field(10)),                # DG1-10
        "outlier_type": _v(seg.get_component(11, 1)),                    # DG1-11.1
        "outlier_days": _i(seg.get_field(12)),                           # DG1-12
        "outlier_cost": _v(seg.get_field(13)),                           # DG1-13
        "grouper_version_and_type": _v(seg.get_field(14)),               # DG1-14
        "diagnosis_priority": _i(seg.get_field(15)),                     # DG1-15
        "diagnosing_clinician": _v(seg.get_rep_component(16, 1, 1)),     # DG1-16.1
        "diagnosis_classification": _v(seg.get_field(17)),               # DG1-17
        "confidential_indicator": _v(seg.get_field(18)),                 # DG1-18
        "attestation_datetime": _parse_dtm(seg.get_field(19)),            # DG1-19
        "diagnosis_identifier": _v(seg.get_component(20, 1)),            # DG1-20.1
        "diagnosis_action_code": _v(seg.get_field(21)),                  # DG1-21
        "parent_diagnosis": _v(seg.get_component(22, 1)),                # DG1-22.1
        "drg_ccl_value_code": _v(seg.get_component(23, 1)),              # DG1-23.1
        "drg_grouping_usage": _v(seg.get_field(24)),                     # DG1-24
        "drg_diagnosis_determination_status": _v(seg.get_field(25)),     # DG1-25
        "present_on_admission_indicator": _v(seg.get_field(26)),         # DG1-26
    }


def _extract_nk1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,                             # NK1-1 (NOT NULL PK; default 1)
        "name": _v(seg.get_first_repetition(2)),                         # NK1-2 raw
        "nk_family_name": _v(seg.get_rep_component(2, 1, 1)),            # NK1-2.1
        "nk_given_name": _v(seg.get_rep_component(2, 1, 2)),             # NK1-2.2
        "nk_middle_name": _v(seg.get_rep_component(2, 1, 3)),            # NK1-2.3
        "relationship": _v(seg.get_field(3)),                            # NK1-3 raw
        "relationship_code": _v(seg.get_component(3, 1)),                # NK1-3.1
        "relationship_text": _v(seg.get_component(3, 2)),                # NK1-3.2
        "address": _v(seg.get_first_repetition(4)),                      # NK1-4 raw
        "phone_number": _v(seg.get_first_repetition(5)),                 # NK1-5
        "business_phone": _v(seg.get_first_repetition(6)),               # NK1-6
        "contact_role": _v(seg.get_component(7, 1)),                     # NK1-7.1
        "start_date": _parse_dtm(seg.get_field(8)),                       # NK1-8
        "end_date": _parse_dtm(seg.get_field(9)),                        # NK1-9
        "job_title": _v(seg.get_field(10)),                              # NK1-10
        "job_code": _v(seg.get_component(11, 1)),                        # NK1-11.1
        "employee_number": _v(seg.get_component(12, 1)),                 # NK1-12.1
        "organization_name": _v(seg.get_rep_component(13, 1, 1)),        # NK1-13.1
        "marital_status": _v(seg.get_component(14, 1)),                  # NK1-14.1
        "administrative_sex": _v(seg.get_field(15)),                     # NK1-15
        "date_of_birth": _parse_dtm(seg.get_field(16)),                   # NK1-16
        "living_dependency": _v(seg.get_rep_component(17, 1, 1)),        # NK1-17.1
        "ambulatory_status": _v(seg.get_rep_component(18, 1, 1)),        # NK1-18.1
        "citizenship": _v(seg.get_rep_component(19, 1, 1)),              # NK1-19.1
        "primary_language": _v(seg.get_component(20, 1)),                # NK1-20.1
        "living_arrangement": _v(seg.get_field(21)),                     # NK1-21
        "publicity_code": _v(seg.get_component(22, 1)),                  # NK1-22.1
        "protection_indicator": _v(seg.get_field(23)),                   # NK1-23
        "student_indicator": _v(seg.get_field(24)),                      # NK1-24
        "religion": _v(seg.get_component(25, 1)),                        # NK1-25.1
        "mothers_maiden_name": _v(seg.get_rep_component(26, 1, 1)),      # NK1-26.1
        "nationality": _v(seg.get_component(27, 1)),                     # NK1-27.1
        "ethnic_group": _v(seg.get_rep_component(28, 1, 1)),             # NK1-28.1
        "contact_reason": _v(seg.get_rep_component(29, 1, 1)),           # NK1-29.1
        "contact_person_name": _v(seg.get_rep_component(30, 1, 1)),      # NK1-30.1
        "contact_person_telephone": _v(seg.get_first_repetition(31)),    # NK1-31
        "contact_persons_address": _v(seg.get_first_repetition(32)),     # NK1-32
        "associated_party_identifiers": _v(seg.get_rep_component(33, 1, 1)),  # NK1-33.1
        "job_status": _v(seg.get_field(34)),                             # NK1-34
        "race": _v(seg.get_rep_component(35, 1, 1)),                     # NK1-35.1
        "handicap": _v(seg.get_field(36)),                               # NK1-36
        "contact_ssn": _v(seg.get_field(37)),                            # NK1-37
        "nk_birth_place": _v(seg.get_field(38)),                         # NK1-38
        "vip_indicator": _v(seg.get_field(39)),                          # NK1-39
    }


def _extract_evn(seg: HL7Segment) -> dict:
    return {
        "event_type_code": _v(seg.get_field(1)),                         # EVN-1
        "recorded_datetime": _parse_dtm(seg.get_field(2)),                # EVN-2
        "date_time_planned_event": _parse_dtm(seg.get_field(3)),         # EVN-3
        "event_reason_code": _v(seg.get_component(4, 1)),                # EVN-4.1
        "operator_id": _v(seg.get_rep_component(5, 1, 1)),               # EVN-5.1
        "event_occurred": _parse_dtm(seg.get_field(6)),                   # EVN-6
        "event_facility": _v(seg.get_component(7, 1)),                   # EVN-7.1
    }


def _extract_generic(seg: HL7Segment) -> dict:
    """Fallback extractor for Z-segments and unknown segment types."""
    return {"segment_type": seg.segment_type} | {
        f"field_{i}": _v(seg.get_field(i)) for i in range(1, 26)
    }


_EXTRACTORS = {
    "msh": _extract_msh,
    "pid": _extract_pid,
    "pv1": _extract_pv1,
    "obr": _extract_obr,
    "obx": _extract_obx,
    "al1": _extract_al1,
    "dg1": _extract_dg1,
    "nk1": _extract_nk1,
    "evn": _extract_evn,
}


# ---------------------------------------------------------------------------
# Multi-message file splitter
# ---------------------------------------------------------------------------


def _split_messages(text: str) -> list[str]:
    """Split an HL7 batch file into individual message strings.

    Each message starts with an MSH line.  FHS/BHS/BTS/FTS batch-envelope
    segments are skipped.
    """
    normalised = text.strip().replace("\r\n", "\r").replace("\n", "\r")
    lines = normalised.split("\r")

    messages: list[str] = []
    current: list[str] = []
    _ENVELOPE = {"FHS", "BHS", "BTS", "FTS"}

    for line in lines:
        if not line.strip():
            continue
        seg_type = line[:3].upper()
        if seg_type in _ENVELOPE:
            continue
        if seg_type == "MSH":
            if current:
                messages.append("\r".join(current))
            current = [line]
        else:
            current.append(line)

    if current:
        messages.append("\r".join(current))

    return messages


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class HL7LakeflowConnect(LakeflowConnect):
    """LakeflowConnect implementation for HL7 v2 message files.

    Reads ``.hl7`` (or other pattern) files from a Unity Catalog Volume path.
    Each HL7 segment type is a separate table.  Incremental loading is driven
    by file modification timestamps.

    Required options:
        volume_path (str): Path to the directory containing .hl7 files,
            e.g. ``/Volumes/catalog/schema/vol/hl7_feed/``.

    Optional options:
        file_pattern (str): Glob pattern for file matching (default ``*.hl7``).
            Use ``**/*.hl7`` for recursive search.
        max_records_per_batch (str): Max records per micro-batch (default 10000).
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        if "volume_path" not in options:
            raise ValueError("'volume_path' is required in connector options.")
        self._volume_path = Path(options["volume_path"])
        self._file_pattern = options.get("file_pattern", _DEFAULT_FILE_PATTERN)
        self._max_records = int(options.get("max_records_per_batch", str(_DEFAULT_MAX_RECORDS)))

    # ------------------------------------------------------------------
    # LakeflowConnect interface
    # ------------------------------------------------------------------

    def list_tables(self) -> list[str]:
        return list(SEGMENT_TABLES)

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name)
        return get_schema(segment_type)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).lower()
        # Single-segment tables have one row per message → PK is message_id alone.
        # Repeating-segment tables (multiple OBX, OBR, etc.) need set_id too.
        is_single = segment_type in _SINGLE_SEGMENT_TABLES
        return {
            "ingestion_type": "cdc",
            "cursor_field": "message_timestamp",
            "primary_keys": ["message_id"] if is_single else ["message_id", "set_id"],
            "description": TABLE_DESCRIPTIONS.get(segment_type, ""),
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).upper()
        max_records = int(table_options.get("max_records_per_batch", str(self._max_records)))

        # Determine cursor: epoch-seconds float stored as string in offset dict.
        cursor: float | None = None
        if start_offset and "cursor" in start_offset:
            try:
                cursor = float(start_offset["cursor"])
            except (ValueError, TypeError):
                cursor = None

        files = self._list_files(cursor)
        if not files:
            return iter([]), start_offset or {"cursor": "0"}

        records: list[dict] = []
        max_mtime: float = cursor or 0.0

        for file_path, mtime in files:
            if len(records) >= max_records:
                break
            max_mtime = max(max_mtime, mtime)
            try:
                new_records = self._read_file(file_path, segment_type)
                records.extend(new_records)
            except Exception as exc:  # pylint: disable=broad-except
                warnings.warn(f"Skipping {file_path}: {exc}")

        if not records:
            return iter([]), {"cursor": str(max_mtime)}

        end_offset = {"cursor": str(max_mtime)}
        if start_offset and start_offset == end_offset:
            return iter([]), start_offset

        return iter(records), end_offset

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_table(self, table_name: str, table_options: dict) -> None:
        if table_name not in SEGMENT_TABLES and "segment_type" not in table_options:
            raise ValueError(
                f"Unknown table '{table_name}'. "
                f"Supported tables: {SEGMENT_TABLES}. "
                "For custom/Z-segments, provide 'segment_type' in table_options."
            )

    def _list_files(self, after_mtime: float | None) -> list[tuple[Path, float]]:
        """Return (path, mtime) pairs sorted ascending by mtime.

        Only files with mtime *strictly after* ``after_mtime`` are returned.
        When ``after_mtime`` is None, all matching files are returned.
        """
        try:
            matched = list(self._volume_path.glob(self._file_pattern))
        except (OSError, ValueError) as exc:
            warnings.warn(f"Cannot list files at {self._volume_path}: {exc}")
            return []

        result: list[tuple[Path, float]] = []
        for p in matched:
            if not p.is_file():
                continue
            try:
                mtime = p.stat().st_mtime
            except OSError:
                continue
            if after_mtime is None or mtime > after_mtime:
                result.append((p, mtime))

        result.sort(key=lambda x: x[1])
        return result

    def _read_file(self, file_path: Path, segment_type: str) -> list[dict]:
        """Parse one HL7 file and return records for *segment_type*."""
        text = file_path.read_text(encoding="utf-8", errors="replace")
        source_file = file_path.name
        records: list[dict] = []

        for msg_text in _split_messages(text):
            msg: HL7Message | None = parse_message(msg_text)
            if msg is None:
                continue
            msh = msg.get_segment("MSH")
            meta = _metadata(msh, source_file)

            if segment_type == "MSH":
                if msh is not None:
                    records.append(meta | _extract_msh(msh) | {"raw_segment": msh.raw_line})
            else:
                extractor = _EXTRACTORS.get(segment_type.lower(), _extract_generic)
                for seg in msg.get_segments(segment_type):
                    records.append(meta | extractor(seg) | {"raw_segment": seg.raw_line})

        return records
