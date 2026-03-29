"""HL7 v2 community connector — ingests HL7 messages from Google Cloud Healthcare API.

Messages are fetched from a GCP Healthcare API HL7v2 store via REST.  Each HL7
segment type becomes its own table (msh, pid, pv1, obr, obx, …).

Incremental cursor: ``sendTime`` from the API (RFC3339 timestamp).
The connector uses a sliding time-window strategy to bound each micro-batch.
"""

from __future__ import annotations

import base64
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

import requests
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import service_account as google_sa
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

_DEFAULT_MAX_RECORDS = 10_000
_DEFAULT_WINDOW_SECONDS = 86_400
_RETRIABLE_STATUS_CODES = (429, 500, 503)
_MAX_RETRIES = 3
_INITIAL_BACKOFF = 1
_REQUEST_TIMEOUT = 30
_MAX_PAGE_SIZE = 1000

_SINGLE_SEGMENT_TABLES = frozenset(
    {"msh", "evn", "pid", "pd1", "pv1", "pv2", "mrg", "sch", "txa"}
)


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


def _parse_dtm(s: str) -> str | None:
    """Parse an HL7 DTM string to an ISO-8601 UTC string.

    Handles partial precision (YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHHMMSS)
    and optional timezone offset (e.g. +0500, -0800).  If a timezone offset
    is present the value is converted to UTC first.  Returns an ISO-8601
    string (no timezone suffix) so the schema can use StringType and avoid
    Arrow timestamp-timezone mismatches.
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
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.isoformat()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Metadata builder (from MSH segment, added to every row)
# ---------------------------------------------------------------------------


def _metadata(msh: HL7Segment | None, source_file: str, send_time: str) -> dict:
    if msh is None:
        return {
            "message_id": None,
            "message_timestamp": None,
            "hl7_version": None,
            "source_file": source_file,
            "send_time": send_time,
        }
    return {
        "message_id": _v(msh.get_field(10)),
        "message_timestamp": _v(msh.get_field(7)),
        "hl7_version": _v(msh.get_field(12)),
        "source_file": source_file,
        "send_time": send_time,
    }


# ---------------------------------------------------------------------------
# Per-segment field extractors
# ---------------------------------------------------------------------------


def _extract_msh(seg: HL7Segment) -> dict:
    return {
        "field_separator": _v(seg.get_field(1)),
        "encoding_characters": _v(seg.get_field(2)),
        "sending_application": _v(seg.get_component(3, 1)),
        "sending_facility": _v(seg.get_component(4, 1)),
        "receiving_application": _v(seg.get_component(5, 1)),
        "receiving_facility": _v(seg.get_component(6, 1)),
        "message_datetime": _parse_dtm(seg.get_field(7)),
        "security": _v(seg.get_field(8)),
        "message_code": _v(seg.get_component(9, 1)),
        "trigger_event": _v(seg.get_component(9, 2)),
        "message_structure": _v(seg.get_component(9, 3)),
        "message_control_id": _v(seg.get_field(10)),
        "processing_id": _v(seg.get_component(11, 1)),
        "version_id": _v(seg.get_field(12)),
        "sequence_number": _i(seg.get_field(13)),
        "continuation_pointer": _v(seg.get_field(14)),
        "accept_acknowledgment_type": _v(seg.get_field(15)),
        "application_acknowledgment_type": _v(seg.get_field(16)),
        "country_code": _v(seg.get_field(17)),
        "character_set": _v(seg.get_field(18)),
        "principal_language": _v(seg.get_component(19, 1)),
        "alt_character_set_handling": _v(seg.get_field(20)),
        "message_profile_identifier": _v(seg.get_component(21, 1)),
        "sending_responsible_org": _v(seg.get_component(22, 1)),
        "receiving_responsible_org": _v(seg.get_component(23, 1)),
        "sending_network_address": _v(seg.get_component(24, 1)),
        "receiving_network_address": _v(seg.get_component(25, 1)),
    }


def _extract_pid(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)),
        "patient_id": _v(seg.get_field(2)),
        "patient_identifier_list": _v(seg.get_first_repetition(3)),
        "patient_id_value": _v(seg.get_rep_component(3, 1, 1)),
        "patient_id_check_digit": _v(seg.get_rep_component(3, 1, 2)),
        "patient_id_check_digit_scheme": _v(seg.get_rep_component(3, 1, 3)),
        "patient_id_assigning_authority": _v(seg.get_rep_component(3, 1, 4)),
        "patient_id_type_code": _v(seg.get_rep_component(3, 1, 5)),
        "alternate_patient_id": _v(seg.get_first_repetition(4)),
        "patient_name": _v(seg.get_first_repetition(5)),
        "patient_family_name": _v(seg.get_rep_component(5, 1, 1)),
        "patient_given_name": _v(seg.get_rep_component(5, 1, 2)),
        "patient_middle_name": _v(seg.get_rep_component(5, 1, 3)),
        "patient_name_suffix": _v(seg.get_rep_component(5, 1, 5)),
        "patient_name_prefix": _v(seg.get_rep_component(5, 1, 6)),
        "mothers_maiden_name": _v(seg.get_rep_component(6, 1, 1)),
        "date_of_birth": _parse_dtm(seg.get_field(7)),
        "administrative_sex": _v(seg.get_field(8)),
        "patient_alias": _v(seg.get_first_repetition(9)),
        "race": _v(seg.get_rep_component(10, 1, 1)),
        "patient_address": _v(seg.get_first_repetition(11)),
        "address_street": _v(seg.get_rep_component(11, 1, 1)),
        "address_other_designation": _v(seg.get_rep_component(11, 1, 2)),
        "address_city": _v(seg.get_rep_component(11, 1, 3)),
        "address_state": _v(seg.get_rep_component(11, 1, 4)),
        "address_zip": _v(seg.get_rep_component(11, 1, 5)),
        "address_country": _v(seg.get_rep_component(11, 1, 6)),
        "address_type": _v(seg.get_rep_component(11, 1, 7)),
        "county_code": _v(seg.get_field(12)),
        "home_phone": _v(seg.get_first_repetition(13)),
        "business_phone": _v(seg.get_first_repetition(14)),
        "primary_language": _v(seg.get_component(15, 1)),
        "marital_status": _v(seg.get_component(16, 1)),
        "religion": _v(seg.get_component(17, 1)),
        "patient_account_number": _v(seg.get_component(18, 1)),
        "ssn": _v(seg.get_field(19)),
        "drivers_license": _v(seg.get_field(20)),
        "mothers_identifier": _v(seg.get_rep_component(21, 1, 1)),
        "ethnic_group": _v(seg.get_rep_component(22, 1, 1)),
        "birth_place": _v(seg.get_field(23)),
        "multiple_birth_indicator": _v(seg.get_field(24)),
        "birth_order": _i(seg.get_field(25)),
        "citizenship": _v(seg.get_rep_component(26, 1, 1)),
        "veterans_military_status": _v(seg.get_component(27, 1)),
        "nationality": _v(seg.get_component(28, 1)),
        "patient_death_datetime": _parse_dtm(seg.get_field(29)),
        "patient_death_indicator": _v(seg.get_field(30)),
        "identity_unknown_indicator": _v(seg.get_field(31)),
        "identity_reliability_code": _v(seg.get_first_repetition(32)),
        "last_update_datetime": _parse_dtm(seg.get_field(33)),
        "last_update_facility": _v(seg.get_component(34, 1)),
        "species_code": _v(seg.get_component(35, 1)),
        "breed_code": _v(seg.get_component(36, 1)),
        "strain": _v(seg.get_field(37)),
        "production_class_code": _v(seg.get_component(38, 1)),
        "tribal_citizenship": _v(seg.get_component(39, 1)),
        "patient_telecommunication": _v(seg.get_first_repetition(40)),
    }


def _extract_pv1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)),
        "patient_class": _v(seg.get_field(2)),
        "assigned_patient_location": _v(seg.get_field(3)),
        "location_point_of_care": _v(seg.get_component(3, 1)),
        "location_room": _v(seg.get_component(3, 2)),
        "location_bed": _v(seg.get_component(3, 3)),
        "location_facility": _v(seg.get_component(3, 4)),
        "location_status": _v(seg.get_component(3, 5)),
        "location_type": _v(seg.get_component(3, 9)),
        "admission_type": _v(seg.get_field(4)),
        "preadmit_number": _v(seg.get_component(5, 1)),
        "prior_patient_location": _v(seg.get_field(6)),
        "attending_doctor": _v(seg.get_first_repetition(7)),
        "attending_doctor_id": _v(seg.get_rep_component(7, 1, 1)),
        "attending_doctor_family_name": _v(seg.get_rep_component(7, 1, 2)),
        "attending_doctor_given_name": _v(seg.get_rep_component(7, 1, 3)),
        "attending_doctor_prefix": _v(seg.get_rep_component(7, 1, 6)),
        "referring_doctor": _v(seg.get_first_repetition(8)),
        "referring_doctor_id": _v(seg.get_rep_component(8, 1, 1)),
        "referring_doctor_family_name": _v(seg.get_rep_component(8, 1, 2)),
        "referring_doctor_given_name": _v(seg.get_rep_component(8, 1, 3)),
        "consulting_doctor": _v(seg.get_first_repetition(9)),
        "hospital_service": _v(seg.get_field(10)),
        "temporary_location": _v(seg.get_field(11)),
        "preadmit_test_indicator": _v(seg.get_field(12)),
        "readmission_indicator": _v(seg.get_field(13)),
        "admit_source": _v(seg.get_field(14)),
        "ambulatory_status": _v(seg.get_first_repetition(15)),
        "vip_indicator": _v(seg.get_field(16)),
        "admitting_doctor": _v(seg.get_first_repetition(17)),
        "admitting_doctor_id": _v(seg.get_rep_component(17, 1, 1)),
        "admitting_doctor_family_name": _v(seg.get_rep_component(17, 1, 2)),
        "admitting_doctor_given_name": _v(seg.get_rep_component(17, 1, 3)),
        "patient_type": _v(seg.get_field(18)),
        "visit_number": _v(seg.get_component(19, 1)),
        "financial_class": _v(seg.get_rep_component(20, 1, 1)),
        "charge_price_indicator": _v(seg.get_field(21)),
        "courtesy_code": _v(seg.get_field(22)),
        "credit_rating": _v(seg.get_field(23)),
        "contract_code": _v(seg.get_first_repetition(24)),
        "contract_effective_date": _v(seg.get_first_repetition(25)),
        "contract_amount": _v(seg.get_first_repetition(26)),
        "contract_period": _v(seg.get_first_repetition(27)),
        "interest_code": _v(seg.get_field(28)),
        "transfer_to_bad_debt_code": _v(seg.get_field(29)),
        "transfer_to_bad_debt_date": _v(seg.get_field(30)),
        "bad_debt_agency_code": _v(seg.get_field(31)),
        "bad_debt_transfer_amount": _v(seg.get_field(32)),
        "bad_debt_recovery_amount": _v(seg.get_field(33)),
        "delete_account_indicator": _v(seg.get_field(34)),
        "delete_account_date": _v(seg.get_field(35)),
        "discharge_disposition": _v(seg.get_field(36)),
        "discharged_to_location": _v(seg.get_component(37, 1)),
        "diet_type": _v(seg.get_component(38, 1)),
        "servicing_facility": _v(seg.get_field(39)),
        "bed_status": _v(seg.get_field(40)),
        "account_status": _v(seg.get_field(41)),
        "pending_location": _v(seg.get_field(42)),
        "prior_temporary_location": _v(seg.get_field(43)),
        "admit_datetime": _parse_dtm(seg.get_first_repetition(44)),
        "discharge_datetime": _parse_dtm(seg.get_first_repetition(45)),
        "current_patient_balance": _v(seg.get_field(46)),
        "total_charges": _v(seg.get_field(47)),
        "total_adjustments": _v(seg.get_field(48)),
        "total_payments": _v(seg.get_field(49)),
        "alternate_visit_id": _v(seg.get_component(50, 1)),
        "visit_indicator": _v(seg.get_field(51)),
        "other_healthcare_provider": _v(seg.get_first_repetition(52)),
    }


def _extract_obr(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "placer_order_number": _v(seg.get_component(2, 1)),
        "filler_order_number": _v(seg.get_component(3, 1)),
        "universal_service_identifier": _v(seg.get_field(4)),
        "service_id": _v(seg.get_component(4, 1)),
        "service_text": _v(seg.get_component(4, 2)),
        "service_coding_system": _v(seg.get_component(4, 3)),
        "priority": _v(seg.get_field(5)),
        "requested_datetime": _parse_dtm(seg.get_field(6)),
        "observation_datetime": _parse_dtm(seg.get_field(7)),
        "observation_end_datetime": _parse_dtm(seg.get_field(8)),
        "collection_volume": _v(seg.get_component(9, 1)),
        "collector_identifier": _v(seg.get_rep_component(10, 1, 1)),
        "specimen_action_code": _v(seg.get_field(11)),
        "danger_code": _v(seg.get_component(12, 1)),
        "relevant_clinical_information": _v(seg.get_field(13)),
        "specimen_received_datetime": _parse_dtm(seg.get_field(14)),
        "specimen_source": _v(seg.get_field(15)),
        "ordering_provider": _v(seg.get_first_repetition(16)),
        "ordering_provider_id": _v(seg.get_rep_component(16, 1, 1)),
        "ordering_provider_family_name": _v(seg.get_rep_component(16, 1, 2)),
        "ordering_provider_given_name": _v(seg.get_rep_component(16, 1, 3)),
        "order_callback_phone": _v(seg.get_first_repetition(17)),
        "placer_field_1": _v(seg.get_field(18)),
        "placer_field_2": _v(seg.get_field(19)),
        "filler_field_1": _v(seg.get_field(20)),
        "filler_field_2": _v(seg.get_field(21)),
        "results_rpt_status_chng_datetime": _parse_dtm(seg.get_field(22)),
        "charge_to_practice": _v(seg.get_field(23)),
        "diagnostic_service_section_id": _v(seg.get_field(24)),
        "result_status": _v(seg.get_field(25)),
        "parent_result": _v(seg.get_field(26)),
        "quantity_timing": _v(seg.get_first_repetition(27)),
        "result_copies_to": _v(seg.get_rep_component(28, 1, 1)),
        "parent_placer_order_number": _v(seg.get_component(29, 1)),
        "transportation_mode": _v(seg.get_field(30)),
        "reason_for_study": _v(seg.get_rep_component(31, 1, 1)),
        "principal_result_interpreter": _v(seg.get_component(32, 1)),
        "assistant_result_interpreter": _v(seg.get_rep_component(33, 1, 1)),
        "technician": _v(seg.get_rep_component(34, 1, 1)),
        "transcriptionist": _v(seg.get_rep_component(35, 1, 1)),
        "scheduled_datetime": _parse_dtm(seg.get_field(36)),
        "number_of_sample_containers": _i(seg.get_field(37)),
        "transport_logistics": _v(seg.get_rep_component(38, 1, 1)),
        "collectors_comment": _v(seg.get_rep_component(39, 1, 1)),
        "transport_arrangement_responsibility": _v(seg.get_component(40, 1)),
        "transport_arranged": _v(seg.get_field(41)),
        "escort_required": _v(seg.get_field(42)),
        "planned_patient_transport_comment": _v(seg.get_rep_component(43, 1, 1)),
        "procedure_code": _v(seg.get_component(44, 1)),
        "procedure_code_modifier": _v(seg.get_rep_component(45, 1, 1)),
        "placer_supplemental_service_info": _v(seg.get_rep_component(46, 1, 1)),
        "filler_supplemental_service_info": _v(seg.get_rep_component(47, 1, 1)),
        "medically_necessary_dup_proc_reason": _v(seg.get_component(48, 1)),
        "result_handling": _v(seg.get_component(49, 1)),
        "parent_universal_service_id": _v(seg.get_component(50, 1)),
    }


def _extract_obx(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "value_type": _v(seg.get_field(2)),
        "observation_identifier": _v(seg.get_field(3)),
        "observation_id": _v(seg.get_component(3, 1)),
        "observation_text": _v(seg.get_component(3, 2)),
        "observation_coding_system": _v(seg.get_component(3, 3)),
        "observation_alt_id": _v(seg.get_component(3, 4)),
        "observation_alt_text": _v(seg.get_component(3, 5)),
        "observation_alt_coding_system": _v(seg.get_component(3, 6)),
        "observation_sub_id": _v(seg.get_field(4)),
        "observation_value": _v(seg.get_first_repetition(5)),
        "units": _v(seg.get_field(6)),
        "units_code": _v(seg.get_component(6, 1)),
        "units_text": _v(seg.get_component(6, 2)),
        "units_coding_system": _v(seg.get_component(6, 3)),
        "references_range": _v(seg.get_field(7)),
        "interpretation_codes": _v(seg.get_first_repetition(8)),
        "probability": _v(seg.get_field(9)),
        "nature_of_abnormal_test": _v(seg.get_first_repetition(10)),
        "observation_result_status": _v(seg.get_field(11)),
        "effective_date_of_ref_range": _parse_dtm(seg.get_field(12)),
        "user_defined_access_checks": _v(seg.get_field(13)),
        "datetime_of_observation": _parse_dtm(seg.get_field(14)),
        "producers_id": _v(seg.get_component(15, 1)),
        "responsible_observer": _v(seg.get_rep_component(16, 1, 1)),
        "observation_method": _v(seg.get_rep_component(17, 1, 1)),
        "equipment_instance_identifier": _v(seg.get_rep_component(18, 1, 1)),
        "datetime_of_analysis": _parse_dtm(seg.get_field(19)),
        "observation_site": _v(seg.get_rep_component(20, 1, 1)),
        "observation_instance_identifier": _v(seg.get_component(21, 1)),
        "mood_code": _v(seg.get_component(22, 1)),
        "performing_organization_name": _v(seg.get_component(23, 1)),
        "performing_organization_address": _v(seg.get_field(24)),
        "performing_org_medical_director": _v(seg.get_rep_component(25, 1, 1)),
        "patient_results_release_category": _v(seg.get_field(26)),
        "root_cause": _v(seg.get_component(27, 1)),
        "local_process_control": _v(seg.get_rep_component(28, 1, 1)),
    }


def _extract_al1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "allergen_type_code": _v(seg.get_component(2, 1)),
        "allergen_code": _v(seg.get_field(3)),
        "allergen_id": _v(seg.get_component(3, 1)),
        "allergen_text": _v(seg.get_component(3, 2)),
        "allergen_coding_system": _v(seg.get_component(3, 3)),
        "allergy_severity_code": _v(seg.get_component(4, 1)),
        "allergy_reaction_code": _v(seg.get_first_repetition(5)),
        "identification_date": _parse_dtm(seg.get_field(6)),
    }


def _extract_dg1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "diagnosis_coding_method": _v(seg.get_field(2)),
        "diagnosis_code": _v(seg.get_field(3)),
        "diagnosis_id": _v(seg.get_component(3, 1)),
        "diagnosis_text": _v(seg.get_component(3, 2)),
        "diagnosis_coding_system": _v(seg.get_component(3, 3)),
        "diagnosis_description": _v(seg.get_field(4)),
        "diagnosis_datetime": _parse_dtm(seg.get_field(5)),
        "diagnosis_type": _v(seg.get_field(6)),
        "major_diagnostic_category": _v(seg.get_component(7, 1)),
        "diagnostic_related_group": _v(seg.get_component(8, 1)),
        "drg_approval_indicator": _v(seg.get_field(9)),
        "drg_grouper_review_code": _v(seg.get_field(10)),
        "outlier_type": _v(seg.get_component(11, 1)),
        "outlier_days": _i(seg.get_field(12)),
        "outlier_cost": _v(seg.get_field(13)),
        "grouper_version_and_type": _v(seg.get_field(14)),
        "diagnosis_priority": _i(seg.get_field(15)),
        "diagnosing_clinician": _v(seg.get_rep_component(16, 1, 1)),
        "diagnosis_classification": _v(seg.get_field(17)),
        "confidential_indicator": _v(seg.get_field(18)),
        "attestation_datetime": _parse_dtm(seg.get_field(19)),
        "diagnosis_identifier": _v(seg.get_component(20, 1)),
        "diagnosis_action_code": _v(seg.get_field(21)),
        "parent_diagnosis": _v(seg.get_component(22, 1)),
        "drg_ccl_value_code": _v(seg.get_component(23, 1)),
        "drg_grouping_usage": _v(seg.get_field(24)),
        "drg_diagnosis_determination_status": _v(seg.get_field(25)),
        "present_on_admission_indicator": _v(seg.get_field(26)),
    }


def _extract_nk1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "name": _v(seg.get_first_repetition(2)),
        "nk_family_name": _v(seg.get_rep_component(2, 1, 1)),
        "nk_given_name": _v(seg.get_rep_component(2, 1, 2)),
        "nk_middle_name": _v(seg.get_rep_component(2, 1, 3)),
        "relationship": _v(seg.get_field(3)),
        "relationship_code": _v(seg.get_component(3, 1)),
        "relationship_text": _v(seg.get_component(3, 2)),
        "address": _v(seg.get_first_repetition(4)),
        "phone_number": _v(seg.get_first_repetition(5)),
        "business_phone": _v(seg.get_first_repetition(6)),
        "contact_role": _v(seg.get_component(7, 1)),
        "start_date": _parse_dtm(seg.get_field(8)),
        "end_date": _parse_dtm(seg.get_field(9)),
        "job_title": _v(seg.get_field(10)),
        "job_code": _v(seg.get_component(11, 1)),
        "employee_number": _v(seg.get_component(12, 1)),
        "organization_name": _v(seg.get_rep_component(13, 1, 1)),
        "marital_status": _v(seg.get_component(14, 1)),
        "administrative_sex": _v(seg.get_field(15)),
        "date_of_birth": _parse_dtm(seg.get_field(16)),
        "living_dependency": _v(seg.get_rep_component(17, 1, 1)),
        "ambulatory_status": _v(seg.get_rep_component(18, 1, 1)),
        "citizenship": _v(seg.get_rep_component(19, 1, 1)),
        "primary_language": _v(seg.get_component(20, 1)),
        "living_arrangement": _v(seg.get_field(21)),
        "publicity_code": _v(seg.get_component(22, 1)),
        "protection_indicator": _v(seg.get_field(23)),
        "student_indicator": _v(seg.get_field(24)),
        "religion": _v(seg.get_component(25, 1)),
        "mothers_maiden_name": _v(seg.get_rep_component(26, 1, 1)),
        "nationality": _v(seg.get_component(27, 1)),
        "ethnic_group": _v(seg.get_rep_component(28, 1, 1)),
        "contact_reason": _v(seg.get_rep_component(29, 1, 1)),
        "contact_person_name": _v(seg.get_rep_component(30, 1, 1)),
        "contact_person_telephone": _v(seg.get_first_repetition(31)),
        "contact_persons_address": _v(seg.get_first_repetition(32)),
        "associated_party_identifiers": _v(seg.get_rep_component(33, 1, 1)),
        "job_status": _v(seg.get_field(34)),
        "race": _v(seg.get_rep_component(35, 1, 1)),
        "handicap": _v(seg.get_field(36)),
        "contact_ssn": _v(seg.get_field(37)),
        "nk_birth_place": _v(seg.get_field(38)),
        "vip_indicator": _v(seg.get_field(39)),
    }


def _extract_evn(seg: HL7Segment) -> dict:
    return {
        "event_type_code": _v(seg.get_field(1)),
        "recorded_datetime": _parse_dtm(seg.get_field(2)),
        "date_time_planned_event": _parse_dtm(seg.get_field(3)),
        "event_reason_code": _v(seg.get_component(4, 1)),
        "operator_id": _v(seg.get_rep_component(5, 1, 1)),
        "event_occurred": _parse_dtm(seg.get_field(6)),
        "event_facility": _v(seg.get_component(7, 1)),
    }


def _extract_pd1(seg: HL7Segment) -> dict:
    return {
        "living_dependency": _v(seg.get_first_repetition(1)),
        "living_arrangement": _v(seg.get_field(2)),
        "patient_primary_facility": _v(seg.get_rep_component(3, 1, 1)),
        "patient_primary_care_provider": _v(seg.get_rep_component(4, 1, 1)),
        "student_indicator": _v(seg.get_field(5)),
        "handicap": _v(seg.get_field(6)),
        "living_will_code": _v(seg.get_field(7)),
        "organ_donor_code": _v(seg.get_field(8)),
        "separate_bill": _v(seg.get_field(9)),
        "duplicate_patient": _v(seg.get_rep_component(10, 1, 1)),
        "publicity_code": _v(seg.get_component(11, 1)),
        "protection_indicator": _v(seg.get_field(12)),
        "protection_indicator_effective_date": _v(seg.get_field(13)),
        "place_of_worship": _v(seg.get_rep_component(14, 1, 1)),
        "advance_directive_code": _v(seg.get_rep_component(15, 1, 1)),
        "immunization_registry_status": _v(seg.get_field(16)),
        "immunization_registry_status_effective_date": _v(seg.get_field(17)),
        "publicity_code_effective_date": _v(seg.get_field(18)),
        "military_branch": _v(seg.get_field(19)),
        "military_rank_grade": _v(seg.get_field(20)),
        "military_status": _v(seg.get_field(21)),
    }


def _extract_pv2(seg: HL7Segment) -> dict:
    return {
        "prior_pending_location": _v(seg.get_field(1)),
        "accommodation_code": _v(seg.get_component(2, 1)),
        "admit_reason": _v(seg.get_component(3, 1)),
        "transfer_reason": _v(seg.get_component(4, 1)),
        "patient_valuables": _v(seg.get_first_repetition(5)),
        "patient_valuables_location": _v(seg.get_field(6)),
        "visit_user_code": _v(seg.get_first_repetition(7)),
        "expected_admit_datetime": _parse_dtm(seg.get_field(8)),
        "expected_discharge_datetime": _parse_dtm(seg.get_field(9)),
        "estimated_length_of_inpatient_stay": _i(seg.get_field(10)),
        "actual_length_of_inpatient_stay": _i(seg.get_field(11)),
        "visit_description": _v(seg.get_field(12)),
        "referral_source_code": _v(seg.get_rep_component(13, 1, 1)),
        "previous_service_date": _v(seg.get_field(14)),
        "employment_illness_related_indicator": _v(seg.get_field(15)),
        "purge_status_code": _v(seg.get_field(16)),
        "purge_status_date": _v(seg.get_field(17)),
        "special_program_code": _v(seg.get_field(18)),
        "retention_indicator": _v(seg.get_field(19)),
        "expected_number_of_insurance_plans": _i(seg.get_field(20)),
        "visit_publicity_code": _v(seg.get_field(21)),
        "visit_protection_indicator": _v(seg.get_field(22)),
        "clinic_organization_name": _v(seg.get_rep_component(23, 1, 1)),
        "patient_status_code": _v(seg.get_field(24)),
        "visit_priority_code": _v(seg.get_field(25)),
        "previous_treatment_date": _v(seg.get_field(26)),
        "expected_discharge_disposition": _v(seg.get_field(27)),
        "signature_on_file_date": _v(seg.get_field(28)),
        "first_similar_illness_date": _v(seg.get_field(29)),
        "patient_charge_adjustment_code": _v(seg.get_component(30, 1)),
        "recurring_service_code": _v(seg.get_field(31)),
        "billing_media_code": _v(seg.get_field(32)),
        "expected_surgery_datetime": _parse_dtm(seg.get_field(33)),
        "military_partnership_code": _v(seg.get_field(34)),
        "military_non_availability_code": _v(seg.get_field(35)),
        "newborn_baby_indicator": _v(seg.get_field(36)),
        "baby_detained_indicator": _v(seg.get_field(37)),
        "mode_of_arrival_code": _v(seg.get_component(38, 1)),
        "recreational_drug_use_code": _v(seg.get_rep_component(39, 1, 1)),
        "admission_level_of_care_code": _v(seg.get_component(40, 1)),
        "precaution_code": _v(seg.get_rep_component(41, 1, 1)),
        "patient_condition_code": _v(seg.get_component(42, 1)),
        "living_will_code_pv2": _v(seg.get_field(43)),
        "organ_donor_code_pv2": _v(seg.get_field(44)),
        "advance_directive_code_pv2": _v(seg.get_rep_component(45, 1, 1)),
        "patient_status_effective_date": _v(seg.get_field(46)),
        "expected_loa_return_datetime": _parse_dtm(seg.get_field(47)),
        "expected_preadmission_testing_datetime": _parse_dtm(seg.get_field(48)),
        "notify_clergy_code": _v(seg.get_first_repetition(49)),
    }


def _extract_mrg(seg: HL7Segment) -> dict:
    return {
        "prior_patient_identifier_list": _v(seg.get_first_repetition(1)),
        "prior_patient_id_value": _v(seg.get_rep_component(1, 1, 1)),
        "prior_alternate_patient_id": _v(seg.get_first_repetition(2)),
        "prior_patient_account_number": _v(seg.get_component(3, 1)),
        "prior_patient_id": _v(seg.get_component(4, 1)),
        "prior_visit_number": _v(seg.get_component(5, 1)),
        "prior_alternate_visit_id": _v(seg.get_component(6, 1)),
        "prior_patient_name": _v(seg.get_first_repetition(7)),
        "prior_patient_family_name": _v(seg.get_rep_component(7, 1, 1)),
        "prior_patient_given_name": _v(seg.get_rep_component(7, 1, 2)),
    }


def _extract_iam(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "allergen_type_code": _v(seg.get_component(2, 1)),
        "allergen_code": _v(seg.get_field(3)),
        "allergen_id": _v(seg.get_component(3, 1)),
        "allergen_text": _v(seg.get_component(3, 2)),
        "allergen_coding_system": _v(seg.get_component(3, 3)),
        "allergy_severity_code": _v(seg.get_component(4, 1)),
        "allergy_reaction_code": _v(seg.get_first_repetition(5)),
        "allergy_action_code": _v(seg.get_component(6, 1)),
        "allergy_unique_identifier": _v(seg.get_component(7, 1)),
        "action_reason": _v(seg.get_field(8)),
        "sensitivity_to_causative_agent_code": _v(seg.get_component(9, 1)),
        "allergen_group_code": _v(seg.get_component(10, 1)),
        "allergen_group_text": _v(seg.get_component(10, 2)),
        "onset_date": _v(seg.get_field(11)),
        "onset_date_text": _v(seg.get_field(12)),
        "reported_datetime": _parse_dtm(seg.get_field(13)),
        "reported_by": _v(seg.get_first_repetition(14)),
        "reported_by_family_name": _v(seg.get_rep_component(14, 1, 1)),
        "reported_by_given_name": _v(seg.get_rep_component(14, 1, 2)),
        "relationship_to_patient_code": _v(seg.get_component(15, 1)),
        "alert_device_code": _v(seg.get_component(16, 1)),
        "allergy_clinical_status_code": _v(seg.get_component(17, 1)),
        "statused_by_person": _v(seg.get_component(18, 1)),
        "statused_by_organization": _v(seg.get_component(19, 1)),
        "statused_at_datetime": _parse_dtm(seg.get_field(20)),
    }


def _extract_pr1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "procedure_coding_method": _v(seg.get_field(2)),
        "procedure_code": _v(seg.get_field(3)),
        "procedure_id": _v(seg.get_component(3, 1)),
        "procedure_text": _v(seg.get_component(3, 2)),
        "procedure_coding_system": _v(seg.get_component(3, 3)),
        "procedure_description": _v(seg.get_field(4)),
        "procedure_datetime": _parse_dtm(seg.get_field(5)),
        "procedure_functional_type": _v(seg.get_field(6)),
        "procedure_minutes": _i(seg.get_field(7)),
        "anesthesiologist": _v(seg.get_rep_component(8, 1, 1)),
        "anesthesia_code": _v(seg.get_field(9)),
        "anesthesia_minutes": _i(seg.get_field(10)),
        "surgeon": _v(seg.get_rep_component(11, 1, 1)),
        "procedure_practitioner": _v(seg.get_rep_component(12, 1, 1)),
        "consent_code": _v(seg.get_component(13, 1)),
        "procedure_priority": _v(seg.get_field(14)),
        "associated_diagnosis_code": _v(seg.get_component(15, 1)),
        "procedure_code_modifier": _v(seg.get_rep_component(16, 1, 1)),
        "procedure_drg_type": _v(seg.get_field(17)),
        "tissue_type_code": _v(seg.get_rep_component(18, 1, 1)),
        "procedure_identifier": _v(seg.get_component(19, 1)),
        "procedure_action_code": _v(seg.get_field(20)),
    }


def _extract_orc(seg: HL7Segment) -> dict:
    return {
        "order_control": _v(seg.get_field(1)),
        "placer_order_number": _v(seg.get_component(2, 1)),
        "filler_order_number": _v(seg.get_component(3, 1)),
        "placer_group_number": _v(seg.get_component(4, 1)),
        "order_status": _v(seg.get_field(5)),
        "response_flag": _v(seg.get_field(6)),
        "quantity_timing": _v(seg.get_first_repetition(7)),
        "parent_order": _v(seg.get_field(8)),
        "datetime_of_transaction": _parse_dtm(seg.get_field(9)),
        "entered_by": _v(seg.get_rep_component(10, 1, 1)),
        "verified_by": _v(seg.get_rep_component(11, 1, 1)),
        "ordering_provider": _v(seg.get_first_repetition(12)),
        "ordering_provider_id": _v(seg.get_rep_component(12, 1, 1)),
        "ordering_provider_family_name": _v(seg.get_rep_component(12, 1, 2)),
        "ordering_provider_given_name": _v(seg.get_rep_component(12, 1, 3)),
        "enterers_location": _v(seg.get_field(13)),
        "call_back_phone_number": _v(seg.get_first_repetition(14)),
        "order_effective_datetime": _parse_dtm(seg.get_field(15)),
        "order_control_code_reason": _v(seg.get_component(16, 1)),
        "entering_organization": _v(seg.get_component(17, 1)),
        "entering_device": _v(seg.get_component(18, 1)),
        "action_by": _v(seg.get_rep_component(19, 1, 1)),
        "advanced_beneficiary_notice_code": _v(seg.get_component(20, 1)),
        "ordering_facility_name": _v(seg.get_rep_component(21, 1, 1)),
        "ordering_facility_address": _v(seg.get_first_repetition(22)),
        "ordering_facility_phone": _v(seg.get_first_repetition(23)),
        "ordering_provider_address": _v(seg.get_first_repetition(24)),
        "order_status_modifier": _v(seg.get_component(25, 1)),
        "abn_override_reason": _v(seg.get_component(26, 1)),
        "fillers_expected_availability_datetime": _parse_dtm(seg.get_field(27)),
        "confidentiality_code": _v(seg.get_component(28, 1)),
        "order_type": _v(seg.get_component(29, 1)),
        "enterer_authorization_mode": _v(seg.get_component(30, 1)),
        "parent_universal_service_id": _v(seg.get_component(31, 1)),
    }


def _extract_nte(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "source_of_comment": _v(seg.get_field(2)),
        "comment": _v(seg.get_first_repetition(3)),
        "comment_type": _v(seg.get_component(4, 1)),
    }


def _extract_spm(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "specimen_id": _v(seg.get_component(2, 1)),
        "specimen_parent_ids": _v(seg.get_rep_component(3, 1, 1)),
        "specimen_type": _v(seg.get_field(4)),
        "specimen_type_code": _v(seg.get_component(4, 1)),
        "specimen_type_text": _v(seg.get_component(4, 2)),
        "specimen_type_modifier": _v(seg.get_rep_component(5, 1, 1)),
        "specimen_additives": _v(seg.get_rep_component(6, 1, 1)),
        "specimen_collection_method": _v(seg.get_component(7, 1)),
        "specimen_source_site": _v(seg.get_component(8, 1)),
        "specimen_source_site_modifier": _v(seg.get_rep_component(9, 1, 1)),
        "specimen_collection_site": _v(seg.get_component(10, 1)),
        "specimen_role": _v(seg.get_rep_component(11, 1, 1)),
        "specimen_collection_amount": _v(seg.get_component(12, 1)),
        "grouped_specimen_count": _i(seg.get_field(13)),
        "specimen_description": _v(seg.get_first_repetition(14)),
        "specimen_handling_code": _v(seg.get_rep_component(15, 1, 1)),
        "specimen_risk_code": _v(seg.get_rep_component(16, 1, 1)),
        "specimen_collection_datetime": _v(seg.get_component(17, 1)),
        "specimen_received_datetime": _parse_dtm(seg.get_field(18)),
        "specimen_expiration_datetime": _parse_dtm(seg.get_field(19)),
        "specimen_availability": _v(seg.get_field(20)),
        "specimen_reject_reason": _v(seg.get_rep_component(21, 1, 1)),
        "specimen_quality": _v(seg.get_component(22, 1)),
        "specimen_appropriateness": _v(seg.get_component(23, 1)),
        "specimen_condition": _v(seg.get_rep_component(24, 1, 1)),
        "specimen_current_quantity": _v(seg.get_component(25, 1)),
        "number_of_specimen_containers": _i(seg.get_field(26)),
        "container_type": _v(seg.get_component(27, 1)),
        "container_condition": _v(seg.get_component(28, 1)),
        "specimen_child_role": _v(seg.get_component(29, 1)),
    }


def _extract_in1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "insurance_plan_id": _v(seg.get_component(2, 1)),
        "insurance_plan_text": _v(seg.get_component(2, 2)),
        "insurance_company_id": _v(seg.get_rep_component(3, 1, 1)),
        "insurance_company_name": _v(seg.get_rep_component(4, 1, 1)),
        "insurance_company_address": _v(seg.get_first_repetition(5)),
        "insurance_co_contact_person": _v(seg.get_rep_component(6, 1, 1)),
        "insurance_co_phone_number": _v(seg.get_first_repetition(7)),
        "group_number": _v(seg.get_field(8)),
        "group_name": _v(seg.get_rep_component(9, 1, 1)),
        "insureds_group_emp_id": _v(seg.get_rep_component(10, 1, 1)),
        "insureds_group_emp_name": _v(seg.get_rep_component(11, 1, 1)),
        "plan_effective_date": _v(seg.get_field(12)),
        "plan_expiration_date": _v(seg.get_field(13)),
        "authorization_information": _v(seg.get_component(14, 1)),
        "plan_type": _v(seg.get_field(15)),
        "name_of_insured": _v(seg.get_first_repetition(16)),
        "name_of_insured_family": _v(seg.get_rep_component(16, 1, 1)),
        "name_of_insured_given": _v(seg.get_rep_component(16, 1, 2)),
        "insureds_relationship_to_patient": _v(seg.get_component(17, 1)),
        "insureds_date_of_birth": _parse_dtm(seg.get_field(18)),
        "insureds_address": _v(seg.get_first_repetition(19)),
        "assignment_of_benefits": _v(seg.get_field(20)),
        "coordination_of_benefits": _v(seg.get_field(21)),
        "coord_of_ben_priority": _v(seg.get_field(22)),
        "notice_of_admission_flag": _v(seg.get_field(23)),
        "notice_of_admission_date": _v(seg.get_field(24)),
        "report_of_eligibility_flag": _v(seg.get_field(25)),
        "report_of_eligibility_date": _v(seg.get_field(26)),
        "release_information_code": _v(seg.get_field(27)),
        "pre_admit_cert": _v(seg.get_field(28)),
        "verification_datetime": _parse_dtm(seg.get_field(29)),
        "verification_by": _v(seg.get_rep_component(30, 1, 1)),
        "type_of_agreement_code": _v(seg.get_field(31)),
        "billing_status": _v(seg.get_field(32)),
        "lifetime_reserve_days": _i(seg.get_field(33)),
        "delay_before_lr_day": _i(seg.get_field(34)),
        "company_plan_code": _v(seg.get_field(35)),
        "policy_number": _v(seg.get_field(36)),
        "policy_deductible": _v(seg.get_component(37, 1)),
        "policy_limit_amount": _v(seg.get_component(38, 1)),
        "policy_limit_days": _i(seg.get_field(39)),
        "room_rate_semi_private": _v(seg.get_component(40, 1)),
        "room_rate_private": _v(seg.get_component(41, 1)),
        "insureds_employment_status": _v(seg.get_component(42, 1)),
        "insureds_administrative_sex": _v(seg.get_field(43)),
        "insureds_employers_address": _v(seg.get_first_repetition(44)),
        "verification_status": _v(seg.get_field(45)),
        "prior_insurance_plan_id": _v(seg.get_field(46)),
        "coverage_type": _v(seg.get_field(47)),
        "handicap": _v(seg.get_field(48)),
        "insureds_id_number": _v(seg.get_rep_component(49, 1, 1)),
        "signature_code": _v(seg.get_field(50)),
        "signature_code_date": _v(seg.get_field(51)),
        "insureds_birth_place": _v(seg.get_field(52)),
        "vip_indicator": _v(seg.get_field(53)),
    }


def _extract_gt1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "guarantor_number": _v(seg.get_rep_component(2, 1, 1)),
        "guarantor_name": _v(seg.get_first_repetition(3)),
        "guarantor_family_name": _v(seg.get_rep_component(3, 1, 1)),
        "guarantor_given_name": _v(seg.get_rep_component(3, 1, 2)),
        "guarantor_spouse_name": _v(seg.get_rep_component(4, 1, 1)),
        "guarantor_address": _v(seg.get_first_repetition(5)),
        "guarantor_ph_num_home": _v(seg.get_first_repetition(6)),
        "guarantor_ph_num_business": _v(seg.get_first_repetition(7)),
        "guarantor_date_of_birth": _parse_dtm(seg.get_field(8)),
        "guarantor_administrative_sex": _v(seg.get_field(9)),
        "guarantor_type": _v(seg.get_field(10)),
        "guarantor_relationship": _v(seg.get_component(11, 1)),
        "guarantor_ssn": _v(seg.get_field(12)),
        "guarantor_date_begin": _v(seg.get_field(13)),
        "guarantor_date_end": _v(seg.get_field(14)),
        "guarantor_priority": _i(seg.get_field(15)),
        "guarantor_employer_name": _v(seg.get_rep_component(16, 1, 1)),
        "guarantor_employer_address": _v(seg.get_first_repetition(17)),
        "guarantor_employer_phone_number": _v(seg.get_first_repetition(18)),
        "guarantor_employee_id_number": _v(seg.get_rep_component(19, 1, 1)),
        "guarantor_employment_status": _v(seg.get_field(20)),
        "guarantor_organization_name": _v(seg.get_rep_component(21, 1, 1)),
        "guarantor_billing_hold_flag": _v(seg.get_field(22)),
        "guarantor_credit_rating_code": _v(seg.get_component(23, 1)),
        "guarantor_death_date_and_time": _parse_dtm(seg.get_field(24)),
        "guarantor_death_flag": _v(seg.get_field(25)),
        "guarantor_charge_adjustment_code": _v(seg.get_component(26, 1)),
        "guarantor_household_annual_income": _v(seg.get_component(27, 1)),
        "guarantor_household_size": _i(seg.get_field(28)),
        "guarantor_employer_id_number": _v(seg.get_rep_component(29, 1, 1)),
        "guarantor_marital_status_code": _v(seg.get_component(30, 1)),
        "guarantor_hire_effective_date": _v(seg.get_field(31)),
        "employment_stop_date": _v(seg.get_field(32)),
        "living_dependency": _v(seg.get_field(33)),
        "ambulatory_status": _v(seg.get_first_repetition(34)),
        "citizenship": _v(seg.get_rep_component(35, 1, 1)),
        "primary_language": _v(seg.get_component(36, 1)),
        "living_arrangement": _v(seg.get_field(37)),
        "publicity_code": _v(seg.get_component(38, 1)),
        "protection_indicator": _v(seg.get_field(39)),
        "student_indicator": _v(seg.get_field(40)),
        "religion": _v(seg.get_component(41, 1)),
        "mothers_maiden_name": _v(seg.get_rep_component(42, 1, 1)),
        "nationality": _v(seg.get_component(43, 1)),
        "ethnic_group": _v(seg.get_rep_component(44, 1, 1)),
        "contact_persons_name": _v(seg.get_rep_component(45, 1, 1)),
        "contact_persons_telephone_number": _v(seg.get_first_repetition(46)),
        "contact_reason": _v(seg.get_component(47, 1)),
        "contact_relationship": _v(seg.get_field(48)),
        "job_title": _v(seg.get_field(49)),
        "job_code_class": _v(seg.get_component(50, 1)),
        "guarantor_employers_org_name": _v(seg.get_rep_component(51, 1, 1)),
        "handicap": _v(seg.get_field(52)),
        "job_status": _v(seg.get_field(53)),
        "guarantor_financial_class": _v(seg.get_component(54, 1)),
        "guarantor_race": _v(seg.get_rep_component(55, 1, 1)),
        "guarantor_birth_place": _v(seg.get_field(56)),
        "vip_indicator": _v(seg.get_field(57)),
    }


def _extract_ft1(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "transaction_id": _v(seg.get_field(2)),
        "transaction_batch_id": _v(seg.get_field(3)),
        "transaction_date": _v(seg.get_component(4, 1)),
        "transaction_posting_date": _parse_dtm(seg.get_field(5)),
        "transaction_type": _v(seg.get_field(6)),
        "transaction_code": _v(seg.get_field(7)),
        "transaction_code_id": _v(seg.get_component(7, 1)),
        "transaction_code_text": _v(seg.get_component(7, 2)),
        "transaction_description": _v(seg.get_field(8)),
        "transaction_description_alt": _v(seg.get_field(9)),
        "transaction_quantity": _i(seg.get_field(10)),
        "transaction_amount_extended": _v(seg.get_component(11, 1)),
        "transaction_amount_unit": _v(seg.get_component(12, 1)),
        "department_code": _v(seg.get_component(13, 1)),
        "insurance_plan_id": _v(seg.get_component(14, 1)),
        "insurance_amount": _v(seg.get_component(15, 1)),
        "assigned_patient_location": _v(seg.get_field(16)),
        "fee_schedule": _v(seg.get_field(17)),
        "patient_type": _v(seg.get_field(18)),
        "diagnosis_code": _v(seg.get_rep_component(19, 1, 1)),
        "performed_by_code": _v(seg.get_rep_component(20, 1, 1)),
        "ordered_by_code": _v(seg.get_rep_component(21, 1, 1)),
        "unit_cost": _v(seg.get_component(22, 1)),
        "filler_order_number": _v(seg.get_component(23, 1)),
        "entered_by_code": _v(seg.get_rep_component(24, 1, 1)),
        "procedure_code": _v(seg.get_component(25, 1)),
        "procedure_code_modifier": _v(seg.get_rep_component(26, 1, 1)),
        "advanced_beneficiary_notice_code": _v(seg.get_component(27, 1)),
        "medically_necessary_dup_proc_reason": _v(seg.get_component(28, 1)),
        "ndc_code": _v(seg.get_component(29, 1)),
        "payment_reference_id": _v(seg.get_component(30, 1)),
        "transaction_reference_key": _v(seg.get_first_repetition(31)),
    }


def _extract_rxa(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "administration_sub_id_counter": _i(seg.get_field(2)),
        "datetime_start_of_administration": _parse_dtm(seg.get_field(3)),
        "datetime_end_of_administration": _parse_dtm(seg.get_field(4)),
        "administered_code": _v(seg.get_field(5)),
        "administered_code_id": _v(seg.get_component(5, 1)),
        "administered_code_text": _v(seg.get_component(5, 2)),
        "administered_amount": _v(seg.get_field(6)),
        "administered_units": _v(seg.get_component(7, 1)),
        "administered_dosage_form": _v(seg.get_component(8, 1)),
        "administration_notes": _v(seg.get_rep_component(9, 1, 1)),
        "administering_provider": _v(seg.get_rep_component(10, 1, 1)),
        "administered_at_location": _v(seg.get_field(11)),
        "administered_per_time_unit": _v(seg.get_field(12)),
        "administered_strength": _v(seg.get_field(13)),
        "administered_strength_units": _v(seg.get_component(14, 1)),
        "substance_lot_number": _v(seg.get_first_repetition(15)),
        "substance_expiration_date": _parse_dtm(seg.get_first_repetition(16)),
        "substance_manufacturer_name": _v(seg.get_rep_component(17, 1, 1)),
        "substance_treatment_refusal_reason": _v(seg.get_rep_component(18, 1, 1)),
        "indication": _v(seg.get_rep_component(19, 1, 1)),
        "completion_status": _v(seg.get_field(20)),
        "action_code_rxa": _v(seg.get_field(21)),
        "system_entry_datetime": _parse_dtm(seg.get_field(22)),
        "administered_drug_strength_volume": _v(seg.get_field(23)),
        "administered_drug_strength_volume_units": _v(seg.get_component(24, 1)),
        "administered_barcode_identifier": _v(seg.get_component(25, 1)),
        "pharmacy_order_type": _v(seg.get_field(26)),
    }


def _extract_sch(seg: HL7Segment) -> dict:
    return {
        "placer_appointment_id": _v(seg.get_component(1, 1)),
        "filler_appointment_id": _v(seg.get_component(2, 1)),
        "occurrence_number": _i(seg.get_field(3)),
        "placer_group_number": _v(seg.get_component(4, 1)),
        "schedule_id": _v(seg.get_component(5, 1)),
        "event_reason": _v(seg.get_component(6, 1)),
        "appointment_reason": _v(seg.get_component(7, 1)),
        "appointment_type": _v(seg.get_component(8, 1)),
        "appointment_duration": _i(seg.get_field(9)),
        "appointment_duration_units": _v(seg.get_component(10, 1)),
        "appointment_timing_quantity": _v(seg.get_first_repetition(11)),
        "placer_contact_person": _v(seg.get_rep_component(12, 1, 1)),
        "placer_contact_phone_number": _v(seg.get_field(13)),
        "placer_contact_address": _v(seg.get_first_repetition(14)),
        "placer_contact_location": _v(seg.get_field(15)),
        "filler_contact_person": _v(seg.get_rep_component(16, 1, 1)),
        "filler_contact_phone_number": _v(seg.get_field(17)),
        "filler_contact_address": _v(seg.get_first_repetition(18)),
        "filler_contact_location": _v(seg.get_field(19)),
        "entered_by_person": _v(seg.get_rep_component(20, 1, 1)),
        "entered_by_phone_number": _v(seg.get_first_repetition(21)),
        "entered_by_location": _v(seg.get_field(22)),
        "parent_placer_appointment_id": _v(seg.get_component(23, 1)),
        "parent_filler_appointment_id": _v(seg.get_component(24, 1)),
        "filler_status_code": _v(seg.get_component(25, 1)),
        "placer_order_number": _v(seg.get_rep_component(26, 1, 1)),
        "filler_order_number": _v(seg.get_rep_component(27, 1, 1)),
    }


def _extract_txa(seg: HL7Segment) -> dict:
    return {
        "set_id": _i(seg.get_field(1)) or 1,
        "document_type": _v(seg.get_field(2)),
        "document_content_presentation": _v(seg.get_field(3)),
        "activity_datetime": _parse_dtm(seg.get_field(4)),
        "primary_activity_provider": _v(seg.get_rep_component(5, 1, 1)),
        "origination_datetime": _parse_dtm(seg.get_field(6)),
        "transcription_datetime": _parse_dtm(seg.get_field(7)),
        "edit_datetime": _parse_dtm(seg.get_first_repetition(8)),
        "originator": _v(seg.get_rep_component(9, 1, 1)),
        "assigned_document_authenticator": _v(seg.get_rep_component(10, 1, 1)),
        "transcriptionist": _v(seg.get_rep_component(11, 1, 1)),
        "unique_document_number": _v(seg.get_component(12, 1)),
        "parent_document_number": _v(seg.get_component(13, 1)),
        "placer_order_number": _v(seg.get_rep_component(14, 1, 1)),
        "filler_order_number": _v(seg.get_component(15, 1)),
        "unique_document_file_name": _v(seg.get_field(16)),
        "document_completion_status": _v(seg.get_field(17)),
        "document_confidentiality_status": _v(seg.get_field(18)),
        "document_availability_status": _v(seg.get_field(19)),
        "document_storage_status": _v(seg.get_field(20)),
        "document_change_reason": _v(seg.get_field(21)),
        "authentication_person_time_stamp": _v(seg.get_first_repetition(22)),
        "distributed_copies": _v(seg.get_rep_component(23, 1, 1)),
    }


def _extract_generic(seg: HL7Segment) -> dict:
    """Fallback extractor for Z-segments and unknown segment types."""
    return {"segment_type": seg.segment_type} | {
        f"field_{i}": _v(seg.get_field(i)) for i in range(1, 26)
    }


_EXTRACTORS = {
    "msh": _extract_msh,
    "evn": _extract_evn,
    "pid": _extract_pid,
    "pd1": _extract_pd1,
    "pv1": _extract_pv1,
    "pv2": _extract_pv2,
    "nk1": _extract_nk1,
    "mrg": _extract_mrg,
    "al1": _extract_al1,
    "iam": _extract_iam,
    "dg1": _extract_dg1,
    "pr1": _extract_pr1,
    "orc": _extract_orc,
    "obr": _extract_obr,
    "obx": _extract_obx,
    "nte": _extract_nte,
    "spm": _extract_spm,
    "in1": _extract_in1,
    "gt1": _extract_gt1,
    "ft1": _extract_ft1,
    "rxa": _extract_rxa,
    "sch": _extract_sch,
    "txa": _extract_txa,
}


# ---------------------------------------------------------------------------
# Multi-message splitter
# ---------------------------------------------------------------------------


def _split_messages(text: str) -> list[str]:
    """Split an HL7 batch into individual message strings.

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
    """LakeflowConnect implementation for HL7 v2 messages from GCP Healthcare API.

    Fetches messages from a Google Cloud Healthcare API HL7v2 store.
    Each HL7 segment type is a separate table.  Incremental loading is driven
    by the ``sendTime`` field from the API using a sliding time-window.

    Required connection options (from connector_spec.yaml):
        project_id, location, dataset_id, hl7v2_store_id, service_account_json

    Table options:
        segment_type (str): Override segment type for custom/Z-segments.
        max_records_per_batch (str): Best-effort cap on records per micro-batch
            (default 10000).  The sliding window may yield more.
        window_seconds (str): Duration of the sliding time-window in seconds
            (default 86400).  Smaller values produce smaller batches.
        start_timestamp (str): RFC3339 timestamp to start reading from when no
            prior offset exists and auto-discovery is not possible.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        for key in ("project_id", "location", "dataset_id", "hl7v2_store_id", "service_account_json"):
            if key not in options:
                raise ValueError(f"'{key}' is required in connector options.")

        self._base_url = (
            f"https://healthcare.googleapis.com/v1"
            f"/projects/{options['project_id']}"
            f"/locations/{options['location']}"
            f"/datasets/{options['dataset_id']}"
            f"/hl7V2Stores/{options['hl7v2_store_id']}/messages"
        )

        raw_sa = options["service_account_json"]
        if isinstance(raw_sa, dict):
            sa_info = raw_sa
        else:
            try:
                sa_info = json.loads(raw_sa)
            except json.JSONDecodeError:
                sa_info = json.loads(raw_sa, strict=False)
        self._creds = google_sa.Credentials.from_service_account_info(
            sa_info, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self._google_request = google_auth_requests.Request()
        self._session = requests.Session()

        self._creds.refresh(self._google_request)

        self._init_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._oldest_send_time: str | None = None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get_headers(self) -> dict[str, str]:
        if not self._creds.valid:
            self._creds.refresh(self._google_request)
        return {"Authorization": f"Bearer {self._creds.token}"}

    def _api_get(self, params: dict[str, str]) -> dict:
        """GET the messages endpoint with retry on transient errors."""
        backoff = _INITIAL_BACKOFF
        last_resp = None
        for attempt in range(_MAX_RETRIES):
            resp = self._session.get(
                self._base_url,
                headers=self._get_headers(),
                params=params,
                timeout=_REQUEST_TIMEOUT,
            )
            last_resp = resp
            if resp.status_code not in _RETRIABLE_STATUS_CODES:
                resp.raise_for_status()
                return resp.json()
            if attempt < _MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= 2
        last_resp.raise_for_status()
        return last_resp.json()

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
        is_single = segment_type in _SINGLE_SEGMENT_TABLES
        return {
            "ingestion_type": "append",
            "cursor_field": "send_time",
            "primary_keys": ["message_id"] if is_single else ["message_id", "set_id"],
            "description": TABLE_DESCRIPTIONS.get(segment_type, ""),
        }

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        """Sliding time-window incremental read from the GCP Healthcare API.

        Fetches all messages whose ``sendTime`` falls in
        ``(since, since + window_seconds]``, parses them, and returns rows
        for the requested segment type.  The cursor advances to the window
        end regardless of whether data was found, ensuring forward progress.
        """
        self._validate_table(table_name, table_options)
        segment_type = table_options.get("segment_type", table_name).upper()

        since = start_offset.get("cursor") if start_offset is not None else None
        if not since:
            since = table_options.get("start_timestamp")
        if not since:
            since = self._peek_oldest_send_time()
        if not since:
            return iter([]), start_offset or {}

        if since >= self._init_ts:
            return iter([]), start_offset or {}

        window_seconds = int(table_options.get("window_seconds", str(_DEFAULT_WINDOW_SECONDS)))

        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        window_end_dt = since_dt + timedelta(seconds=window_seconds)
        window_end = min(
            window_end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            self._init_ts,
        )

        api_messages = self._fetch_messages_in_window(since, window_end)

        records = self._parse_api_messages(api_messages, segment_type)

        end_offset = {"cursor": window_end}
        if start_offset is not None and start_offset == end_offset:
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

    def _peek_oldest_send_time(self) -> str | None:
        """Auto-discover the earliest sendTime by fetching the first message.

        The result is cached for the lifetime of this connector instance since
        the oldest message never changes once discovered.
        """
        if self._oldest_send_time is not None:
            return self._oldest_send_time
        body = self._api_get({
            "view": "FULL",
            "pageSize": "1",
            "orderBy": "sendTime asc",
        })
        messages = body.get("hl7V2Messages", [])
        if messages:
            ts = messages[0].get("sendTime")
            if ts:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                dt -= timedelta(seconds=1)
                ts = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            self._oldest_send_time = ts
        return self._oldest_send_time

    def _fetch_messages_in_window(self, since: str, until: str) -> list[dict]:
        """Fetch all API messages with sendTime in (since, until]."""
        filter_str = f'sendTime > "{since}" AND sendTime <= "{until}"'
        messages: list[dict] = []
        page_token: str | None = None

        while True:
            params: dict[str, str] = {
                "view": "FULL",
                "pageSize": str(_MAX_PAGE_SIZE),
                "filter": filter_str,
                "orderBy": "sendTime asc",
            }
            if page_token:
                params["pageToken"] = page_token

            body = self._api_get(params)
            batch = body.get("hl7V2Messages", [])
            messages.extend(batch)

            page_token = body.get("nextPageToken")
            if not page_token:
                break

        return messages

    def _parse_api_messages(self, api_messages: list[dict], segment_type: str) -> list[dict]:
        """Decode, parse, and extract rows from API message payloads."""
        records: list[dict] = []

        for msg_data in api_messages:
            send_time = msg_data.get("sendTime", "")
            raw_data = msg_data.get("data", "")
            if not raw_data:
                continue
            raw_hl7 = base64.b64decode(raw_data).decode("utf-8", errors="replace")
            source_name = msg_data.get("name", "")

            for msg_text in _split_messages(raw_hl7):
                msg: HL7Message | None = parse_message(msg_text)
                if msg is None:
                    continue
                msh = msg.get_segment("MSH")
                meta = _metadata(msh, source_name, send_time)

                if segment_type == "MSH":
                    if msh is not None:
                        records.append(meta | _extract_msh(msh) | {"raw_segment": msh.raw_line})
                else:
                    extractor = _EXTRACTORS.get(segment_type.lower(), _extract_generic)
                    for idx, seg in enumerate(msg.get_segments(segment_type), start=1):
                        row = meta | extractor(seg) | {"raw_segment": seg.raw_line}
                        if segment_type.lower() not in _SINGLE_SEGMENT_TABLES and "set_id" not in row:
                            row["set_id"] = idx
                        records.append(row)

        return records
