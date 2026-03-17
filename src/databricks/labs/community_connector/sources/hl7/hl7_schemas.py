"""Spark StructType schemas for HL7 v2 segment tables.

Every schema is a *superset* of all fields defined across HL7 v2.1–v2.8.
Fields absent in a given message version are returned as ``None`` by the
connector, and Spark receives them as ``null``.

All wire-format values are strings; only ``set_id`` / sequence-number
fields are typed as ``LongType`` because they are guaranteed-numeric.

Every table includes four common metadata columns:
    message_id        — MSH-10 (join key across all segment tables)
    message_timestamp — MSH-7  (raw HL7 DTM string, cursor field)
    hl7_version       — MSH-12 (e.g. "2.5.1")
    source_file       — basename of the source .hl7 file
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _s(name: str) -> StructField:
    """Nullable StringType field."""
    return StructField(name, StringType(), nullable=True)


def _i(name: str) -> StructField:
    """Nullable LongType field (for HL7 numeric fields like set_id)."""
    return StructField(name, LongType(), nullable=True)


# Fields present in every segment table.
_METADATA_FIELDS: list[StructField] = [
    _s("message_id"),         # MSH-10
    _s("message_timestamp"),  # MSH-7 raw DTM string (e.g. "20240101120000")
    _s("hl7_version"),        # MSH-12 (e.g. "2.5.1")
    _s("source_file"),        # source filename for traceability
]

# ---------------------------------------------------------------------------
# MSH — Message Header
# ---------------------------------------------------------------------------

MSH_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("field_separator"),                    # MSH-1  "|"
        _s("encoding_characters"),                # MSH-2  "^~\&"
        _s("sending_application"),                # MSH-3.1
        _s("sending_facility"),                   # MSH-4.1
        _s("receiving_application"),              # MSH-5.1
        _s("receiving_facility"),                 # MSH-6.1
        _s("message_datetime"),                   # MSH-7  (same as message_timestamp)
        _s("security"),                           # MSH-8
        _s("message_code"),                       # MSH-9.1 (e.g. "ADT")
        _s("trigger_event"),                      # MSH-9.2 (e.g. "A01")
        _s("message_structure"),                  # MSH-9.3 (e.g. "ADT_A01")
        _s("message_control_id"),                 # MSH-10  (same as message_id)
        _s("processing_id"),                      # MSH-11
        _s("version_id"),                         # MSH-12  (same as hl7_version)
        _i("sequence_number"),                    # MSH-13
        _s("continuation_pointer"),               # MSH-14
        _s("accept_acknowledgment_type"),         # MSH-15
        _s("application_acknowledgment_type"),    # MSH-16
        _s("country_code"),                       # MSH-17
        _s("character_set"),                      # MSH-18
        _s("principal_language"),                 # MSH-19.1
        _s("alt_character_set_handling"),         # MSH-20
        _s("message_profile_identifier"),         # MSH-21.1
        _s("sending_responsible_org"),            # MSH-22.1  (v2.7+)
        _s("receiving_responsible_org"),          # MSH-23.1  (v2.7+)
        _s("sending_network_address"),            # MSH-24.1  (v2.7+)
        _s("receiving_network_address"),          # MSH-25.1  (v2.7+)
    ]
)

# ---------------------------------------------------------------------------
# PID — Patient Identification
# ---------------------------------------------------------------------------

PID_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id"),                             # PID-1
        _s("patient_id"),                         # PID-2  (deprecated v2.7)
        _s("patient_identifier_list"),            # PID-3  raw composite
        _s("patient_id_value"),                   # PID-3.1 (MRN / identifier)
        _s("patient_id_check_digit"),             # PID-3.2
        _s("patient_id_check_digit_scheme"),      # PID-3.3
        _s("patient_id_assigning_authority"),     # PID-3.4
        _s("patient_id_type_code"),               # PID-3.5
        _s("alternate_patient_id"),               # PID-4  (deprecated v2.7)
        _s("patient_name"),                       # PID-5  raw composite
        _s("patient_family_name"),                # PID-5.1
        _s("patient_given_name"),                 # PID-5.2
        _s("patient_middle_name"),                # PID-5.3
        _s("patient_name_suffix"),                # PID-5.5
        _s("patient_name_prefix"),                # PID-5.6
        _s("mothers_maiden_name"),                # PID-6.1
        _s("date_of_birth"),                      # PID-7
        _s("administrative_sex"),                 # PID-8
        _s("patient_alias"),                      # PID-9  (deprecated v2.7)
        _s("race"),                               # PID-10.1
        _s("patient_address"),                    # PID-11 raw composite
        _s("address_street"),                     # PID-11.1
        _s("address_other_designation"),          # PID-11.2
        _s("address_city"),                       # PID-11.3
        _s("address_state"),                      # PID-11.4
        _s("address_zip"),                        # PID-11.5
        _s("address_country"),                    # PID-11.6
        _s("address_type"),                       # PID-11.7
        _s("county_code"),                        # PID-12  (deprecated v2.6)
        _s("home_phone"),                         # PID-13
        _s("business_phone"),                     # PID-14
        _s("primary_language"),                   # PID-15.1
        _s("marital_status"),                     # PID-16.1
        _s("religion"),                           # PID-17.1
        _s("patient_account_number"),             # PID-18.1
        _s("ssn"),                                # PID-19  (deprecated v2.7)
        _s("drivers_license"),                    # PID-20  (deprecated v2.7)
        _s("mothers_identifier"),                 # PID-21.1
        _s("ethnic_group"),                       # PID-22.1
        _s("birth_place"),                        # PID-23
        _s("multiple_birth_indicator"),           # PID-24
        _i("birth_order"),                        # PID-25
        _s("citizenship"),                        # PID-26.1
        _s("veterans_military_status"),           # PID-27.1
        _s("nationality"),                        # PID-28.1  (deprecated v2.7)
        _s("patient_death_datetime"),             # PID-29
        _s("patient_death_indicator"),            # PID-30
        _s("identity_unknown_indicator"),         # PID-31  (v2.5+)
        _s("identity_reliability_code"),          # PID-32  (v2.5+)
        _s("last_update_datetime"),               # PID-33  (v2.5+)
        _s("last_update_facility"),               # PID-34.1  (v2.5+)
        _s("species_code"),                       # PID-35.1  (v2.5+, formerly taxonomic classification)
        _s("breed_code"),                         # PID-36.1  (v2.5+)
        _s("strain"),                             # PID-37  (v2.5+)
        _s("production_class_code"),              # PID-38.1  (v2.5+)
        _s("tribal_citizenship"),                 # PID-39.1  (v2.6+)
        _s("patient_telecommunication"),          # PID-40  (v2.7+)
    ]
)

# ---------------------------------------------------------------------------
# PV1 — Patient Visit
# ---------------------------------------------------------------------------

PV1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id"),                             # PV1-1
        _s("patient_class"),                      # PV1-2  (I/O/E/R/B/C/N/U)
        _s("assigned_patient_location"),          # PV1-3  raw composite
        _s("location_point_of_care"),             # PV1-3.1
        _s("location_room"),                      # PV1-3.2
        _s("location_bed"),                       # PV1-3.3
        _s("location_facility"),                  # PV1-3.4
        _s("location_status"),                    # PV1-3.5
        _s("location_type"),                      # PV1-3.9  (person location type)
        _s("admission_type"),                     # PV1-4
        _s("preadmit_number"),                    # PV1-5.1
        _s("prior_patient_location"),             # PV1-6
        _s("attending_doctor"),                   # PV1-7  raw composite
        _s("attending_doctor_id"),                # PV1-7.1
        _s("attending_doctor_family_name"),       # PV1-7.2
        _s("attending_doctor_given_name"),        # PV1-7.3
        _s("attending_doctor_prefix"),            # PV1-7.6  (title)
        _s("referring_doctor"),                   # PV1-8  raw composite
        _s("referring_doctor_id"),                # PV1-8.1
        _s("referring_doctor_family_name"),       # PV1-8.2
        _s("referring_doctor_given_name"),        # PV1-8.3
        _s("consulting_doctor"),                  # PV1-9  raw composite
        _s("hospital_service"),                   # PV1-10
        _s("temporary_location"),                 # PV1-11
        _s("preadmit_test_indicator"),            # PV1-12
        _s("readmission_indicator"),              # PV1-13
        _s("admit_source"),                       # PV1-14
        _s("ambulatory_status"),                  # PV1-15
        _s("vip_indicator"),                      # PV1-16
        _s("admitting_doctor"),                   # PV1-17  raw composite
        _s("admitting_doctor_id"),                # PV1-17.1
        _s("admitting_doctor_family_name"),       # PV1-17.2
        _s("admitting_doctor_given_name"),        # PV1-17.3
        _s("patient_type"),                       # PV1-18
        _s("visit_number"),                       # PV1-19.1
        _s("financial_class"),                    # PV1-20.1
        _s("charge_price_indicator"),             # PV1-21
        _s("courtesy_code"),                      # PV1-22
        _s("credit_rating"),                      # PV1-23
        _s("contract_code"),                      # PV1-24
        _s("contract_effective_date"),            # PV1-25
        _s("contract_amount"),                    # PV1-26
        _s("contract_period"),                    # PV1-27
        _s("interest_code"),                      # PV1-28
        _s("transfer_to_bad_debt_code"),          # PV1-29
        _s("transfer_to_bad_debt_date"),          # PV1-30
        _s("bad_debt_agency_code"),               # PV1-31
        _s("bad_debt_transfer_amount"),           # PV1-32
        _s("bad_debt_recovery_amount"),           # PV1-33
        _s("delete_account_indicator"),           # PV1-34
        _s("delete_account_date"),                # PV1-35
        _s("discharge_disposition"),              # PV1-36
        _s("discharged_to_location"),             # PV1-37.1
        _s("diet_type"),                          # PV1-38.1
        _s("servicing_facility"),                 # PV1-39
        _s("bed_status"),                         # PV1-40  (deprecated v2.6)
        _s("account_status"),                     # PV1-41
        _s("pending_location"),                   # PV1-42
        _s("prior_temporary_location"),           # PV1-43
        _s("admit_datetime"),                     # PV1-44
        _s("discharge_datetime"),                 # PV1-45
        _s("current_patient_balance"),            # PV1-46
        _s("total_charges"),                      # PV1-47
        _s("total_adjustments"),                  # PV1-48
        _s("total_payments"),                     # PV1-49
        _s("alternate_visit_id"),                 # PV1-50.1
        _s("visit_indicator"),                    # PV1-51
        _s("other_healthcare_provider"),          # PV1-52  (deprecated v2.7)
    ]
)

# ---------------------------------------------------------------------------
# OBR — Observation Request
# ---------------------------------------------------------------------------

OBR_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id"),                             # OBR-1
        _s("placer_order_number"),                # OBR-2.1
        _s("filler_order_number"),                # OBR-3.1
        _s("universal_service_identifier"),       # OBR-4  raw composite
        _s("service_id"),                         # OBR-4.1
        _s("service_text"),                       # OBR-4.2
        _s("service_coding_system"),              # OBR-4.3
        _s("priority"),                           # OBR-5  (deprecated v2.7)
        _s("requested_datetime"),                 # OBR-6  (deprecated v2.7)
        _s("observation_datetime"),               # OBR-7
        _s("observation_end_datetime"),           # OBR-8
        _s("collection_volume"),                  # OBR-9.1
        _s("collector_identifier"),               # OBR-10.1
        _s("specimen_action_code"),               # OBR-11
        _s("danger_code"),                        # OBR-12.1
        _s("relevant_clinical_information"),      # OBR-13
        _s("specimen_received_datetime"),         # OBR-14
        _s("specimen_source"),                    # OBR-15  (deprecated v2.7)
        _s("ordering_provider"),                  # OBR-16  raw composite
        _s("ordering_provider_id"),               # OBR-16.1
        _s("ordering_provider_family_name"),      # OBR-16.2
        _s("ordering_provider_given_name"),       # OBR-16.3
        _s("order_callback_phone"),               # OBR-17
        _s("placer_field_1"),                     # OBR-18
        _s("placer_field_2"),                     # OBR-19
        _s("filler_field_1"),                     # OBR-20
        _s("filler_field_2"),                     # OBR-21
        _s("results_rpt_status_chng_datetime"),   # OBR-22
        _s("charge_to_practice"),                 # OBR-23
        _s("diagnostic_service_section_id"),      # OBR-24
        _s("result_status"),                      # OBR-25
        _s("parent_result"),                      # OBR-26
        _s("quantity_timing"),                    # OBR-27  (deprecated v2.7)
        _s("result_copies_to"),                   # OBR-28.1
        _s("parent_placer_order_number"),         # OBR-29.1
        _s("transportation_mode"),                # OBR-30
        _s("reason_for_study"),                   # OBR-31.1
        _s("principal_result_interpreter"),       # OBR-32.1
        _s("assistant_result_interpreter"),       # OBR-33.1
        _s("technician"),                         # OBR-34.1
        _s("transcriptionist"),                   # OBR-35.1
        _s("scheduled_datetime"),                 # OBR-36
        _i("number_of_sample_containers"),        # OBR-37
        _s("transport_logistics"),                # OBR-38.1
        _s("collectors_comment"),                 # OBR-39.1
        _s("transport_arrangement_responsibility"),# OBR-40.1
        _s("transport_arranged"),                 # OBR-41
        _s("escort_required"),                    # OBR-42
        _s("planned_patient_transport_comment"),  # OBR-43.1
        _s("procedure_code"),                     # OBR-44.1  (v2.3+)
        _s("procedure_code_modifier"),            # OBR-45.1  (v2.3+)
        _s("placer_supplemental_service_info"),   # OBR-46.1  (v2.5+)
        _s("filler_supplemental_service_info"),   # OBR-47.1  (v2.5+)
        _s("medically_necessary_dup_proc_reason"),# OBR-48.1  (v2.5+)
        _s("result_handling"),                    # OBR-49.1  (v2.6+)
        _s("parent_universal_service_id"),        # OBR-50.1  (v2.7+)
    ]
)

# ---------------------------------------------------------------------------
# OBX — Observation Result
# ---------------------------------------------------------------------------

OBX_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id"),                             # OBX-1
        _s("value_type"),                         # OBX-2  (NM, ST, TX, CWE, TS, etc.)
        _s("observation_identifier"),             # OBX-3  raw composite (LOINC etc.)
        _s("observation_id"),                     # OBX-3.1
        _s("observation_text"),                   # OBX-3.2
        _s("observation_coding_system"),          # OBX-3.3
        _s("observation_alt_id"),                 # OBX-3.4
        _s("observation_alt_text"),               # OBX-3.5
        _s("observation_alt_coding_system"),      # OBX-3.6
        _s("observation_sub_id"),                 # OBX-4
        _s("observation_value"),                  # OBX-5  (raw; type depends on value_type)
        _s("units"),                              # OBX-6  raw composite
        _s("units_code"),                         # OBX-6.1
        _s("units_text"),                         # OBX-6.2
        _s("units_coding_system"),                # OBX-6.3
        _s("references_range"),                   # OBX-7
        _s("interpretation_codes"),               # OBX-8  (formerly abnormal_flags)
        _s("probability"),                        # OBX-9
        _s("nature_of_abnormal_test"),            # OBX-10
        _s("observation_result_status"),          # OBX-11  (F/P/C/X/etc.)
        _s("effective_date_of_ref_range"),        # OBX-12
        _s("user_defined_access_checks"),         # OBX-13
        _s("datetime_of_observation"),            # OBX-14
        _s("producers_id"),                       # OBX-15.1
        _s("responsible_observer"),               # OBX-16.1
        _s("observation_method"),                 # OBX-17.1
        _s("equipment_instance_identifier"),      # OBX-18.1  (v2.5+)
        _s("datetime_of_analysis"),               # OBX-19  (v2.5+)
        _s("observation_site"),                   # OBX-20.1  (v2.7+)
        _s("observation_instance_identifier"),    # OBX-21.1  (v2.7+)
        _s("mood_code"),                          # OBX-22.1  (v2.7+)
        _s("performing_organization_name"),       # OBX-23.1  (v2.7+)
        _s("performing_organization_address"),    # OBX-24  (v2.7+)
        _s("performing_org_medical_director"),    # OBX-25.1  (v2.7+)
        _s("patient_results_release_category"),   # OBX-26  (v2.8+)
        _s("root_cause"),                         # OBX-27.1  (v2.8+)
        _s("local_process_control"),              # OBX-28.1  (v2.8+)
    ]
)

# ---------------------------------------------------------------------------
# AL1 — Patient Allergy
# ---------------------------------------------------------------------------

AL1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id"),                             # AL1-1
        _s("allergen_type_code"),                 # AL1-2.1
        _s("allergen_code"),                      # AL1-3  raw composite
        _s("allergen_id"),                        # AL1-3.1
        _s("allergen_text"),                      # AL1-3.2
        _s("allergen_coding_system"),             # AL1-3.3
        _s("allergy_severity_code"),              # AL1-4.1
        _s("allergy_reaction_code"),              # AL1-5  (repeating; first value)
        _s("identification_date"),                # AL1-6  (deprecated v2.6)
    ]
)

# ---------------------------------------------------------------------------
# DG1 — Diagnosis
# ---------------------------------------------------------------------------

DG1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id"),                             # DG1-1
        _s("diagnosis_coding_method"),            # DG1-2  (deprecated v2.7)
        _s("diagnosis_code"),                     # DG1-3  raw composite (ICD code)
        _s("diagnosis_id"),                       # DG1-3.1
        _s("diagnosis_text"),                     # DG1-3.2
        _s("diagnosis_coding_system"),            # DG1-3.3
        _s("diagnosis_description"),              # DG1-4  (deprecated v2.7)
        _s("diagnosis_datetime"),                 # DG1-5
        _s("diagnosis_type"),                     # DG1-6  (A=admitting, W=working, F=final)
        _s("major_diagnostic_category"),          # DG1-7.1
        _s("diagnostic_related_group"),           # DG1-8.1
        _s("drg_approval_indicator"),             # DG1-9
        _s("drg_grouper_review_code"),            # DG1-10
        _s("outlier_type"),                       # DG1-11.1
        _i("outlier_days"),                       # DG1-12
        _s("outlier_cost"),                       # DG1-13
        _s("grouper_version_and_type"),           # DG1-14
        _i("diagnosis_priority"),                 # DG1-15
        _s("diagnosing_clinician"),               # DG1-16.1
        _s("diagnosis_classification"),           # DG1-17
        _s("confidential_indicator"),             # DG1-18
        _s("attestation_datetime"),               # DG1-19
        _s("diagnosis_identifier"),               # DG1-20.1  (v2.5+)
        _s("diagnosis_action_code"),              # DG1-21  (v2.5+)
        _s("parent_diagnosis"),                   # DG1-22.1  (v2.6+)
        _s("drg_ccl_value_code"),                 # DG1-23.1  (v2.6+)
        _s("drg_grouping_usage"),                 # DG1-24  (v2.7+)
        _s("drg_diagnosis_determination_status"), # DG1-25  (v2.7+)
        _s("present_on_admission_indicator"),     # DG1-26  (v2.7+)
    ]
)

# ---------------------------------------------------------------------------
# NK1 — Next of Kin / Associated Parties
# ---------------------------------------------------------------------------

NK1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id"),                             # NK1-1
        _s("name"),                               # NK1-2  raw composite
        _s("nk_family_name"),                     # NK1-2.1
        _s("nk_given_name"),                      # NK1-2.2
        _s("nk_middle_name"),                     # NK1-2.3
        _s("relationship"),                       # NK1-3  raw composite
        _s("relationship_code"),                  # NK1-3.1
        _s("relationship_text"),                  # NK1-3.2
        _s("address"),                            # NK1-4  raw composite
        _s("phone_number"),                       # NK1-5
        _s("business_phone"),                     # NK1-6
        _s("contact_role"),                       # NK1-7.1
        _s("start_date"),                         # NK1-8
        _s("end_date"),                           # NK1-9
        _s("job_title"),                          # NK1-10
        _s("job_code"),                           # NK1-11.1
        _s("employee_number"),                    # NK1-12.1
        _s("organization_name"),                  # NK1-13.1
        _s("marital_status"),                     # NK1-14.1
        _s("administrative_sex"),                 # NK1-15
        _s("date_of_birth"),                      # NK1-16
        _s("living_dependency"),                  # NK1-17.1
        _s("ambulatory_status"),                  # NK1-18.1
        _s("citizenship"),                        # NK1-19.1
        _s("primary_language"),                   # NK1-20.1
        _s("living_arrangement"),                 # NK1-21
        _s("publicity_code"),                     # NK1-22.1
        _s("protection_indicator"),               # NK1-23
        _s("student_indicator"),                  # NK1-24
        _s("religion"),                           # NK1-25.1
        _s("mothers_maiden_name"),                # NK1-26.1
        _s("nationality"),                        # NK1-27.1
        _s("ethnic_group"),                       # NK1-28.1
        _s("contact_reason"),                     # NK1-29.1
        _s("contact_person_name"),                # NK1-30.1
        _s("contact_person_telephone"),           # NK1-31
        _s("contact_persons_address"),            # NK1-32
        _s("associated_party_identifiers"),       # NK1-33.1
        _s("job_status"),                         # NK1-34
        _s("race"),                               # NK1-35.1
        _s("handicap"),                           # NK1-36
        _s("contact_ssn"),                        # NK1-37  (deprecated v2.7)
        _s("nk_birth_place"),                     # NK1-38  (v2.6+)
        _s("vip_indicator"),                      # NK1-39  (v2.6+)
    ]
)

# ---------------------------------------------------------------------------
# EVN — Event Type
# ---------------------------------------------------------------------------

EVN_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("event_type_code"),                    # EVN-1  (deprecated v2.5)
        _s("recorded_datetime"),                  # EVN-2
        _s("date_time_planned_event"),            # EVN-3
        _s("event_reason_code"),                  # EVN-4.1
        _s("operator_id"),                        # EVN-5.1
        _s("event_occurred"),                     # EVN-6
        _s("event_facility"),                     # EVN-7.1  (v2.5+)
    ]
)

# ---------------------------------------------------------------------------
# Generic segment schema — used for Z-segments and any unknown segments.
# Returns segment_type + up to 25 field_N columns.
# ---------------------------------------------------------------------------

_GENERIC_FIELD_COUNT = 25

GENERIC_SEGMENT_SCHEMA = StructType(
    _METADATA_FIELDS
    + [_s("segment_type")]
    + [_s(f"field_{i}") for i in range(1, _GENERIC_FIELD_COUNT + 1)]
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

#: Segment tables exposed by the connector by default.
SEGMENT_TABLES: list[str] = ["msh", "pid", "pv1", "obr", "obx", "al1", "dg1", "nk1", "evn"]

#: Map from lowercase segment name → StructType.
SEGMENT_SCHEMAS: dict[str, StructType] = {
    "msh": MSH_SCHEMA,
    "pid": PID_SCHEMA,
    "pv1": PV1_SCHEMA,
    "obr": OBR_SCHEMA,
    "obx": OBX_SCHEMA,
    "al1": AL1_SCHEMA,
    "dg1": DG1_SCHEMA,
    "nk1": NK1_SCHEMA,
    "evn": EVN_SCHEMA,
}


def get_schema(segment_type: str) -> StructType:
    """Return the StructType for *segment_type* (case-insensitive).

    Falls back to :data:`GENERIC_SEGMENT_SCHEMA` for unknown / Z-segments.
    """
    return SEGMENT_SCHEMAS.get(segment_type.lower(), GENERIC_SEGMENT_SCHEMA)
