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

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _s(name: str, comment: str = "") -> StructField:
    """Nullable StringType field with an optional Unity Catalog column comment."""
    meta = {"comment": comment} if comment else {}
    return StructField(name, StringType(), nullable=True, metadata=meta)


def _i(name: str, comment: str = "") -> StructField:
    """Nullable LongType field with an optional Unity Catalog column comment."""
    meta = {"comment": comment} if comment else {}
    return StructField(name, LongType(), nullable=True, metadata=meta)


def _ts(name: str, comment: str = "") -> StructField:
    """Nullable TimestampType field with an optional Unity Catalog column comment."""
    meta = {"comment": comment} if comment else {}
    return StructField(name, TimestampType(), nullable=True, metadata=meta)


def _pk_s(name: str, comment: str = "") -> StructField:
    """NOT NULL StringType field used as (part of) a primary key.

    NOT NULL is required for Unity Catalog PRIMARY KEY constraints and makes the
    column visible as a key column in the Catalog Explorer UI.
    """
    meta = {"comment": comment} if comment else {}
    return StructField(name, StringType(), nullable=False, metadata=meta)


def _pk_i(name: str, comment: str = "") -> StructField:
    """NOT NULL LongType field used as (part of) a composite primary key."""
    meta = {"comment": comment} if comment else {}
    return StructField(name, LongType(), nullable=False, metadata=meta)


# Fields present in every segment table.
_METADATA_FIELDS: list[StructField] = [
    _pk_s("message_id",     "Unique message identifier (MSH-10); primary join key across all segment tables"),
    _s("message_timestamp", "Message creation date/time (MSH-7) in HL7 DTM format, e.g. 20240101120000"),
    _s("hl7_version",       "HL7 version string (MSH-12), e.g. 2.5.1"),
    _s("source_file",       "Basename of the source .hl7 file for traceability"),
    _s("raw_segment",       "Raw pipe-delimited text of this HL7 segment for lossless recovery and debugging"),
]

# ---------------------------------------------------------------------------
# Table-level descriptions (surfaced via read_table_metadata)
# ---------------------------------------------------------------------------

TABLE_DESCRIPTIONS: dict[str, str] = {
    "msh": (
        "Message Header — present in every HL7 message. Contains routing information "
        "(sending/receiving application and facility), message type, trigger event, "
        "timestamp, and HL7 version. One row per message."
    ),
    "pid": (
        "Patient Identification — core patient demographics. Contains name, date of birth, "
        "sex, medical record number (MRN), address, phone numbers, and insurance account number. "
        "One row per message."
    ),
    "pv1": (
        "Patient Visit — encounter details. Contains patient class (inpatient/outpatient), "
        "bed location, attending and admitting physician, admit source, discharge disposition, "
        "and admit/discharge timestamps. One row per message."
    ),
    "obr": (
        "Observation Request — lab or radiology order. Contains the ordered test "
        "(universal service identifier), specimen details, ordering provider, and overall "
        "result status. One row per order; paired with OBX rows via message_id."
    ),
    "obx": (
        "Observation Result — individual clinical result. Contains a single lab value, "
        "vital sign, or coded observation including value, units, reference range, and "
        "abnormal flag. Multiple OBX rows per OBR (one per result component)."
    ),
    "al1": (
        "Patient Allergy — allergy or adverse reaction record. Contains allergen code and "
        "description, allergy type (drug/food/environmental), reaction, severity, and "
        "identification date. One row per allergy."
    ),
    "dg1": (
        "Diagnosis — ICD or SNOMED diagnosis with type (A=admitting, W=working, F=final) "
        "and optional DRG grouping details. Multiple DG1 rows per encounter are common. "
        "One row per diagnosis."
    ),
    "nk1": (
        "Next of Kin / Associated Parties — emergency contact or guarantor. Contains "
        "contact name, relationship to patient, address, phone number, and contact role. "
        "One row per associated party."
    ),
    "evn": (
        "Event Type — trigger event metadata. Contains the event type code, the date/time "
        "the event was recorded, the date/time it occurred, the event reason, and the "
        "operator who initiated the event. One row per message."
    ),
}

# ---------------------------------------------------------------------------
# MSH — Message Header
# ---------------------------------------------------------------------------

MSH_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("field_separator",                  "Field separator character (MSH-1); always '|' per the HL7 standard"),
        _s("encoding_characters",              "Encoding characters (MSH-2): component ^, repetition ~, escape \\, subcomponent &"),
        _s("sending_application",              "Name/identifier of the application that created the message (MSH-3.1)"),
        _s("sending_facility",                 "Facility that sent the message (MSH-4.1)"),
        _s("receiving_application",            "Name/identifier of the intended recipient application (MSH-5.1)"),
        _s("receiving_facility",               "Facility that will receive the message (MSH-6.1)"),
        _ts("message_datetime",                "Date/time the message was created (MSH-7); typed version of message_timestamp"),
        _s("security",                         "Security or access-restriction string (MSH-8); rarely populated"),
        _s("message_code",                     "Message code, first component of message type (MSH-9.1), e.g. ADT, ORU, ORM, ACK"),
        _s("trigger_event",                    "Trigger event, second component of message type (MSH-9.2), e.g. A01, A08, R01"),
        _s("message_structure",                "Message structure, third component of message type (MSH-9.3), e.g. ADT_A01, ORU_R01"),
        _s("message_control_id",               "Unique message control ID assigned by the sending application (MSH-10); same as message_id"),
        _s("processing_id",                    "Processing mode (MSH-11): P=Production, T=Training, D=Debugging"),
        _s("version_id",                       "HL7 version used for this message (MSH-12), e.g. 2.3, 2.5.1; same as hl7_version"),
        _i("sequence_number",                  "Optional sequence number for application-level message ordering (MSH-13)"),
        _s("continuation_pointer",             "Pointer used to continue a fragmented message (MSH-14); rarely populated"),
        _s("accept_acknowledgment_type",       "Conditions requiring an accept (transport-level) acknowledgment (MSH-15): AL, NE, SU, ER"),
        _s("application_acknowledgment_type",  "Conditions requiring an application-level acknowledgment (MSH-16): AL, NE, SU, ER"),
        _s("country_code",                     "ISO 3166 three-letter country code for the message (MSH-17)"),
        _s("character_set",                    "Character encoding used in the message (MSH-18), e.g. ASCII, UTF-8, 8859/1"),
        _s("principal_language",               "Primary language of the message content (MSH-19.1)"),
        _s("alt_character_set_handling",       "Alternate character set handling scheme (MSH-20)"),
        _s("message_profile_identifier",       "Conformance profile that constrains the message (MSH-21.1)"),
        _s("sending_responsible_org",          "Organization accountable for the sending application (MSH-22.1, v2.7+)"),
        _s("receiving_responsible_org",        "Organization accountable for the receiving application (MSH-23.1, v2.7+)"),
        _s("sending_network_address",          "Network address of the sending application (MSH-24.1, v2.7+)"),
        _s("receiving_network_address",        "Network address of the receiving application (MSH-25.1, v2.7+)"),
    ]
)

# ---------------------------------------------------------------------------
# PID — Patient Identification
# ---------------------------------------------------------------------------

PID_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id",                        "Sequence number when multiple PID segments appear in a message (PID-1)"),
        _s("patient_id",                    "External patient ID from a prior system (PID-2, deprecated in v2.7)"),
        _s("patient_identifier_list",       "Full composite list of patient identifiers (PID-3), raw; use patient_id_value for MRN"),
        _s("patient_id_value",              "Primary patient identifier value, typically the Medical Record Number (PID-3.1)"),
        _s("patient_id_check_digit",        "Check digit computed for the patient identifier (PID-3.2)"),
        _s("patient_id_check_digit_scheme", "Algorithm used to compute the check digit, e.g. M10, M11 (PID-3.3)"),
        _s("patient_id_assigning_authority","Facility or system that assigned the patient identifier (PID-3.4)"),
        _s("patient_id_type_code",          "Type of patient identifier, e.g. MR=Medical Record, PI=Patient Internal (PID-3.5)"),
        _s("alternate_patient_id",          "Alternate patient identifier from a prior system (PID-4, deprecated in v2.7)"),
        _s("patient_name",                  "Full patient name composite (PID-5), raw; use patient_family_name / patient_given_name"),
        _s("patient_family_name",           "Patient last/family name (PID-5.1)"),
        _s("patient_given_name",            "Patient first/given name (PID-5.2)"),
        _s("patient_middle_name",           "Patient middle name or initial (PID-5.3)"),
        _s("patient_name_suffix",           "Name suffix, e.g. Jr, Sr, III (PID-5.5)"),
        _s("patient_name_prefix",           "Name prefix/title, e.g. Dr, Mr, Ms (PID-5.6)"),
        _s("mothers_maiden_name",           "Mother's maiden family name (PID-6.1)"),
        _ts("date_of_birth",                "Date of birth parsed to timestamp (PID-7)"),
        _s("administrative_sex",            "Administrative gender code (PID-8): M=Male, F=Female, O=Other, U=Unknown"),
        _s("patient_alias",                 "Alias name(s) for the patient (PID-9, deprecated in v2.7)"),
        _s("race",                          "Race category code per HL7 table 0005 (PID-10.1)"),
        _s("patient_address",               "Full patient address composite (PID-11), raw; use address_street / address_city etc."),
        _s("address_street",                "Street address line 1 (PID-11.1)"),
        _s("address_other_designation",     "Street address line 2, apartment, or suite (PID-11.2)"),
        _s("address_city",                  "City or municipality (PID-11.3)"),
        _s("address_state",                 "State, province, or region (PID-11.4)"),
        _s("address_zip",                   "Postal or ZIP code (PID-11.5)"),
        _s("address_country",               "ISO 3166 country code (PID-11.6)"),
        _s("address_type",                  "Address type code (PID-11.7): H=Home, B=Business, M=Mailing, C=Current"),
        _s("county_code",                   "County or parish code (PID-12, deprecated in v2.6)"),
        _s("home_phone",                    "Home telephone number (PID-13)"),
        _s("business_phone",                "Business telephone number (PID-14)"),
        _s("primary_language",              "Patient's primary spoken language (PID-15.1)"),
        _s("marital_status",                "Marital status code (PID-16.1): S=Single, M=Married, D=Divorced, W=Widowed"),
        _s("religion",                      "Religion code (PID-17.1)"),
        _s("patient_account_number",        "Patient account number at the healthcare facility (PID-18.1)"),
        _s("ssn",                           "Social Security Number (PID-19, deprecated in v2.7)"),
        _s("drivers_license",               "Driver's license number and issuing state (PID-20, deprecated in v2.7)"),
        _s("mothers_identifier",            "Identifier for the patient's mother; used in neonatal records (PID-21.1)"),
        _s("ethnic_group",                  "Ethnic group code per HL7 table 0189 (PID-22.1)"),
        _s("birth_place",                   "Birthplace as free text (PID-23)"),
        _s("multiple_birth_indicator",      "Whether the patient is one of a multiple birth: Y or N (PID-24)"),
        _i("birth_order",                   "Birth sequence number for multiple-birth patients, e.g. 1, 2, 3 (PID-25)"),
        _s("citizenship",                   "Citizenship country code (PID-26.1)"),
        _s("veterans_military_status",      "Veteran or military service status (PID-27.1)"),
        _s("nationality",                   "Nationality code (PID-28.1, deprecated in v2.7)"),
        _ts("patient_death_datetime",       "Date/time of patient death parsed to timestamp (PID-29)"),
        _s("patient_death_indicator",       "Death indicator: Y=deceased, N=alive (PID-30)"),
        _s("identity_unknown_indicator",    "Whether the patient's identity is unknown: Y or N (PID-31, v2.5+)"),
        _s("identity_reliability_code",     "Code indicating reliability of patient identity, e.g. AL, UA (PID-32, v2.5+)"),
        _ts("last_update_datetime",         "Date/time the patient record was last updated, parsed to timestamp (PID-33, v2.5+)"),
        _s("last_update_facility",          "Facility where the last update occurred (PID-34.1, v2.5+)"),
        _s("species_code",                  "Species code for veterinary use (PID-35.1, v2.5+)"),
        _s("breed_code",                    "Breed code for veterinary use (PID-36.1, v2.5+)"),
        _s("strain",                        "Strain description for veterinary use (PID-37, v2.5+)"),
        _s("production_class_code",         "Production class code for veterinary/agricultural use (PID-38.1, v2.5+)"),
        _s("tribal_citizenship",            "Tribal citizenship or affiliation (PID-39.1, v2.6+)"),
        _s("patient_telecommunication",     "Patient telecommunication address, e.g. email or mobile (PID-40, v2.7+)"),
    ]
)

# ---------------------------------------------------------------------------
# PV1 — Patient Visit
# ---------------------------------------------------------------------------

PV1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id",                       "Sequence number when multiple PV1 segments appear (PV1-1)"),
        _s("patient_class",                "Patient classification (PV1-2): I=Inpatient, O=Outpatient, E=Emergency, R=Recurring, B=Obstetrics"),
        _s("assigned_patient_location",    "Full assigned bed location composite (PV1-3), raw; use location_* fields"),
        _s("location_point_of_care",       "Unit or nursing station, e.g. ICU, MED (PV1-3.1)"),
        _s("location_room",                "Room number within the unit (PV1-3.2)"),
        _s("location_bed",                 "Bed identifier within the room (PV1-3.3)"),
        _s("location_facility",            "Facility where the patient is located (PV1-3.4)"),
        _s("location_status",              "Bed status, e.g. C=Closed, H=Housekeeping, O=Occupied (PV1-3.5)"),
        _s("location_type",                "Person location type, e.g. N=Nursing Unit, C=Clinic (PV1-3.9)"),
        _s("admission_type",               "Admission type (PV1-4): A=Accident, E=Emergency, L=Labor, R=Routine, N=Newborn"),
        _s("preadmit_number",              "Pre-admission or reservation number (PV1-5.1)"),
        _s("prior_patient_location",       "Prior bed location before transfer (PV1-6), raw composite"),
        _s("attending_doctor",             "Attending physician composite (PV1-7), raw; use attending_doctor_* fields"),
        _s("attending_doctor_id",          "Attending physician identifier/NPI (PV1-7.1)"),
        _s("attending_doctor_family_name", "Attending physician last name (PV1-7.2)"),
        _s("attending_doctor_given_name",  "Attending physician first name (PV1-7.3)"),
        _s("attending_doctor_prefix",      "Attending physician prefix/title, e.g. Dr (PV1-7.6)"),
        _s("referring_doctor",             "Referring physician composite (PV1-8), raw; use referring_doctor_* fields"),
        _s("referring_doctor_id",          "Referring physician identifier/NPI (PV1-8.1)"),
        _s("referring_doctor_family_name", "Referring physician last name (PV1-8.2)"),
        _s("referring_doctor_given_name",  "Referring physician first name (PV1-8.3)"),
        _s("consulting_doctor",            "Consulting physician composite (PV1-9), raw; may repeat"),
        _s("hospital_service",             "Type of service the patient is under, e.g. MED, SUR, ORT (PV1-10)"),
        _s("temporary_location",           "Temporary bed/location during a transfer (PV1-11)"),
        _s("preadmit_test_indicator",      "Whether pre-admission testing was performed: Y or N (PV1-12)"),
        _s("readmission_indicator",        "Whether this is a readmission: R=Readmission (PV1-13)"),
        _s("admit_source",                 "Source of the admission, e.g. 1=Physician referral, 7=Emergency room (PV1-14)"),
        _s("ambulatory_status",            "Ambulatory status code, e.g. A1=No functional limitations (PV1-15)"),
        _s("vip_indicator",                "VIP flag for special patient handling (PV1-16)"),
        _s("admitting_doctor",             "Admitting physician composite (PV1-17), raw; use admitting_doctor_* fields"),
        _s("admitting_doctor_id",          "Admitting physician identifier/NPI (PV1-17.1)"),
        _s("admitting_doctor_family_name", "Admitting physician last name (PV1-17.2)"),
        _s("admitting_doctor_given_name",  "Admitting physician first name (PV1-17.3)"),
        _s("patient_type",                 "Site-defined patient type or classification (PV1-18)"),
        _s("visit_number",                 "Unique visit/encounter number (PV1-19.1)"),
        _s("financial_class",              "Financial class or payer category (PV1-20.1)"),
        _s("charge_price_indicator",       "Price indicator for charging purposes (PV1-21)"),
        _s("courtesy_code",                "Courtesy code for billing discounts (PV1-22)"),
        _s("credit_rating",                "Patient credit rating (PV1-23)"),
        _s("contract_code",                "Contract type code(s) (PV1-24)"),
        _s("contract_effective_date",      "Effective date of the contract (PV1-25)"),
        _s("contract_amount",              "Amount owed under the contract (PV1-26)"),
        _s("contract_period",              "Duration of the contract in days (PV1-27)"),
        _s("interest_code",                "Interest rate code for overdue accounts (PV1-28)"),
        _s("transfer_to_bad_debt_code",    "Code indicating transfer to bad debt (PV1-29)"),
        _s("transfer_to_bad_debt_date",    "Date the account was transferred to bad debt (PV1-30)"),
        _s("bad_debt_agency_code",         "Agency handling bad debt collection (PV1-31)"),
        _s("bad_debt_transfer_amount",     "Amount transferred to bad debt (PV1-32)"),
        _s("bad_debt_recovery_amount",     "Amount recovered from bad debt (PV1-33)"),
        _s("delete_account_indicator",     "Whether the account has been marked for deletion (PV1-34)"),
        _s("delete_account_date",          "Date the account was deleted (PV1-35)"),
        _s("discharge_disposition",        "Discharge disposition code (PV1-36): 01=Home, 02=SNF, 07=AMA, 20=Expired"),
        _s("discharged_to_location",       "Location to which the patient was discharged (PV1-37.1)"),
        _s("diet_type",                    "Diet type ordered at discharge (PV1-38.1)"),
        _s("servicing_facility",           "Facility providing the service (PV1-39)"),
        _s("bed_status",                   "Current bed status (PV1-40, deprecated in v2.6)"),
        _s("account_status",               "Account status code (PV1-41)"),
        _s("pending_location",             "Bed reserved for a pending admission or transfer (PV1-42)"),
        _s("prior_temporary_location",     "Prior temporary location before the current transfer (PV1-43)"),
        _ts("admit_datetime",              "Date/time of admission parsed to timestamp (PV1-44)"),
        _ts("discharge_datetime",          "Date/time of discharge parsed to timestamp (PV1-45)"),
        _s("current_patient_balance",      "Current outstanding patient balance (PV1-46)"),
        _s("total_charges",                "Total charges for the visit (PV1-47)"),
        _s("total_adjustments",            "Total adjustments applied to the visit charges (PV1-48)"),
        _s("total_payments",               "Total payments received for the visit (PV1-49)"),
        _s("alternate_visit_id",           "Alternate visit identifier (PV1-50.1)"),
        _s("visit_indicator",              "Visit indicator: V=Visit level, A=Account level (PV1-51)"),
        _s("other_healthcare_provider",    "Other healthcare provider(s) for the visit (PV1-52, deprecated in v2.7)"),
    ]
)

# ---------------------------------------------------------------------------
# OBR — Observation Request
# ---------------------------------------------------------------------------

OBR_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                           "Sequence number of this OBR within the message; part of composite primary key (OBR-1)"),
        _s("placer_order_number",                 "Order number assigned by the ordering application (OBR-2.1)"),
        _s("filler_order_number",                 "Order number assigned by the performing lab/radiology (OBR-3.1)"),
        _s("universal_service_identifier",        "Ordered test composite (OBR-4), raw; use service_id / service_text"),
        _s("service_id",                          "Coded test identifier, e.g. LOINC or CPT code (OBR-4.1)"),
        _s("service_text",                        "Human-readable test name, e.g. Basic Metabolic Panel (OBR-4.2)"),
        _s("service_coding_system",               "Coding system for the test code, e.g. LN=LOINC, CPT4 (OBR-4.3)"),
        _s("priority",                            "Order priority (OBR-5, deprecated in v2.7): R=Routine, S=STAT, A=ASAP"),
        _ts("requested_datetime",                 "Requested date/time for the observation, parsed to timestamp (OBR-6, deprecated in v2.7)"),
        _ts("observation_datetime",               "Date/time specimen was collected or observation started, parsed to timestamp (OBR-7)"),
        _ts("observation_end_datetime",           "Date/time observation ended or specimen collection completed, parsed to timestamp (OBR-8)"),
        _s("collection_volume",                   "Volume of specimen collected with units (OBR-9.1)"),
        _s("collector_identifier",                "Person who collected the specimen (OBR-10.1)"),
        _s("specimen_action_code",                "Action to take on the specimen (OBR-11): A=Add, G=Generated, L=Lab, O=Obtained"),
        _s("danger_code",                         "Code indicating a hazardous specimen (OBR-12.1)"),
        _s("relevant_clinical_information",       "Clinical information relevant to the order, e.g. patient condition (OBR-13)"),
        _ts("specimen_received_datetime",         "Date/time the specimen was received by the lab, parsed to timestamp (OBR-14)"),
        _s("specimen_source",                     "Specimen source and collection method (OBR-15, deprecated in v2.7)"),
        _s("ordering_provider",                   "Ordering physician composite (OBR-16), raw; use ordering_provider_* fields"),
        _s("ordering_provider_id",                "Ordering physician identifier/NPI (OBR-16.1)"),
        _s("ordering_provider_family_name",       "Ordering physician last name (OBR-16.2)"),
        _s("ordering_provider_given_name",        "Ordering physician first name (OBR-16.3)"),
        _s("order_callback_phone",                "Phone number to call with results (OBR-17)"),
        _s("placer_field_1",                      "Placer-defined field 1 for local use (OBR-18)"),
        _s("placer_field_2",                      "Placer-defined field 2 for local use (OBR-19)"),
        _s("filler_field_1",                      "Filler-defined field 1 for local use (OBR-20)"),
        _s("filler_field_2",                      "Filler-defined field 2 for local use (OBR-21)"),
        _ts("results_rpt_status_chng_datetime",   "Date/time the result status last changed, parsed to timestamp (OBR-22)"),
        _s("charge_to_practice",                  "Charge information for billing purposes (OBR-23)"),
        _s("diagnostic_service_section_id",       "Lab section performing the test, e.g. HM=Hematology, CH=Chemistry (OBR-24)"),
        _s("result_status",                       "Overall result status (OBR-25): F=Final, P=Preliminary, C=Corrected, X=Canceled"),
        _s("parent_result",                       "Reference to the parent result for reflex tests (OBR-26)"),
        _s("quantity_timing",                     "Quantity and timing of the order (OBR-27, deprecated in v2.7)"),
        _s("result_copies_to",                    "Provider to receive a copy of the result (OBR-28.1)"),
        _s("parent_placer_order_number",          "Placer order number of the parent order for reflex tests (OBR-29.1)"),
        _s("transportation_mode",                 "How the specimen is to be transported to the lab (OBR-30)"),
        _s("reason_for_study",                    "Clinical reason or indication for the order (OBR-31.1)"),
        _s("principal_result_interpreter",        "Provider who interpreted the result (OBR-32.1)"),
        _s("assistant_result_interpreter",        "Assistant provider who helped interpret the result (OBR-33.1)"),
        _s("technician",                          "Technician who performed the test (OBR-34.1)"),
        _s("transcriptionist",                    "Person who transcribed the result (OBR-35.1)"),
        _ts("scheduled_datetime",                 "Scheduled date/time for the observation, parsed to timestamp (OBR-36)"),
        _i("number_of_sample_containers",         "Number of specimen containers required (OBR-37)"),
        _s("transport_logistics",                 "Special instructions for specimen transport (OBR-38.1)"),
        _s("collectors_comment",                  "Comments from the specimen collector (OBR-39.1)"),
        _s("transport_arrangement_responsibility","Party responsible for arranging specimen transport (OBR-40.1)"),
        _s("transport_arranged",                  "Whether transport has been arranged: A=Arranged, N=Not arranged (OBR-41)"),
        _s("escort_required",                     "Whether an escort is required for specimen transport: R=Required (OBR-42)"),
        _s("planned_patient_transport_comment",   "Comments about planned patient transport (OBR-43.1)"),
        _s("procedure_code",                      "Procedure code for the observation, e.g. CPT (OBR-44.1, v2.3+)"),
        _s("procedure_code_modifier",             "Modifier to the procedure code (OBR-45.1, v2.3+)"),
        _s("placer_supplemental_service_info",    "Additional service information from the placer (OBR-46.1, v2.5+)"),
        _s("filler_supplemental_service_info",    "Additional service information from the filler (OBR-47.1, v2.5+)"),
        _s("medically_necessary_dup_proc_reason", "Reason a duplicate procedure is medically necessary (OBR-48.1, v2.5+)"),
        _s("result_handling",                     "How the result should be handled, e.g. F=Film-with-patient (OBR-49.1, v2.6+)"),
        _s("parent_universal_service_id",         "Universal service ID of the parent order (OBR-50.1, v2.7+)"),
    ]
)

# ---------------------------------------------------------------------------
# OBX — Observation Result
# ---------------------------------------------------------------------------

OBX_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                       "Sequence number of this OBX within the message; part of composite primary key (OBX-1)"),
        _s("value_type",                      "Data type of the observation value (OBX-2): NM=Numeric, ST=String, CWE=Coded, TX=Text, TS=Timestamp"),
        _s("observation_identifier",          "Full observation code composite (OBX-3), raw; use observation_id / observation_text"),
        _s("observation_id",                  "Coded observation identifier, e.g. LOINC code 2951-2 for Sodium (OBX-3.1)"),
        _s("observation_text",                "Human-readable observation name, e.g. Sodium (OBX-3.2)"),
        _s("observation_coding_system",       "Coding system for the observation, e.g. LN=LOINC, SNOMED, LOCAL (OBX-3.3)"),
        _s("observation_alt_id",              "Alternate observation code from a secondary coding system (OBX-3.4)"),
        _s("observation_alt_text",            "Alternate observation name from the secondary coding system (OBX-3.5)"),
        _s("observation_alt_coding_system",   "Secondary coding system name (OBX-3.6)"),
        _s("observation_sub_id",              "Sub-identifier to group related OBX rows, e.g. for waveform or panel data (OBX-4)"),
        _s("observation_value",               "The result value; interpret according to value_type (OBX-5), e.g. 138 for Sodium NM"),
        _s("units",                           "Units of measure composite (OBX-6), raw; use units_code / units_text"),
        _s("units_code",                      "Coded units, e.g. UCUM code mEq/L (OBX-6.1)"),
        _s("units_text",                      "Human-readable units description, e.g. milliequivalents per liter (OBX-6.2)"),
        _s("units_coding_system",             "Coding system for units, e.g. UCUM, ISO+ (OBX-6.3)"),
        _s("references_range",                "Normal or reference range for the result, e.g. 136-145 (OBX-7)"),
        _s("interpretation_codes",            "Abnormality flag (OBX-8): N=Normal, H=High, L=Low, A=Abnormal, C=Critical"),
        _s("probability",                     "Probability of the observation being correct, 0-1 scale (OBX-9)"),
        _s("nature_of_abnormal_test",         "What the reference range is based on: A=Age, S=Sex, R=Race (OBX-10)"),
        _s("observation_result_status",       "Result status (OBX-11): F=Final, P=Preliminary, C=Corrected, X=Deleted, R=Not yet verified"),
        _ts("effective_date_of_ref_range",    "Date the reference range became effective, parsed to timestamp (OBX-12)"),
        _s("user_defined_access_checks",      "Site-defined access control value (OBX-13)"),
        _ts("datetime_of_observation",        "Date/time this specific observation was made, parsed to timestamp (OBX-14)"),
        _s("producers_id",                    "Lab or system that produced the result (OBX-15.1)"),
        _s("responsible_observer",            "Clinician responsible for verifying the result (OBX-16.1)"),
        _s("observation_method",              "Method used to perform the observation, e.g. LOINC method code (OBX-17.1)"),
        _s("equipment_instance_identifier",   "Analyzer or instrument that generated the result (OBX-18.1, v2.5+)"),
        _ts("datetime_of_analysis",           "Date/time the specimen was analyzed on the instrument, parsed to timestamp (OBX-19, v2.5+)"),
        _s("observation_site",                "Body site where the observation was performed, e.g. LA=Left arm (OBX-20.1, v2.7+)"),
        _s("observation_instance_identifier", "Unique instance identifier for this observation event (OBX-21.1, v2.7+)"),
        _s("mood_code",                       "Mood of the observation: EVN=Event (actual result), INT=Intent (ordered) (OBX-22.1, v2.7+)"),
        _s("performing_organization_name",    "Name of the organization that performed the test (OBX-23.1, v2.7+)"),
        _s("performing_organization_address", "Address of the performing organization (OBX-24, v2.7+)"),
        _s("performing_org_medical_director", "Medical director of the performing organization (OBX-25.1, v2.7+)"),
        _s("patient_results_release_category","Category controlling release of results to the patient (OBX-26, v2.8+)"),
        _s("root_cause",                      "Root cause for a corrected or amended result (OBX-27.1, v2.8+)"),
        _s("local_process_control",           "Site-defined codes for local processing workflow (OBX-28.1, v2.8+)"),
    ]
)

# ---------------------------------------------------------------------------
# AL1 — Patient Allergy
# ---------------------------------------------------------------------------

AL1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",               "Sequence number of this allergy within the message; part of composite primary key (AL1-1)"),
        _s("allergen_type_code",      "Allergy category (AL1-2.1): DA=Drug, FA=Food, EA=Environmental, MA=Miscellaneous"),
        _s("allergen_code",           "Full allergen composite (AL1-3), raw; use allergen_id / allergen_text"),
        _s("allergen_id",             "Coded allergen identifier, e.g. RxNorm or local drug code (AL1-3.1)"),
        _s("allergen_text",           "Human-readable allergen name, e.g. Penicillin G (AL1-3.2)"),
        _s("allergen_coding_system",  "Coding system for the allergen code, e.g. RXNORM, LOCAL (AL1-3.3)"),
        _s("allergy_severity_code",   "Severity of the allergic reaction (AL1-4.1): SV=Severe, MO=Moderate, MI=Minor, U=Unknown"),
        _s("allergy_reaction_code",   "Clinical manifestation of the reaction, e.g. HIVES, ANAPHYLAXIS, RASH (AL1-5)"),
        _ts("identification_date",    "Date the allergy was first identified or recorded, parsed to timestamp (AL1-6, deprecated in v2.6)"),
    ]
)

# ---------------------------------------------------------------------------
# DG1 — Diagnosis
# ---------------------------------------------------------------------------

DG1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                            "Sequence number of this diagnosis within the message; part of composite primary key (DG1-1)"),
        _s("diagnosis_coding_method",              "Diagnosis coding method (DG1-2, deprecated in v2.7); use diagnosis_coding_system"),
        _s("diagnosis_code",                       "Full diagnosis code composite (DG1-3), raw; use diagnosis_id / diagnosis_text"),
        _s("diagnosis_id",                         "Coded diagnosis identifier, e.g. ICD-10-CM code J18.9 (DG1-3.1)"),
        _s("diagnosis_text",                       "Human-readable diagnosis description, e.g. Pneumonia, unspecified (DG1-3.2)"),
        _s("diagnosis_coding_system",              "Coding system used, e.g. ICD10CM, ICD9CM, SNOMED (DG1-3.3)"),
        _s("diagnosis_description",                "Free-text diagnosis description (DG1-4, deprecated in v2.7)"),
        _ts("diagnosis_datetime",                  "Date/time the diagnosis was established, parsed to timestamp (DG1-5)"),
        _s("diagnosis_type",                       "Diagnosis type (DG1-6): A=Admitting, W=Working, F=Final"),
        _s("major_diagnostic_category",            "CMS Major Diagnostic Category (MDC) code used for DRG grouping (DG1-7.1)"),
        _s("diagnostic_related_group",             "DRG code assigned by the grouper software (DG1-8.1)"),
        _s("drg_approval_indicator",               "Whether the DRG assignment was approved: Y or N (DG1-9)"),
        _s("drg_grouper_review_code",              "Review code returned by the DRG grouper (DG1-10)"),
        _s("outlier_type",                         "Type of cost or length-of-stay outlier (DG1-11.1)"),
        _i("outlier_days",                         "Number of outlier days beyond the DRG length-of-stay threshold (DG1-12)"),
        _s("outlier_cost",                         "Outlier cost amount beyond the DRG cost threshold (DG1-13)"),
        _s("grouper_version_and_type",             "Version and type of the DRG grouper software (DG1-14)"),
        _i("diagnosis_priority",                   "Priority rank of this diagnosis; 1=principal diagnosis (DG1-15)"),
        _s("diagnosing_clinician",                 "Clinician who established the diagnosis (DG1-16.1)"),
        _s("diagnosis_classification",             "Classification of the diagnosis: C=Chronic, A=Acute (DG1-17)"),
        _s("confidential_indicator",               "Whether the diagnosis is confidential and access-restricted: Y or N (DG1-18)"),
        _ts("attestation_datetime",                "Date/time the diagnosis was attested by the physician, parsed to timestamp (DG1-19)"),
        _s("diagnosis_identifier",                 "Unique instance identifier for this diagnosis record (DG1-20.1, v2.5+)"),
        _s("diagnosis_action_code",                "Action to take on this diagnosis: A=Add, U=Update, D=Delete (DG1-21, v2.5+)"),
        _s("parent_diagnosis",                     "Parent diagnosis code for hierarchical grouping (DG1-22.1, v2.6+)"),
        _s("drg_ccl_value_code",                   "Complication/Comorbidity Level value for DRG assignment (DG1-23.1, v2.6+)"),
        _s("drg_grouping_usage",                   "Whether this diagnosis was used in DRG grouping: Y or N (DG1-24, v2.7+)"),
        _s("drg_diagnosis_determination_status",   "Status of this diagnosis in the DRG determination process (DG1-25, v2.7+)"),
        _s("present_on_admission_indicator",       "Whether diagnosis was present on admission: Y=Yes, N=No, U=Unknown, W=Clinically undetermined (DG1-26, v2.7+)"),
    ]
)

# ---------------------------------------------------------------------------
# NK1 — Next of Kin / Associated Parties
# ---------------------------------------------------------------------------

NK1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                  "Sequence number of this next-of-kin record within the message; part of composite primary key (NK1-1)"),
        _s("name",                       "Full name composite of the next of kin (NK1-2), raw; use nk_family_name / nk_given_name"),
        _s("nk_family_name",             "Next of kin last/family name (NK1-2.1)"),
        _s("nk_given_name",              "Next of kin first/given name (NK1-2.2)"),
        _s("nk_middle_name",             "Next of kin middle name or initial (NK1-2.3)"),
        _s("relationship",               "Relationship to patient composite (NK1-3), raw; use relationship_code / relationship_text"),
        _s("relationship_code",          "Coded relationship to patient (NK1-3.1): SPO=Spouse, MTH=Mother, FTH=Father, CHD=Child, GRD=Guardian"),
        _s("relationship_text",          "Human-readable relationship description (NK1-3.2)"),
        _s("address",                    "Next of kin address composite (NK1-4), raw"),
        _s("phone_number",               "Next of kin home or primary telephone number (NK1-5)"),
        _s("business_phone",             "Next of kin business telephone number (NK1-6)"),
        _s("contact_role",               "Role of this contact (NK1-7.1): EC=Emergency Contact, C=Guarantor, N=Next of kin"),
        _ts("start_date",                "Date this contact relationship became effective, parsed to timestamp (NK1-8)"),
        _ts("end_date",                  "Date this contact relationship ended, parsed to timestamp (NK1-9)"),
        _s("job_title",                  "Next of kin job title or occupation (NK1-10)"),
        _s("job_code",                   "Occupation code for the next of kin (NK1-11.1)"),
        _s("employee_number",            "Employee number of the next of kin (NK1-12.1)"),
        _s("organization_name",          "Organization where the next of kin is employed (NK1-13.1)"),
        _s("marital_status",             "Marital status of the next of kin (NK1-14.1)"),
        _s("administrative_sex",         "Administrative gender of the next of kin: M, F, O, U (NK1-15)"),
        _ts("date_of_birth",             "Date of birth of the next of kin, parsed to timestamp (NK1-16)"),
        _s("living_dependency",          "Living dependency code for the next of kin (NK1-17.1)"),
        _s("ambulatory_status",          "Ambulatory status code of the next of kin (NK1-18.1)"),
        _s("citizenship",                "Citizenship country code of the next of kin (NK1-19.1)"),
        _s("primary_language",           "Primary spoken language of the next of kin (NK1-20.1)"),
        _s("living_arrangement",         "Living arrangement code (NK1-21): A=Alone, F=Family, I=Institution, R=Relative"),
        _s("publicity_code",             "Consent to contact or publicity level (NK1-22.1)"),
        _s("protection_indicator",       "Whether to restrict sharing of this contact's information: Y or N (NK1-23)"),
        _s("student_indicator",          "Student status of the next of kin: F=Full-time, P=Part-time (NK1-24)"),
        _s("religion",                   "Religion code of the next of kin (NK1-25.1)"),
        _s("mothers_maiden_name",        "Mother's maiden family name of the next of kin (NK1-26.1)"),
        _s("nationality",                "Nationality of the next of kin (NK1-27.1)"),
        _s("ethnic_group",               "Ethnic group code of the next of kin (NK1-28.1)"),
        _s("contact_reason",             "Reason this person is listed as a contact (NK1-29.1)"),
        _s("contact_person_name",        "Name of the contact person if different from the next of kin (NK1-30.1)"),
        _s("contact_person_telephone",   "Telephone number of the contact person (NK1-31)"),
        _s("contact_persons_address",    "Address of the contact person (NK1-32)"),
        _s("associated_party_identifiers","Identifier for the associated party, e.g. employee ID (NK1-33.1)"),
        _s("job_status",                 "Employment status of the next of kin (NK1-34)"),
        _s("race",                       "Race code of the next of kin (NK1-35.1)"),
        _s("handicap",                   "Handicap code indicating a physical or mental disability (NK1-36)"),
        _s("contact_ssn",                "Social Security Number of the contact person (NK1-37, deprecated in v2.7)"),
        _s("nk_birth_place",             "Birth place of the next of kin (NK1-38, v2.6+)"),
        _s("vip_indicator",              "VIP flag for the next of kin (NK1-39, v2.6+)"),
    ]
)

# ---------------------------------------------------------------------------
# EVN — Event Type
# ---------------------------------------------------------------------------

EVN_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("event_type_code",          "Event type code (EVN-1, deprecated in v2.5); superseded by MSH-9 trigger event"),
        _ts("recorded_datetime",       "Date/time the event was recorded in the sending system, parsed to timestamp (EVN-2)"),
        _ts("date_time_planned_event", "Date/time the event was planned to occur, parsed to timestamp (EVN-3)"),
        _s("event_reason_code",        "Coded reason the event occurred (EVN-4.1): 01=Patient request, 02=Physician order, 03=Census management"),
        _s("operator_id",              "Identifier of the person who initiated or recorded the event (EVN-5.1)"),
        _ts("event_occurred",          "Actual date/time the event occurred, parsed to timestamp (EVN-6)"),
        _s("event_facility",           "Facility where the event took place (EVN-7.1, v2.5+)"),
    ]
)

# ---------------------------------------------------------------------------
# Generic segment schema — used for Z-segments and any unknown segments.
# Returns segment_type + up to 25 field_N columns.
# ---------------------------------------------------------------------------

_GENERIC_FIELD_COUNT = 25

GENERIC_SEGMENT_SCHEMA = StructType(
    _METADATA_FIELDS
    + [_s("segment_type", "HL7 segment type identifier, e.g. ZPD for a custom Z-segment")]
    + [
        _s(f"field_{i}", f"Raw value of field {i} in the segment (1-based HL7 field index)")
        for i in range(1, _GENERIC_FIELD_COUNT + 1)
    ]
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
