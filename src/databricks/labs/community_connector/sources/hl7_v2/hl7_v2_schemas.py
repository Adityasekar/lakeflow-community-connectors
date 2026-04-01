"""Spark StructType schemas for HL7 v2 segment tables.

Every schema follows the HL7 v2.9 specification — the latest version, which
is itself a superset of all prior versions (v2.1–v2.8).  Fields absent in a
given message version are returned as ``None`` by the connector, and Spark
receives them as ``null``.

All wire-format values are strings; only ``set_id`` / sequence-number
fields are typed as ``LongType`` because they are guaranteed-numeric.

Every table includes six common metadata columns:
    message_id        — MSH-10 (join key across all segment tables)
    message_timestamp — MSH-7  (raw HL7 DTM string, cursor field)
    hl7_version       — MSH-12 (e.g. "2.5.1")
    source_file       — GCP Healthcare API resource name of the source message
    send_time         — RFC3339 sendTime from the API (incremental cursor)
    raw_segment       — raw pipe-delimited segment text for lossless recovery
"""

from pyspark.sql.types import LongType, StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _s(name: str, comment: str = "") -> StructField:
    """Nullable StringType field.

    The *comment* parameter is used for inline documentation only and is NOT
    stored in StructField metadata.  Arrow metadata mismatches between the JVM
    and Python data-source workers cause ARROW_TYPE_MISMATCH errors when
    StructField metadata is present.
    """
    return StructField(name, StringType(), nullable=True)


def _i(name: str, comment: str = "") -> StructField:
    """Nullable LongType field (see ``_s`` for metadata rationale)."""
    return StructField(name, LongType(), nullable=True)


def _ts(name: str, comment: str = "") -> StructField:
    """Nullable StringType field for ISO-8601 timestamps.

    Uses StringType to avoid Arrow timestamp-timezone mismatches.
    Downstream consumers can CAST to TIMESTAMP in SQL when needed.
    """
    return StructField(name, StringType(), nullable=True)


def _pk_s(name: str, comment: str = "") -> StructField:
    """NOT NULL StringType field used as (part of) a primary key.

    NOT NULL is required for Unity Catalog PRIMARY KEY constraints.
    """
    return StructField(name, StringType(), nullable=False)


def _pk_i(name: str, comment: str = "") -> StructField:
    """NOT NULL LongType field used as (part of) a composite primary key."""
    return StructField(name, LongType(), nullable=False)


# Fields present in every segment table.
_METADATA_FIELDS: list[StructField] = [
    _pk_s("message_id",     "Unique message identifier (MSH-10); primary join key across all segment tables"),
    _s("message_timestamp", "Message creation date/time (MSH-7) in HL7 DTM format, e.g. 20240101120000"),
    _s("hl7_version",       "HL7 version string (MSH-12), e.g. 2.5.1"),
    _s("source_file",       "API resource name of the source HL7 message for traceability"),
    _s("send_time",         "Message send time from the GCP Healthcare API in RFC3339 format; used as incremental cursor"),
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
    "evn": (
        "Event Type — trigger event metadata. Contains the event type code, the date/time "
        "the event was recorded, the date/time it occurred, the event reason, and the "
        "operator who initiated the event. One row per message."
    ),
    "pid": (
        "Patient Identification — core patient demographics. Contains name, date of birth, "
        "sex, medical record number (MRN), address, phone numbers, and insurance account number. "
        "One row per message."
    ),
    "pd1": (
        "Patient Additional Demographic — supplementary patient data including living will, "
        "organ donor status, primary care facility, student indicator, and military information. "
        "One row per message."
    ),
    "pv1": (
        "Patient Visit — encounter details. Contains patient class (inpatient/outpatient), "
        "bed location, attending and admitting physician, admit source, discharge disposition, "
        "and admit/discharge timestamps. One row per message."
    ),
    "pv2": (
        "Patient Visit Additional — extended visit details including admit reason, expected "
        "dates (admit/discharge/surgery), mode of arrival, patient condition, and visit priority. "
        "One row per message."
    ),
    "nk1": (
        "Next of Kin / Associated Parties — emergency contact or guarantor. Contains "
        "contact name, relationship to patient, address, phone number, and contact role. "
        "One row per associated party."
    ),
    "mrg": (
        "Merge Patient Information — prior patient identifiers used in merge/link/unlink events. "
        "Contains prior MRN, account number, visit number, and patient name. One row per message."
    ),
    "al1": (
        "Patient Allergy — allergy or adverse reaction record. Contains allergen code and "
        "description, allergy type (drug/food/environmental), reaction, severity, and "
        "identification date. One row per allergy."
    ),
    "iam": (
        "Patient Adverse Reaction Information — action-code based allergy tracking (newer "
        "replacement for AL1). Contains allergen, severity, reaction, action code (add/delete/update), "
        "unique identifier, and clinical status. One row per adverse reaction."
    ),
    "dg1": (
        "Diagnosis — ICD or SNOMED diagnosis with type (A=admitting, W=working, F=final) "
        "and optional DRG grouping details. Multiple DG1 rows per encounter are common. "
        "One row per diagnosis."
    ),
    "pr1": (
        "Procedures — surgical, diagnostic, and therapeutic procedures. Contains procedure code "
        "(CPT/ICD), date/time, duration, anesthesia details, and associated diagnosis. "
        "One row per procedure."
    ),
    "orc": (
        "Common Order — order control and status information. Contains order control code "
        "(new/cancel/change), placer and filler order numbers, ordering provider, order status, "
        "and transaction date/time. One row per order."
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
    "nte": (
        "Notes and Comments — free-text annotations attached to orders, results, or other "
        "segments. Contains the comment source, text content, and type. One row per comment."
    ),
    "spm": (
        "Specimen — specimen type, collection details, handling instructions, and condition. "
        "Contains specimen identifier, type, source site, collection method, and date/time. "
        "One row per specimen."
    ),
    "in1": (
        "Insurance — policy coverage and billing information. Contains insurance plan, company "
        "name and ID, group number, policy number, insured person details, and plan effective "
        "dates. One row per insurance plan."
    ),
    "gt1": (
        "Guarantor — financially responsible party. Contains guarantor name, address, phone, "
        "relationship to patient, employer information, and financial class. "
        "One row per guarantor."
    ),
    "ft1": (
        "Financial Transaction — charges, payments, and adjustments. Contains transaction "
        "type (charge/credit/payment), code, amount, date, performing provider, and associated "
        "diagnosis and procedure codes. One row per transaction."
    ),
    "rxa": (
        "Pharmacy/Treatment Administration — medication administration records. Contains "
        "drug/vaccine code, amount, units, administration date/time, provider, lot number, "
        "and completion status. One row per administration."
    ),
    "sch": (
        "Scheduling Activity Information — appointment details. Contains placer and filler "
        "appointment IDs, event reason, appointment type, duration, and contact information "
        "for placer and filler. One row per scheduling message."
    ),
    "txa": (
        "Transcription Document Header — document metadata and status. Contains document type, "
        "unique document number, completion status (dictated/authenticated), originator, "
        "transcriptionist, and authentication details. One row per document message."
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
        _s("sending_application",                      "Namespace ID of the sending application (MSH-3.1)"),
        _s("sending_application_universal_id",         "Universal ID (e.g. OID) of the sending application (MSH-3.2)"),
        _s("sending_application_universal_id_type",    "Type of universal ID for the sending application, e.g. ISO (MSH-3.3)"),
        _s("sending_facility",                         "Namespace ID of the sending facility (MSH-4.1)"),
        _s("sending_facility_universal_id",            "Universal ID (e.g. OID) of the sending facility (MSH-4.2)"),
        _s("sending_facility_universal_id_type",       "Type of universal ID for the sending facility, e.g. ISO (MSH-4.3)"),
        _s("receiving_application",                    "Namespace ID of the receiving application (MSH-5.1)"),
        _s("receiving_application_universal_id",       "Universal ID (e.g. OID) of the receiving application (MSH-5.2)"),
        _s("receiving_application_universal_id_type",  "Type of universal ID for the receiving application, e.g. ISO (MSH-5.3)"),
        _s("receiving_facility",                       "Namespace ID of the receiving facility (MSH-6.1)"),
        _s("receiving_facility_universal_id",          "Universal ID (e.g. OID) of the receiving facility (MSH-6.2)"),
        _s("receiving_facility_universal_id_type",     "Type of universal ID for the receiving facility, e.g. ISO (MSH-6.3)"),
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
        _s("message_profile_identifier",       "Conformance profile entity identifier (MSH-21.1)"),
        _s("message_profile_namespace_id",     "Namespace ID of the conformance profile (MSH-21.2)"),
        _s("message_profile_universal_id",     "Universal ID (e.g. OID) of the conformance profile (MSH-21.3)"),
        _s("message_profile_universal_id_type","Type of universal ID for the profile, e.g. ISO (MSH-21.4)"),
        _s("sending_responsible_org",                              "Organization name (MSH-22.1, XON, v2.7+)"),
        _s("sending_responsible_org_type_code",                    "Organization name type code (MSH-22.2, CWE)"),
        _s("sending_responsible_org_id",                           "ID number (MSH-22.3)"),
        _s("sending_responsible_org_check_digit",                  "Identifier check digit (MSH-22.4)"),
        _s("sending_responsible_org_check_digit_scheme",           "Check digit scheme (MSH-22.5)"),
        _s("sending_responsible_org_assigning_authority",          "Assigning authority (MSH-22.6, HD)"),
        _s("sending_responsible_org_id_type_code",                 "Identifier type code (MSH-22.7)"),
        _s("sending_responsible_org_assigning_facility",           "Assigning facility (MSH-22.8, HD)"),
        _s("sending_responsible_org_name_rep_code",                "Name representation code (MSH-22.9)"),
        _s("sending_responsible_org_identifier",                   "Organization identifier (MSH-22.10)"),
        _s("receiving_responsible_org",                            "Organization name (MSH-23.1, XON, v2.7+)"),
        _s("receiving_responsible_org_type_code",                  "Organization name type code (MSH-23.2, CWE)"),
        _s("receiving_responsible_org_id",                         "ID number (MSH-23.3)"),
        _s("receiving_responsible_org_check_digit",                "Identifier check digit (MSH-23.4)"),
        _s("receiving_responsible_org_check_digit_scheme",         "Check digit scheme (MSH-23.5)"),
        _s("receiving_responsible_org_assigning_authority",        "Assigning authority (MSH-23.6, HD)"),
        _s("receiving_responsible_org_id_type_code",               "Identifier type code (MSH-23.7)"),
        _s("receiving_responsible_org_assigning_facility",         "Assigning facility (MSH-23.8, HD)"),
        _s("receiving_responsible_org_name_rep_code",              "Name representation code (MSH-23.9)"),
        _s("receiving_responsible_org_identifier",                 "Organization identifier (MSH-23.10)"),
        _s("sending_network_address",                              "Namespace ID of the sending network address (MSH-24.1, HD, v2.7+)"),
        _s("sending_network_address_universal_id",                 "Universal ID of the sending network address (MSH-24.2)"),
        _s("sending_network_address_universal_id_type",            "Type of universal ID for sending network, e.g. ISO (MSH-24.3)"),
        _s("receiving_network_address",                            "Namespace ID of the receiving network address (MSH-25.1, HD, v2.7+)"),
        _s("receiving_network_address_universal_id",               "Universal ID of the receiving network address (MSH-25.2)"),
        _s("receiving_network_address_universal_id_type",          "Type of universal ID for receiving network, e.g. ISO (MSH-25.3)"),
        _s("security_classification_tag",                          "Security classification code (MSH-26.1, CWE, v2.7.1+)"),
        _s("security_classification_tag_text",                     "Security classification text (MSH-26.2)"),
        _s("security_classification_tag_coding_system",            "Security classification coding system (MSH-26.3)"),
        _s("security_classification_tag_alt_code",                 "Alternate security classification code (MSH-26.4)"),
        _s("security_classification_tag_alt_text",                 "Alternate security classification text (MSH-26.5)"),
        _s("security_classification_tag_alt_coding_system",        "Alternate security classification coding system (MSH-26.6)"),
        _s("security_handling_instructions",                       "Security handling instruction code (MSH-27.1, CWE repeating, v2.7.1+)"),
        _s("security_handling_instructions_text",                  "Security handling instruction text (MSH-27.2)"),
        _s("security_handling_instructions_coding_system",         "Security handling instruction coding system (MSH-27.3)"),
        _s("security_handling_instructions_alt_code",              "Alternate security handling instruction code (MSH-27.4)"),
        _s("security_handling_instructions_alt_text",              "Alternate security handling instruction text (MSH-27.5)"),
        _s("security_handling_instructions_alt_coding_system",     "Alternate security handling instruction coding system (MSH-27.6)"),
        _s("special_access_restriction",                           "Special access restriction instructions (MSH-28, ST, v2.7.1+)"),
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
        _s("mothers_maiden_given_name",     "Mother's maiden given/first name (PID-6.2)"),
        _s("mothers_maiden_middle_name",    "Mother's maiden middle name or initial (PID-6.3)"),
        _ts("date_of_birth",                "Date of birth parsed to timestamp (PID-7)"),
        _s("administrative_sex",            "Administrative gender code (PID-8): M=Male, F=Female, O=Other, U=Unknown"),
        _s("patient_alias",                 "Alias name(s) for the patient (PID-9, deprecated in v2.7)"),
        _s("race",                          "Race category code per HL7 table 0005 (PID-10.1)"),
        _s("race_text",                     "Human-readable race description (PID-10.2)"),
        _s("race_coding_system",            "Coding system for the race code, e.g. CDCREC, HL70005 (PID-10.3)"),
        _s("race_alt_code",                 "Alternate race code from a secondary coding system (PID-10.4)"),
        _s("race_alt_text",                 "Alternate race description from the secondary coding system (PID-10.5)"),
        _s("race_alt_coding_system",        "Secondary coding system for the race code (PID-10.6)"),
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
        _s("primary_language",              "Patient's primary spoken language code (PID-15.1)"),
        _s("primary_language_text",         "Human-readable language description (PID-15.2)"),
        _s("primary_language_coding_system","Coding system for the language code, e.g. ISO639 (PID-15.3)"),
        _s("primary_language_alt_code",     "Alternate language code from a secondary coding system (PID-15.4)"),
        _s("primary_language_alt_text",     "Alternate language description (PID-15.5)"),
        _s("primary_language_alt_coding_system","Secondary coding system for the language code (PID-15.6)"),
        _s("marital_status",                "Marital status code (PID-16.1): S=Single, M=Married, D=Divorced, W=Widowed"),
        _s("marital_status_text",           "Human-readable marital status description (PID-16.2)"),
        _s("marital_status_coding_system",  "Coding system for the marital status code (PID-16.3)"),
        _s("marital_status_alt_code",       "Alternate marital status code (PID-16.4)"),
        _s("marital_status_alt_text",       "Alternate marital status description (PID-16.5)"),
        _s("marital_status_alt_coding_system","Secondary coding system for the marital status code (PID-16.6)"),
        _s("religion",                      "Religion code (PID-17.1)"),
        _s("religion_text",                 "Human-readable religion description (PID-17.2)"),
        _s("religion_coding_system",        "Coding system for the religion code (PID-17.3)"),
        _s("religion_alt_code",             "Alternate religion code (PID-17.4)"),
        _s("religion_alt_text",             "Alternate religion description (PID-17.5)"),
        _s("religion_alt_coding_system",    "Secondary coding system for the religion code (PID-17.6)"),
        _s("patient_account_number",        "Patient account number at the healthcare facility (PID-18.1)"),
        _s("patient_account_check_digit",   "Check digit for the patient account number (PID-18.2)"),
        _s("patient_account_assigning_authority","Authority that assigned the account number (PID-18.4)"),
        _s("patient_account_type_code",     "Type of account identifier (PID-18.5)"),
        _s("ssn",                           "Social Security Number (PID-19, deprecated in v2.7)"),
        _s("drivers_license",               "Driver's license number and issuing state (PID-20, deprecated in v2.7)"),
        _s("mothers_identifier",            "Identifier for the patient's mother; used in neonatal records (PID-21.1)"),
        _s("mothers_id_check_digit",        "Check digit for the mother's identifier (PID-21.2)"),
        _s("mothers_id_assigning_authority","Authority that assigned the mother's identifier (PID-21.4)"),
        _s("mothers_id_type_code",          "Type of mother's identifier, e.g. MR, PI (PID-21.5)"),
        _s("ethnic_group",                  "Ethnic group code per HL7 table 0189 (PID-22.1)"),
        _s("ethnic_group_text",             "Human-readable ethnic group description (PID-22.2)"),
        _s("ethnic_group_coding_system",    "Coding system for the ethnic group code, e.g. CDCREC, HL70189 (PID-22.3)"),
        _s("ethnic_group_alt_code",         "Alternate ethnic group code from a secondary coding system (PID-22.4)"),
        _s("ethnic_group_alt_text",         "Alternate ethnic group description (PID-22.5)"),
        _s("ethnic_group_alt_coding_system","Secondary coding system for the ethnic group code (PID-22.6)"),
        _s("birth_place",                   "Birthplace as free text (PID-23)"),
        _s("multiple_birth_indicator",      "Whether the patient is one of a multiple birth: Y or N (PID-24)"),
        _i("birth_order",                   "Birth sequence number for multiple-birth patients, e.g. 1, 2, 3 (PID-25)"),
        _s("citizenship",                   "Citizenship country code (PID-26.1)"),
        _s("citizenship_text",              "Human-readable citizenship description (PID-26.2)"),
        _s("citizenship_coding_system",     "Coding system for the citizenship code (PID-26.3)"),
        _s("citizenship_alt_code",          "Alternate citizenship code (PID-26.4)"),
        _s("citizenship_alt_text",          "Alternate citizenship description (PID-26.5)"),
        _s("citizenship_alt_coding_system", "Secondary coding system for the citizenship code (PID-26.6)"),
        _s("veterans_military_status",      "Veteran or military service status code (PID-27.1)"),
        _s("veterans_military_status_text", "Human-readable veteran status description (PID-27.2)"),
        _s("veterans_military_status_coding_system","Coding system for the veteran status code (PID-27.3)"),
        _s("veterans_military_status_alt_code","Alternate veteran status code (PID-27.4)"),
        _s("veterans_military_status_alt_text","Alternate veteran status description (PID-27.5)"),
        _s("veterans_military_status_alt_coding_system","Secondary coding system for the veteran status code (PID-27.6)"),
        _s("nationality",                   "Nationality code (PID-28.1, deprecated in v2.7)"),
        _s("nationality_text",              "Human-readable nationality description (PID-28.2, deprecated)"),
        _s("nationality_coding_system",     "Coding system for the nationality code (PID-28.3, deprecated)"),
        _ts("patient_death_datetime",       "Date/time of patient death parsed to timestamp (PID-29)"),
        _s("patient_death_indicator",       "Death indicator: Y=deceased, N=alive (PID-30)"),
        _s("identity_unknown_indicator",    "Whether the patient's identity is unknown: Y or N (PID-31, v2.5+)"),
        _s("identity_reliability_code",     "Code indicating reliability of patient identity, e.g. AL, UA (PID-32, v2.5+)"),
        _ts("last_update_datetime",         "Date/time the patient record was last updated, parsed to timestamp (PID-33, v2.5+)"),
        _s("last_update_facility",          "Facility namespace ID where the last update occurred (PID-34.1, v2.5+)"),
        _s("last_update_facility_universal_id","Universal ID of the facility (PID-34.2, v2.5+)"),
        _s("last_update_facility_universal_id_type","Type of universal ID, e.g. ISO, GUID (PID-34.3, v2.5+)"),
        _s("species_code",                  "Species code for veterinary use (PID-35.1, v2.5+)"),
        _s("species_code_text",             "Human-readable species description (PID-35.2, v2.5+)"),
        _s("species_code_coding_system",    "Coding system for the species code (PID-35.3, v2.5+)"),
        _s("species_code_alt_code",         "Alternate species code (PID-35.4, v2.5+)"),
        _s("species_code_alt_text",         "Alternate species description (PID-35.5, v2.5+)"),
        _s("species_code_alt_coding_system","Secondary coding system for the species code (PID-35.6, v2.5+)"),
        _s("breed_code",                    "Breed code for veterinary use (PID-36.1, v2.5+)"),
        _s("breed_code_text",               "Human-readable breed description (PID-36.2, v2.5+)"),
        _s("breed_code_coding_system",      "Coding system for the breed code (PID-36.3, v2.5+)"),
        _s("breed_code_alt_code",           "Alternate breed code (PID-36.4, v2.5+)"),
        _s("breed_code_alt_text",           "Alternate breed description (PID-36.5, v2.5+)"),
        _s("breed_code_alt_coding_system",  "Secondary coding system for the breed code (PID-36.6, v2.5+)"),
        _s("strain",                        "Strain description for veterinary use (PID-37, v2.5+)"),
        _s("production_class_code",         "Production class code for veterinary/agricultural use (PID-38.1, v2.5+)"),
        _s("production_class_code_text",    "Human-readable production class description (PID-38.2, v2.5+)"),
        _s("production_class_code_coding_system","Coding system for the production class code (PID-38.3, v2.5+)"),
        _s("production_class_code_alt_code","Alternate production class code (PID-38.4, v2.5+)"),
        _s("production_class_code_alt_text","Alternate production class description (PID-38.5, v2.5+)"),
        _s("production_class_code_alt_coding_system","Secondary coding system for the production class code (PID-38.6, v2.5+)"),
        _s("tribal_citizenship",            "Tribal citizenship or affiliation code (PID-39.1, v2.6+)"),
        _s("tribal_citizenship_text",       "Human-readable tribal citizenship description (PID-39.2, v2.6+)"),
        _s("tribal_citizenship_coding_system","Coding system for the tribal citizenship code (PID-39.3, v2.6+)"),
        _s("tribal_citizenship_alt_code",   "Alternate tribal citizenship code (PID-39.4, v2.6+)"),
        _s("tribal_citizenship_alt_text",   "Alternate tribal citizenship description (PID-39.5, v2.6+)"),
        _s("tribal_citizenship_alt_coding_system","Secondary coding system for the tribal citizenship code (PID-39.6, v2.6+)"),
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
        _s("service_episode_description",   "Free-text description of the service episode (PV1-53, v2.8+)"),
        _s("service_episode_identifier",    "Unique identifier for the service episode (PV1-54.1, v2.8+)"),
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
        _s("placer_order_namespace_id",           "Namespace ID of the placer application (OBR-2.2)"),
        _s("placer_order_universal_id",           "Universal ID for the placer order (OBR-2.3)"),
        _s("placer_order_universal_id_type",      "Type of universal ID for the placer order, e.g. ISO (OBR-2.4)"),
        _s("filler_order_number",                 "Order number assigned by the performing lab/radiology (OBR-3.1)"),
        _s("filler_order_namespace_id",           "Namespace ID of the filler application (OBR-3.2)"),
        _s("filler_order_universal_id",           "Universal ID for the filler order (OBR-3.3)"),
        _s("filler_order_universal_id_type",      "Type of universal ID for the filler order, e.g. ISO (OBR-3.4)"),
        _s("universal_service_identifier",        "Ordered test composite (OBR-4), raw; use service_id / service_text"),
        _s("service_id",                          "Coded test identifier, e.g. LOINC or CPT code (OBR-4.1)"),
        _s("service_text",                        "Human-readable test name, e.g. Basic Metabolic Panel (OBR-4.2)"),
        _s("service_coding_system",               "Coding system for the test code, e.g. LN=LOINC, CPT4 (OBR-4.3)"),
        _s("service_alt_id",                      "Alternate test code from a secondary coding system (OBR-4.4)"),
        _s("service_alt_text",                    "Alternate test name from the secondary coding system (OBR-4.5)"),
        _s("service_alt_coding_system",           "Secondary coding system for the test code (OBR-4.6)"),
        _s("priority",                            "Order priority (OBR-5, deprecated in v2.7): R=Routine, S=STAT, A=ASAP"),
        _ts("requested_datetime",                 "Requested date/time for the observation, parsed to timestamp (OBR-6, deprecated in v2.7)"),
        _ts("observation_datetime",               "Date/time specimen was collected or observation started, parsed to timestamp (OBR-7)"),
        _ts("observation_end_datetime",           "Date/time observation ended or specimen collection completed, parsed to timestamp (OBR-8)"),
        _s("collection_volume",                   "Volume of specimen collected (OBR-9.1)"),
        _s("collection_volume_units",             "Units for the specimen volume (OBR-9.2)"),
        _s("collector_identifier",                "Specimen collector ID (OBR-10.1)"),
        _s("collector_family_name",               "Specimen collector last name (OBR-10.2)"),
        _s("collector_given_name",                "Specimen collector first name (OBR-10.3)"),
        _s("collector_prefix",                    "Specimen collector prefix/title, e.g. Dr (OBR-10.6)"),
        _s("specimen_action_code",                "Action to take on the specimen (OBR-11): A=Add, G=Generated, L=Lab, O=Obtained"),
        _s("danger_code",                         "Code indicating a hazardous specimen (OBR-12.1)"),
        _s("danger_code_text",                    "Human-readable danger/hazard description (OBR-12.2)"),
        _s("danger_code_coding_system",           "Coding system for the danger code (OBR-12.3)"),
        _s("danger_code_alt_code",                "Alternate danger code (OBR-12.4)"),
        _s("danger_code_alt_text",                "Alternate danger description (OBR-12.5)"),
        _s("danger_code_alt_coding_system",       "Secondary coding system for the danger code (OBR-12.6)"),
        _s("relevant_clinical_information",       "Clinical information relevant to the order, e.g. patient condition (OBR-13)"),
        _ts("specimen_received_datetime",         "Date/time the specimen was received by the lab, parsed to timestamp (OBR-14)"),
        _s("specimen_source",                     "Specimen source and collection method (OBR-15, deprecated in v2.7)"),
        _s("ordering_provider",                   "Ordering physician composite (OBR-16), raw; use ordering_provider_* fields"),
        _s("ordering_provider_id",                "Ordering physician identifier/NPI (OBR-16.1)"),
        _s("ordering_provider_family_name",       "Ordering physician last name (OBR-16.2)"),
        _s("ordering_provider_given_name",        "Ordering physician first name (OBR-16.3)"),
        _s("ordering_provider_prefix",            "Ordering physician prefix/title, e.g. Dr (OBR-16.6)"),
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
        _s("result_copies_to",                    "Provider ID to receive a copy of the result (OBR-28.1)"),
        _s("result_copies_to_family_name",        "Result copy-to provider last name (OBR-28.2)"),
        _s("result_copies_to_given_name",         "Result copy-to provider first name (OBR-28.3)"),
        _s("result_copies_to_prefix",             "Result copy-to provider prefix/title (OBR-28.6)"),
        _s("parent_placer_order_number",          "Placer order number of the parent order for reflex tests (OBR-29.1)"),
        _s("transportation_mode",                 "How the specimen is to be transported to the lab (OBR-30)"),
        _s("reason_for_study",                    "Clinical reason or indication code (OBR-31.1)"),
        _s("reason_for_study_text",               "Human-readable reason for study description (OBR-31.2)"),
        _s("reason_for_study_coding_system",      "Coding system for the reason code (OBR-31.3)"),
        _s("reason_for_study_alt_code",           "Alternate reason for study code (OBR-31.4)"),
        _s("reason_for_study_alt_text",           "Alternate reason for study description (OBR-31.5)"),
        _s("reason_for_study_alt_coding_system",  "Secondary coding system for the reason code (OBR-31.6)"),
        _s("principal_result_interpreter",        "Provider who interpreted the result (OBR-32.1)"),
        _s("assistant_result_interpreter",        "Assistant provider who helped interpret the result (OBR-33.1)"),
        _s("technician",                          "Technician who performed the test (OBR-34.1)"),
        _s("transcriptionist",                    "Person who transcribed the result (OBR-35.1)"),
        _ts("scheduled_datetime",                 "Scheduled date/time for the observation, parsed to timestamp (OBR-36)"),
        _i("number_of_sample_containers",         "Number of specimen containers required (OBR-37)"),
        _s("transport_logistics",                 "Specimen transport logistics code (OBR-38.1)"),
        _s("transport_logistics_text",            "Human-readable transport logistics description (OBR-38.2)"),
        _s("transport_logistics_coding_system",   "Coding system for transport logistics (OBR-38.3)"),
        _s("collectors_comment",                  "Specimen collector comment code (OBR-39.1)"),
        _s("collectors_comment_text",             "Human-readable collector comment (OBR-39.2)"),
        _s("collectors_comment_coding_system",    "Coding system for collector comment (OBR-39.3)"),
        _s("transport_arrangement_responsibility","Party responsible for transport code (OBR-40.1)"),
        _s("transport_arrangement_responsibility_text","Human-readable transport responsibility description (OBR-40.2)"),
        _s("transport_arrangement_responsibility_coding_system","Coding system for transport responsibility (OBR-40.3)"),
        _s("transport_arranged",                  "Whether transport has been arranged: A=Arranged, N=Not arranged (OBR-41)"),
        _s("escort_required",                     "Whether an escort is required for specimen transport: R=Required (OBR-42)"),
        _s("planned_patient_transport_comment",   "Planned patient transport comment code (OBR-43.1)"),
        _s("planned_patient_transport_comment_text","Human-readable transport comment (OBR-43.2)"),
        _s("planned_patient_transport_comment_coding_system","Coding system for transport comment (OBR-43.3)"),
        _s("procedure_code",                      "Procedure code, e.g. CPT (OBR-44.1, v2.3+)"),
        _s("procedure_code_text",                 "Human-readable procedure name (OBR-44.2, v2.3+)"),
        _s("procedure_code_coding_system",        "Coding system for the procedure code, e.g. CPT4 (OBR-44.3, v2.3+)"),
        _s("procedure_code_alt_code",             "Alternate procedure code (OBR-44.4, v2.3+)"),
        _s("procedure_code_alt_text",             "Alternate procedure name (OBR-44.5, v2.3+)"),
        _s("procedure_code_alt_coding_system",    "Secondary coding system for the procedure code (OBR-44.6, v2.3+)"),
        _s("procedure_code_modifier",             "Procedure code modifier code (OBR-45.1, v2.3+)"),
        _s("procedure_code_modifier_text",        "Human-readable procedure modifier (OBR-45.2, v2.3+)"),
        _s("procedure_code_modifier_coding_system","Coding system for the procedure modifier (OBR-45.3, v2.3+)"),
        _s("placer_supplemental_service_info",    "Placer supplemental service info code (OBR-46.1, v2.5+)"),
        _s("placer_supplemental_service_info_text","Human-readable placer supplemental info (OBR-46.2, v2.5+)"),
        _s("placer_supplemental_service_info_coding_system","Coding system for placer supplemental info (OBR-46.3, v2.5+)"),
        _s("filler_supplemental_service_info",    "Filler supplemental service info code (OBR-47.1, v2.5+)"),
        _s("filler_supplemental_service_info_text","Human-readable filler supplemental info (OBR-47.2, v2.5+)"),
        _s("filler_supplemental_service_info_coding_system","Coding system for filler supplemental info (OBR-47.3, v2.5+)"),
        _s("medically_necessary_dup_proc_reason", "Reason code for medically necessary duplicate procedure (OBR-48.1, v2.5+)"),
        _s("medically_necessary_dup_proc_reason_text","Human-readable duplicate procedure reason (OBR-48.2, v2.5+)"),
        _s("medically_necessary_dup_proc_reason_coding_system","Coding system for the duplicate procedure reason (OBR-48.3, v2.5+)"),
        _s("medically_necessary_dup_proc_reason_alt_code","Alternate duplicate procedure reason code (OBR-48.4, v2.5+)"),
        _s("medically_necessary_dup_proc_reason_alt_text","Alternate duplicate procedure reason description (OBR-48.5, v2.5+)"),
        _s("medically_necessary_dup_proc_reason_alt_coding_system","Secondary coding system for duplicate procedure reason (OBR-48.6, v2.5+)"),
        _s("result_handling",                     "How the result should be handled, e.g. F=Film-with-patient (OBR-49.1, v2.6+)"),
        _s("parent_universal_service_id",         "Parent order universal service code (OBR-50.1, v2.7+)"),
        _s("parent_universal_service_id_text",    "Human-readable parent order service name (OBR-50.2, v2.7+)"),
        _s("parent_universal_service_id_coding_system","Coding system for the parent service code (OBR-50.3, v2.7+)"),
        _s("parent_universal_service_id_alt_code","Alternate parent service code (OBR-50.4, v2.7+)"),
        _s("parent_universal_service_id_alt_text","Alternate parent service name (OBR-50.5, v2.7+)"),
        _s("parent_universal_service_id_alt_coding_system","Secondary coding system for parent service code (OBR-50.6, v2.7+)"),
        _s("observation_group_id",                "Observation group entity identifier (OBR-51.1, v2.8.2+)"),
        _s("observation_group_namespace_id",      "Observation group namespace ID (OBR-51.2, v2.8.2+)"),
        _s("observation_group_universal_id",      "Observation group universal ID (OBR-51.3, v2.8.2+)"),
        _s("observation_group_universal_id_type", "Observation group universal ID type (OBR-51.4, v2.8.2+)"),
        _s("parent_observation_group_id",         "Parent observation group entity identifier (OBR-52.1, v2.8.2+)"),
        _s("parent_observation_group_namespace_id","Parent observation group namespace ID (OBR-52.2, v2.8.2+)"),
        _s("parent_observation_group_universal_id","Parent observation group universal ID (OBR-52.3, v2.8.2+)"),
        _s("parent_observation_group_universal_id_type","Parent observation group universal ID type (OBR-52.4, v2.8.2+)"),
        _s("alternate_placer_order_number_obr",   "Alternate placer order number ID (OBR-53.1, v2.8.2+)"),
        _s("alternate_placer_order_check_digit",  "Alternate placer order check digit (OBR-53.2, v2.8.2+)"),
        _s("alternate_placer_order_assigning_authority","Alternate placer order assigning authority (OBR-53.4, v2.8.2+)"),
        _s("alternate_placer_order_type_code",    "Alternate placer order ID type code (OBR-53.5, v2.8.2+)"),
        _s("parent_order",                        "Parent order identifier (OBR-54.1, v2.9+)"),
        _s("obr_action_code",                     "Action code (OBR-55, v2.9+)"),
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
        _s("observation_value",               "The result value composite (OBX-5), raw; use observation_value_code / observation_value_text for coded types"),
        _s("observation_value_code",          "Code/identifier from the observation value (OBX-5.1); meaningful when value_type is CWE, CE, or CNE"),
        _s("observation_value_text",          "Human-readable text from the observation value (OBX-5.2); meaningful when value_type is CWE, CE, or CNE"),
        _s("observation_value_coding_system", "Coding system for the observation value code (OBX-5.3), e.g. HL70136, SNOMED, LOINC"),
        _s("observation_value_alt_code",      "Alternate code from a secondary coding system (OBX-5.4)"),
        _s("observation_value_alt_text",      "Alternate text from the secondary coding system (OBX-5.5)"),
        _s("observation_value_alt_coding_system", "Secondary coding system name (OBX-5.6)"),
        _s("units",                           "Units of measure composite (OBX-6), raw; use units_code / units_text"),
        _s("units_code",                      "Coded units, e.g. UCUM code mEq/L (OBX-6.1)"),
        _s("units_text",                      "Human-readable units description, e.g. milliequivalents per liter (OBX-6.2)"),
        _s("units_coding_system",             "Coding system for units, e.g. UCUM, ISO+ (OBX-6.3)"),
        _s("units_alt_code",                  "Alternate units code from a secondary coding system (OBX-6.4)"),
        _s("units_alt_text",                  "Alternate units description (OBX-6.5)"),
        _s("units_alt_coding_system",         "Secondary coding system for units (OBX-6.6)"),
        _s("references_range",                "Normal or reference range for the result, e.g. 136-145 (OBX-7)"),
        _s("interpretation_codes",            "Abnormality flag (OBX-8): N=Normal, H=High, L=Low, A=Abnormal, C=Critical"),
        _s("probability",                     "Probability of the observation being correct, 0-1 scale (OBX-9)"),
        _s("nature_of_abnormal_test",         "What the reference range is based on: A=Age, S=Sex, R=Race (OBX-10)"),
        _s("observation_result_status",       "Result status (OBX-11): F=Final, P=Preliminary, C=Corrected, X=Deleted, R=Not yet verified"),
        _ts("effective_date_of_ref_range",    "Date the reference range became effective, parsed to timestamp (OBX-12)"),
        _s("user_defined_access_checks",      "Site-defined access control value (OBX-13)"),
        _ts("datetime_of_observation",        "Date/time this specific observation was made, parsed to timestamp (OBX-14)"),
        _s("producers_id",                    "Lab or system that produced the result (OBX-15.1)"),
        _s("producers_id_text",               "Human-readable producer name (OBX-15.2)"),
        _s("producers_id_coding_system",      "Coding system for the producer ID (OBX-15.3)"),
        _s("producers_id_alt_code",           "Alternate producer code (OBX-15.4)"),
        _s("producers_id_alt_text",           "Alternate producer description (OBX-15.5)"),
        _s("producers_id_alt_coding_system",  "Secondary coding system for the producer ID (OBX-15.6)"),
        _s("responsible_observer",            "Clinician ID responsible for verifying the result (OBX-16.1)"),
        _s("responsible_observer_family_name","Responsible observer last name (OBX-16.2)"),
        _s("responsible_observer_given_name", "Responsible observer first name (OBX-16.3)"),
        _s("responsible_observer_prefix",     "Responsible observer prefix/title, e.g. Dr (OBX-16.6)"),
        _s("observation_method",              "Observation method code, e.g. LOINC method code (OBX-17.1)"),
        _s("observation_method_text",         "Human-readable observation method description (OBX-17.2)"),
        _s("observation_method_coding_system","Coding system for the observation method (OBX-17.3)"),
        _s("observation_method_alt_code",     "Alternate observation method code (OBX-17.4)"),
        _s("observation_method_alt_text",     "Alternate observation method description (OBX-17.5)"),
        _s("observation_method_alt_coding_system","Secondary coding system for the observation method (OBX-17.6)"),
        _s("equipment_instance_identifier",   "Analyzer/instrument entity ID (OBX-18.1, v2.5+)"),
        _s("equipment_instance_namespace_id", "Namespace ID of the analyzer (OBX-18.2, v2.5+)"),
        _s("equipment_instance_universal_id", "Universal ID of the analyzer (OBX-18.3, v2.5+)"),
        _s("equipment_instance_universal_id_type","Type of universal ID for the analyzer (OBX-18.4, v2.5+)"),
        _ts("datetime_of_analysis",           "Date/time the specimen was analyzed on the instrument, parsed to timestamp (OBX-19, v2.5+)"),
        _s("observation_site",                "Body site code, e.g. LA=Left arm (OBX-20.1, v2.7+)"),
        _s("observation_site_text",           "Human-readable body site description (OBX-20.2, v2.7+)"),
        _s("observation_site_coding_system",  "Coding system for the body site code (OBX-20.3, v2.7+)"),
        _s("observation_site_alt_code",       "Alternate body site code (OBX-20.4, v2.7+)"),
        _s("observation_site_alt_text",       "Alternate body site description (OBX-20.5, v2.7+)"),
        _s("observation_site_alt_coding_system","Secondary coding system for the body site code (OBX-20.6, v2.7+)"),
        _s("observation_instance_identifier", "Observation instance entity ID (OBX-21.1, v2.7+)"),
        _s("observation_instance_namespace_id","Namespace ID of the observation instance (OBX-21.2, v2.7+)"),
        _s("observation_instance_universal_id","Universal ID of the observation instance (OBX-21.3, v2.7+)"),
        _s("observation_instance_universal_id_type","Type of universal ID for the observation instance (OBX-21.4, v2.7+)"),
        _s("mood_code",                       "Mood code: EVN=Event (actual result), INT=Intent (ordered) (OBX-22.1, v2.7+)"),
        _s("mood_code_text",                  "Human-readable mood description (OBX-22.2, v2.7+)"),
        _s("mood_code_coding_system",         "Coding system for the mood code (OBX-22.3, v2.7+)"),
        _s("mood_code_alt_code",              "Alternate mood code (OBX-22.4, v2.7+)"),
        _s("mood_code_alt_text",              "Alternate mood description (OBX-22.5, v2.7+)"),
        _s("mood_code_alt_coding_system",     "Secondary coding system for the mood code (OBX-22.6, v2.7+)"),
        _s("performing_organization_name",    "Name of the organization that performed the test (OBX-23.1, v2.7+)"),
        _s("performing_organization_type_code","Organization name type code (OBX-23.2, v2.7+)"),
        _s("performing_organization_id",      "Organization ID number (OBX-23.3, v2.7+)"),
        _s("performing_organization_check_digit","Organization check digit (OBX-23.4, v2.7+)"),
        _s("performing_organization_check_digit_scheme","Check digit scheme for the organization ID (OBX-23.5, v2.7+)"),
        _s("performing_organization_assigning_authority","Authority that assigned the organization ID (OBX-23.6, v2.7+)"),
        _s("performing_organization_id_type_code","Type of organization ID, e.g. NPI, CLIA (OBX-23.7, v2.7+)"),
        _s("performing_organization_assigning_facility","Facility that assigned the organization ID (OBX-23.8, v2.7+)"),
        _s("performing_organization_name_rep_code","Name representation code (OBX-23.9, v2.7+)"),
        _s("performing_organization_identifier","Organization identifier (OBX-23.10, v2.7+)"),
        _s("performing_organization_address", "Full performing organization address composite (OBX-24, v2.7+), raw"),
        _s("performing_org_address_street",   "Performing organization street address (OBX-24.1, v2.7+)"),
        _s("performing_org_address_other",    "Performing organization address line 2 (OBX-24.2, v2.7+)"),
        _s("performing_org_address_city",     "Performing organization city (OBX-24.3, v2.7+)"),
        _s("performing_org_address_state",    "Performing organization state/province (OBX-24.4, v2.7+)"),
        _s("performing_org_address_zip",      "Performing organization postal code (OBX-24.5, v2.7+)"),
        _s("performing_org_address_country",  "Performing organization country (OBX-24.6, v2.7+)"),
        _s("performing_org_address_type",     "Performing organization address type (OBX-24.7, v2.7+)"),
        _s("performing_org_medical_director", "Medical director ID (OBX-25.1, v2.7+)"),
        _s("performing_org_medical_director_family_name","Medical director last name (OBX-25.2, v2.7+)"),
        _s("performing_org_medical_director_given_name","Medical director first name (OBX-25.3, v2.7+)"),
        _s("performing_org_medical_director_prefix","Medical director prefix/title (OBX-25.6, v2.7+)"),
        _s("patient_results_release_category","Category controlling release of results to the patient (OBX-26, v2.8+)"),
        _s("root_cause",                      "Root cause code for a corrected or amended result (OBX-27.1, v2.8+)"),
        _s("root_cause_text",                 "Human-readable root cause description (OBX-27.2, v2.8+)"),
        _s("root_cause_coding_system",        "Coding system for the root cause code (OBX-27.3, v2.8+)"),
        _s("root_cause_alt_code",             "Alternate root cause code (OBX-27.4, v2.8+)"),
        _s("root_cause_alt_text",             "Alternate root cause description (OBX-27.5, v2.8+)"),
        _s("root_cause_alt_coding_system",    "Secondary coding system for the root cause (OBX-27.6, v2.8+)"),
        _s("local_process_control",           "Site-defined local process code (OBX-28.1, v2.8+)"),
        _s("local_process_control_text",      "Human-readable local process description (OBX-28.2, v2.8+)"),
        _s("local_process_control_coding_system","Coding system for the local process code (OBX-28.3, v2.8+)"),
        _s("observation_type",                "Observation type (OBX-29, v2.8.2+)"),
        _s("observation_sub_type",            "Observation sub-type (OBX-30, v2.8.2+)"),
        _s("obx_action_code",                 "Action code (OBX-31, v2.9+)"),
        _s("observation_value_absent_reason", "Reason code for absent observation value (OBX-32.1, v2.9+)"),
        _s("observation_value_absent_reason_text","Human-readable absent value reason description (OBX-32.2, v2.9+)"),
        _s("observation_value_absent_reason_coding_system","Coding system for the absent value reason (OBX-32.3, v2.9+)"),
        _s("observation_related_specimen_id", "Related specimen entity identifier (OBX-33.1, v2.9+)"),
        _s("observation_related_specimen_namespace_id","Related specimen namespace ID (OBX-33.2, v2.9+)"),
        _s("observation_related_specimen_universal_id","Related specimen universal ID (OBX-33.3, v2.9+)"),
        _s("observation_related_specimen_universal_id_type","Related specimen universal ID type (OBX-33.4, v2.9+)"),
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
        _s("nk_telecommunication_info",  "Next of kin telecommunication information (NK1-40, v2.7+)"),
        _s("contact_telecommunication_info", "Contact person telecommunication information (NK1-41, v2.7+)"),
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
# PD1 — Patient Additional Demographic
# ---------------------------------------------------------------------------

PD1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("living_dependency",                        "Living dependency code (PD1-1): S=Spouse, M=Medical Supervision, C=Small Children"),
        _s("living_arrangement",                       "Living arrangement code (PD1-2): A=Alone, F=Family, I=Institution, R=Relative, U=Unknown"),
        _s("patient_primary_facility",                 "Primary care facility name (PD1-3.1)"),
        _s("patient_primary_care_provider",            "Primary care provider identifier (PD1-4.1, deprecated)"),
        _s("student_indicator",                        "Student status (PD1-5): F=Full-time, P=Part-time, N=Not a student"),
        _s("handicap",                                 "Handicap code (PD1-6)"),
        _s("living_will_code",                         "Living will status (PD1-7): Y=Yes, F=Filed with patient, N=No"),
        _s("organ_donor_code",                         "Organ donor status (PD1-8): Y=Yes, F=Filed with patient, N=No"),
        _s("separate_bill",                            "Separate billing flag Y/N (PD1-9)"),
        _s("duplicate_patient",                        "Duplicate patient identifiers (PD1-10.1)"),
        _s("publicity_code",                           "Publicity/directory listing code (PD1-11.1)"),
        _s("protection_indicator",                     "Protection indicator Y/N (PD1-12); if Y, patient info is restricted"),
        _s("protection_indicator_effective_date",      "Date the protection indicator became effective (PD1-13)"),
        _s("place_of_worship",                         "Place of worship organization name (PD1-14.1)"),
        _s("advance_directive_code",                   "Advance directive code (PD1-15.1): DNR=Do Not Resuscitate"),
        _s("immunization_registry_status",             "Immunization registry status (PD1-16): A=Active, I=Inactive, P=Protected"),
        _s("immunization_registry_status_effective_date", "Immunization registry status effective date (PD1-17)"),
        _s("publicity_code_effective_date",            "Publicity code effective date (PD1-18)"),
        _s("military_branch",                          "Military branch (PD1-19): USA=Army, USN=Navy, USAF=Air Force, USMC=Marines"),
        _s("military_rank_grade",                      "Military rank or grade (PD1-20)"),
        _s("military_status",                          "Military status (PD1-21): ACT=Active duty, RET=Retired, DEC=Deceased"),
        _s("advance_directive_last_verified_date",     "Date advance directive was last verified (PD1-22, v2.8+)"),
        _s("retirement_date",                          "Date the patient retired (PD1-23, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# PV2 — Patient Visit Additional Information
# ---------------------------------------------------------------------------

PV2_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("prior_pending_location",                   "Prior pending transfer location (PV2-1)"),
        _s("accommodation_code",                       "Accommodation code (PV2-2.1)"),
        _s("admit_reason",                             "Reason for admission (PV2-3.1)"),
        _s("transfer_reason",                          "Reason for transfer (PV2-4.1)"),
        _s("patient_valuables",                        "Patient's valuable items description (PV2-5)"),
        _s("patient_valuables_location",               "Location of patient's valuables (PV2-6)"),
        _s("visit_user_code",                          "Visit user code (PV2-7)"),
        _ts("expected_admit_datetime",                 "Expected admission date/time (PV2-8)"),
        _ts("expected_discharge_datetime",             "Expected discharge date/time (PV2-9)"),
        _i("estimated_length_of_inpatient_stay",       "Estimated length of inpatient stay in days (PV2-10)"),
        _i("actual_length_of_inpatient_stay",          "Actual length of inpatient stay in days (PV2-11)"),
        _s("visit_description",                        "Free-text visit description (PV2-12)"),
        _s("referral_source_code",                     "Referral source identifier (PV2-13.1)"),
        _s("previous_service_date",                    "Date of previous service (PV2-14)"),
        _s("employment_illness_related_indicator",     "Employment illness related Y/N (PV2-15)"),
        _s("purge_status_code",                        "Purge status code (PV2-16)"),
        _s("purge_status_date",                        "Purge status date (PV2-17)"),
        _s("special_program_code",                     "Special program code (PV2-18)"),
        _s("retention_indicator",                      "Retention indicator Y/N (PV2-19)"),
        _i("expected_number_of_insurance_plans",       "Expected number of insurance plans (PV2-20)"),
        _s("visit_publicity_code",                     "Visit publicity code (PV2-21)"),
        _s("visit_protection_indicator",               "Visit protection indicator Y/N (PV2-22)"),
        _s("clinic_organization_name",                 "Clinic organization name (PV2-23.1)"),
        _s("patient_status_code",                      "Patient status code (PV2-24)"),
        _s("visit_priority_code",                      "Visit priority code (PV2-25)"),
        _s("previous_treatment_date",                  "Previous treatment date (PV2-26)"),
        _s("expected_discharge_disposition",            "Expected discharge disposition (PV2-27)"),
        _s("signature_on_file_date",                   "Signature on file date (PV2-28)"),
        _s("first_similar_illness_date",               "Date of first similar illness (PV2-29)"),
        _s("patient_charge_adjustment_code",           "Patient charge adjustment code (PV2-30.1)"),
        _s("recurring_service_code",                   "Recurring service code (PV2-31)"),
        _s("billing_media_code",                       "Billing media code Y/N (PV2-32)"),
        _ts("expected_surgery_datetime",               "Expected surgery date/time (PV2-33)"),
        _s("military_partnership_code",                "Military partnership code Y/N (PV2-34)"),
        _s("military_non_availability_code",           "Military non-availability code Y/N (PV2-35)"),
        _s("newborn_baby_indicator",                   "Newborn baby indicator Y/N (PV2-36)"),
        _s("baby_detained_indicator",                  "Baby detained indicator Y/N (PV2-37)"),
        _s("mode_of_arrival_code",                     "Mode of arrival code (PV2-38.1): A=Ambulance, C=Car, F=On foot, H=Helicopter"),
        _s("recreational_drug_use_code",               "Recreational drug use code (PV2-39.1)"),
        _s("admission_level_of_care_code",             "Admission level of care code (PV2-40.1)"),
        _s("precaution_code",                          "Precaution code (PV2-41.1)"),
        _s("patient_condition_code",                   "Patient condition code (PV2-42.1)"),
        _s("living_will_code_pv2",                     "Living will code (PV2-43); same values as PD1-7"),
        _s("organ_donor_code_pv2",                     "Organ donor code (PV2-44); same values as PD1-8"),
        _s("advance_directive_code_pv2",               "Advance directive code (PV2-45.1)"),
        _s("patient_status_effective_date",            "Patient status effective date (PV2-46)"),
        _ts("expected_loa_return_datetime",            "Expected leave of absence return date/time (PV2-47)"),
        _ts("expected_preadmission_testing_datetime",  "Expected pre-admission testing date/time (PV2-48)"),
        _s("notify_clergy_code",                       "Notify clergy code (PV2-49)"),
        _s("advance_directive_last_verified_date_pv2", "Date advance directive was last verified (PV2-50, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# MRG — Merge Patient Information
# ---------------------------------------------------------------------------

MRG_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("prior_patient_identifier_list",   "Prior patient identifier list raw (MRG-1)"),
        _s("prior_patient_id_value",          "Prior patient ID value (MRG-1.1)"),
        _s("prior_alternate_patient_id",      "Prior alternate patient ID (MRG-2, deprecated)"),
        _s("prior_patient_account_number",    "Prior patient account number (MRG-3.1)"),
        _s("prior_patient_id",                "Prior patient ID (MRG-4.1, deprecated)"),
        _s("prior_visit_number",              "Prior visit number (MRG-5.1)"),
        _s("prior_alternate_visit_id",        "Prior alternate visit ID (MRG-6.1)"),
        _s("prior_patient_name",              "Prior patient name raw (MRG-7)"),
        _s("prior_patient_family_name",       "Prior patient family name (MRG-7.1)"),
        _s("prior_patient_given_name",        "Prior patient given name (MRG-7.2)"),
    ]
)

# ---------------------------------------------------------------------------
# IAM — Patient Adverse Reaction Information
# ---------------------------------------------------------------------------

IAM_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                          "Sequence number for this IAM segment within the message (IAM-1)"),
        _s("allergen_type_code",                  "Allergen type (IAM-2.1): DA=Drug, FA=Food, EA=Environmental, MA=Miscellaneous"),
        _s("allergen_code",                       "Allergen code/mnemonic/description raw (IAM-3)"),
        _s("allergen_id",                         "Allergen identifier code (IAM-3.1)"),
        _s("allergen_text",                       "Allergen display text (IAM-3.2)"),
        _s("allergen_coding_system",              "Allergen coding system (IAM-3.3)"),
        _s("allergy_severity_code",               "Allergy severity (IAM-4.1): SV=Severe, MO=Moderate, MI=Mild, U=Unknown"),
        _s("allergy_reaction_code",               "Allergy reaction description (IAM-5)"),
        _s("allergy_action_code",                 "Action code (IAM-6.1): A=Add, D=Delete, U=Update"),
        _s("allergy_unique_identifier",           "Unique allergy identifier (IAM-7.1)"),
        _s("action_reason",                       "Reason for the action (IAM-8)"),
        _s("sensitivity_to_causative_agent_code", "Sensitivity code (IAM-9.1)"),
        _s("allergen_group_code",                 "Allergen group code (IAM-10.1)"),
        _s("allergen_group_text",                 "Allergen group text (IAM-10.2)"),
        _s("onset_date",                          "Allergy onset date (IAM-11)"),
        _s("onset_date_text",                     "Free-text onset date description (IAM-12)"),
        _ts("reported_datetime",                  "When the allergy was reported (IAM-13)"),
        _s("reported_by",                         "Person who reported the allergy raw (IAM-14)"),
        _s("reported_by_family_name",             "Reporter family name (IAM-14.1)"),
        _s("reported_by_given_name",              "Reporter given name (IAM-14.2)"),
        _s("relationship_to_patient_code",        "Reporter's relationship to patient (IAM-15.1)"),
        _s("alert_device_code",                   "Alert device code (IAM-16.1)"),
        _s("allergy_clinical_status_code",        "Clinical status (IAM-17.1): A=Active, I=Inactive, R=Resolved"),
        _s("statused_by_person",                  "Person who set the status (IAM-18.1)"),
        _s("statused_by_organization",            "Organization that set the status (IAM-19.1)"),
        _ts("statused_at_datetime",               "Date/time status was set (IAM-20)"),
        _s("inactivated_by_person",               "Person who inactivated the record (IAM-21.1, v2.6+)"),
        _ts("inactivated_datetime",               "Date/time the record was inactivated (IAM-22, v2.6+)"),
        _s("initially_recorded_by_person",        "Person who initially recorded the reaction (IAM-23.1, v2.6+)"),
        _ts("initially_recorded_datetime",        "Date/time the reaction was initially recorded (IAM-24, v2.6+)"),
        _s("modified_by_person",                  "Person who last modified the record (IAM-25.1, v2.6+)"),
        _ts("modified_datetime",                  "Date/time the record was last modified (IAM-26, v2.6+)"),
        _s("clinician_identified_code",           "Clinician-identified allergen code (IAM-27.1, v2.7+)"),
        _s("initially_recorded_by_organization",  "Organization that initially recorded the reaction (IAM-28.1, v2.9+)"),
        _s("modified_by_organization",            "Organization that last modified the record (IAM-29.1, v2.9+)"),
        _s("inactivated_by_organization",         "Organization that inactivated the record (IAM-30.1, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# PR1 — Procedures
# ---------------------------------------------------------------------------

PR1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                    "Sequence number for this PR1 segment within the message (PR1-1)"),
        _s("procedure_coding_method",      "Procedure coding method (PR1-2, deprecated)"),
        _s("procedure_code",               "Procedure code raw (PR1-3)"),
        _s("procedure_id",                 "CPT/ICD procedure code (PR1-3.1)"),
        _s("procedure_text",               "Procedure display text (PR1-3.2)"),
        _s("procedure_coding_system",      "Procedure coding system (PR1-3.3)"),
        _s("procedure_description",        "Procedure description (PR1-4, deprecated)"),
        _ts("procedure_datetime",          "When the procedure was performed (PR1-5)"),
        _s("procedure_functional_type",    "Functional type (PR1-6): A=Anesthesia, P=Procedure, I=Invasion"),
        _i("procedure_minutes",            "Duration of the procedure in minutes (PR1-7)"),
        _s("anesthesiologist",             "Anesthesiologist identifier (PR1-8.1, deprecated)"),
        _s("anesthesia_code",              "Anesthesia code (PR1-9)"),
        _i("anesthesia_minutes",           "Anesthesia duration in minutes (PR1-10)"),
        _s("surgeon",                      "Surgeon identifier (PR1-11.1, deprecated)"),
        _s("procedure_practitioner",       "Procedure practitioner identifier (PR1-12.1, deprecated)"),
        _s("consent_code",                 "Consent code (PR1-13.1)"),
        _s("procedure_priority",           "Procedure priority (PR1-14)"),
        _s("associated_diagnosis_code",    "Associated diagnosis code (PR1-15.1)"),
        _s("procedure_code_modifier",      "Procedure modifier code (PR1-16.1)"),
        _s("procedure_drg_type",           "DRG type (PR1-17)"),
        _s("tissue_type_code",             "Tissue type code (PR1-18.1)"),
        _s("procedure_identifier",         "Unique procedure identifier (PR1-19.1)"),
        _s("procedure_action_code",        "Action code (PR1-20): A=Add, D=Delete, U=Update"),
        _s("drg_procedure_determination_status", "DRG procedure determination status (PR1-21.1, v2.6+)"),
        _s("drg_procedure_relevance",      "DRG procedure relevance (PR1-22.1, v2.6+)"),
        _s("treating_organizational_unit",  "Treating organizational unit (PR1-23, v2.6+)"),
        _s("respiratory_within_surgery",    "Respiratory within surgery indicator (PR1-24, v2.7+)"),
        _s("parent_procedure_id",           "Parent procedure identifier (PR1-25.1, v2.7+)"),
    ]
)

# ---------------------------------------------------------------------------
# ORC — Common Order
# ---------------------------------------------------------------------------

ORC_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                               "Synthetic sequence number for multiple ORC segments per message"),
        _s("order_control",                            "Order control code (ORC-1): NW=New, CA=Cancel, DC=Discontinue, XO=Change, SC=Status"),
        _s("placer_order_number",                      "Order number from the placer system (ORC-2.1)"),
        _s("filler_order_number",                      "Order number from the filler system (ORC-3.1)"),
        _s("placer_group_number",                      "Placer group number (ORC-4.1)"),
        _s("order_status",                             "Order status (ORC-5): IP=In Process, CM=Completed, SC=Scheduled, CA=Cancelled"),
        _s("response_flag",                            "Response flag (ORC-6): E=Report exceptions, R=Same as initiation, D=Deferred, N=Notification"),
        _s("quantity_timing",                          "Quantity/timing (ORC-7, deprecated)"),
        _s("parent_order",                             "Parent order reference (ORC-8)"),
        _ts("datetime_of_transaction",                 "Transaction date/time (ORC-9)"),
        _s("entered_by",                               "Person who entered the order (ORC-10.1)"),
        _s("verified_by",                              "Person who verified the order (ORC-11.1)"),
        _s("ordering_provider",                        "Ordering provider raw (ORC-12)"),
        _s("ordering_provider_id",                     "Ordering provider identifier (ORC-12.1)"),
        _s("ordering_provider_family_name",            "Ordering provider family name (ORC-12.2)"),
        _s("ordering_provider_given_name",             "Ordering provider given name (ORC-12.3)"),
        _s("enterers_location",                        "Location where order was entered (ORC-13)"),
        _s("call_back_phone_number",                   "Callback phone number (ORC-14)"),
        _ts("order_effective_datetime",                "Order effective date/time (ORC-15)"),
        _s("order_control_code_reason",                "Reason for the order control action (ORC-16.1)"),
        _s("entering_organization",                    "Organization that entered the order (ORC-17.1)"),
        _s("entering_device",                          "Device used to enter the order (ORC-18.1)"),
        _s("action_by",                                "Person who actioned the order (ORC-19.1)"),
        _s("advanced_beneficiary_notice_code",         "ABN code (ORC-20.1)"),
        _s("ordering_facility_name",                   "Ordering facility name (ORC-21.1)"),
        _s("ordering_facility_address",                "Ordering facility address (ORC-22)"),
        _s("ordering_facility_phone",                  "Ordering facility phone (ORC-23)"),
        _s("ordering_provider_address",                "Ordering provider address (ORC-24)"),
        _s("order_status_modifier",                    "Order status modifier (ORC-25.1)"),
        _s("abn_override_reason",                      "ABN override reason (ORC-26.1)"),
        _ts("fillers_expected_availability_datetime",  "Filler's expected availability date/time (ORC-27)"),
        _s("confidentiality_code",                     "Confidentiality code (ORC-28.1)"),
        _s("order_type",                               "Order type (ORC-29.1)"),
        _s("enterer_authorization_mode",               "Enterer authorization mode (ORC-30.1)"),
        _s("parent_universal_service_id",              "Parent universal service identifier (ORC-31.1)"),
        _s("advanced_beneficiary_notice_date",          "Advanced beneficiary notice date (ORC-32, v2.6+)"),
        _s("alternate_placer_order_number",             "Alternate placer order number (ORC-33.1, v2.7+)"),
        _s("order_workflow_profile",                    "Order workflow profile (ORC-34.1, v2.8.2+)"),
        _s("orc_action_code",                           "Action code (ORC-35, v2.9+)"),
        _s("order_status_date_range",                   "Order status date range (ORC-36, v2.9+)"),
        _ts("order_creation_datetime",                  "Order creation date/time (ORC-37, v2.9+)"),
        _s("filler_order_group_number",                 "Filler order group number (ORC-38.1, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# NTE — Notes and Comments
# ---------------------------------------------------------------------------

NTE_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",            "Sequence number for this NTE segment within the message (NTE-1)"),
        _s("source_of_comment",    "Source of comment (NTE-2): L=Ancillary/Filler, P=Orderer/Placer, O=Other"),
        _s("comment",              "Free-text comment/note content (NTE-3); may contain formatted text"),
        _s("comment_type",         "Comment type code (NTE-4.1)"),
        _s("entered_by",           "Person who entered the note (NTE-5.1, v2.6+)"),
        _ts("entered_datetime",    "Date/time the note was entered (NTE-6, v2.6+)"),
        _ts("effective_start_date","Effective start date of the note (NTE-7, v2.6+)"),
        _ts("expiration_date",     "Expiration date of the note (NTE-8, v2.6+)"),
        _s("coded_comment",        "Coded comment (NTE-9.1, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# SPM — Specimen
# ---------------------------------------------------------------------------

SPM_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                      "Sequence number for this SPM segment within the message (SPM-1)"),
        _s("specimen_id",                     "Specimen identifier (SPM-2.1)"),
        _s("specimen_parent_ids",             "Parent specimen identifiers (SPM-3.1)"),
        _s("specimen_type",                   "Specimen type raw (SPM-4)"),
        _s("specimen_type_code",              "Specimen type code (SPM-4.1)"),
        _s("specimen_type_text",              "Specimen type text (SPM-4.2)"),
        _s("specimen_type_modifier",          "Specimen type modifier (SPM-5.1)"),
        _s("specimen_additives",              "Specimen additives/preservatives (SPM-6.1)"),
        _s("specimen_collection_method",      "Collection method (SPM-7.1)"),
        _s("specimen_source_site",            "Source body site (SPM-8.1)"),
        _s("specimen_source_site_modifier",   "Source site modifier (SPM-9.1)"),
        _s("specimen_collection_site",        "Collection site (SPM-10.1)"),
        _s("specimen_role",                   "Specimen role (SPM-11.1)"),
        _s("specimen_collection_amount",      "Collection amount with units (SPM-12.1)"),
        _i("grouped_specimen_count",          "Number of grouped specimens (SPM-13)"),
        _s("specimen_description",            "Free-text specimen description (SPM-14)"),
        _s("specimen_handling_code",          "Handling instructions code (SPM-15.1)"),
        _s("specimen_risk_code",              "Risk code (SPM-16.1)"),
        _s("specimen_collection_datetime",    "Specimen collection date/time range start (SPM-17.1)"),
        _ts("specimen_received_datetime",     "When specimen was received (SPM-18)"),
        _ts("specimen_expiration_datetime",   "Specimen expiration date/time (SPM-19)"),
        _s("specimen_availability",           "Specimen availability Y/N (SPM-20)"),
        _s("specimen_reject_reason",          "Reject reason code (SPM-21.1)"),
        _s("specimen_quality",                "Quality assessment code (SPM-22.1)"),
        _s("specimen_appropriateness",        "Appropriateness assessment code (SPM-23.1)"),
        _s("specimen_condition",              "Specimen condition code (SPM-24.1)"),
        _s("specimen_current_quantity",       "Current specimen quantity (SPM-25.1)"),
        _i("number_of_specimen_containers",   "Number of specimen containers (SPM-26)"),
        _s("container_type",                  "Container type (SPM-27.1)"),
        _s("container_condition",             "Container condition (SPM-28.1)"),
        _s("specimen_child_role",             "Specimen child role (SPM-29.1)"),
        _s("accession_id",                    "Accession identifier (SPM-30.1, v2.7+)"),
        _s("other_specimen_id",               "Other specimen identifier (SPM-31.1, v2.7+)"),
        _s("shipment_id",                     "Shipment identifier (SPM-32.1, v2.7+)"),
        _ts("culture_start_datetime",         "Culture start date/time (SPM-33, v2.9+)"),
        _ts("culture_final_datetime",         "Culture final date/time (SPM-34, v2.9+)"),
        _s("spm_action_code",                 "Action code (SPM-35, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# IN1 — Insurance
# ---------------------------------------------------------------------------

IN1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                       "Sequence number for this IN1 segment within the message (IN1-1)"),
        _s("insurance_plan_id",                "Insurance plan identifier code (IN1-2.1)"),
        _s("insurance_plan_text",              "Insurance plan display text (IN1-2.2)"),
        _s("insurance_company_id",             "Insurance company identifier (IN1-3.1)"),
        _s("insurance_company_name",           "Insurance company name (IN1-4.1)"),
        _s("insurance_company_address",        "Insurance company address (IN1-5)"),
        _s("insurance_co_contact_person",      "Contact person at insurance company (IN1-6.1)"),
        _s("insurance_co_phone_number",        "Insurance company phone number (IN1-7)"),
        _s("group_number",                     "Insurance group/policy group number (IN1-8)"),
        _s("group_name",                       "Insurance group name (IN1-9.1)"),
        _s("insureds_group_emp_id",            "Insured's group employer identifier (IN1-10.1)"),
        _s("insureds_group_emp_name",          "Insured's group employer name (IN1-11.1)"),
        _s("plan_effective_date",              "Plan effective date (IN1-12)"),
        _s("plan_expiration_date",             "Plan expiration date (IN1-13)"),
        _s("authorization_information",        "Authorization information (IN1-14.1)"),
        _s("plan_type",                        "Plan type (IN1-15)"),
        _s("name_of_insured",                  "Insured person's name raw (IN1-16)"),
        _s("name_of_insured_family",           "Insured person's family name (IN1-16.1)"),
        _s("name_of_insured_given",            "Insured person's given name (IN1-16.2)"),
        _s("insureds_relationship_to_patient", "Relationship to patient code (IN1-17.1)"),
        _ts("insureds_date_of_birth",          "Insured's date of birth (IN1-18)"),
        _s("insureds_address",                 "Insured's address (IN1-19)"),
        _s("assignment_of_benefits",           "Assignment of benefits (IN1-20)"),
        _s("coordination_of_benefits",         "Coordination of benefits (IN1-21)"),
        _s("coord_of_ben_priority",            "COB priority (IN1-22)"),
        _s("notice_of_admission_flag",         "Notice of admission flag Y/N (IN1-23)"),
        _s("notice_of_admission_date",         "Admission notice date (IN1-24)"),
        _s("report_of_eligibility_flag",       "Eligibility report flag Y/N (IN1-25)"),
        _s("report_of_eligibility_date",       "Eligibility report date (IN1-26)"),
        _s("release_information_code",         "Release info code (IN1-27)"),
        _s("pre_admit_cert",                   "Pre-admission certification number (IN1-28)"),
        _ts("verification_datetime",           "Verification date/time (IN1-29)"),
        _s("verification_by",                  "Verified by person (IN1-30.1)"),
        _s("type_of_agreement_code",           "Agreement type (IN1-31)"),
        _s("billing_status",                   "Billing status (IN1-32)"),
        _i("lifetime_reserve_days",            "Lifetime reserve days (IN1-33)"),
        _i("delay_before_lr_day",              "Delay before lifetime reserve day (IN1-34)"),
        _s("company_plan_code",                "Company plan code (IN1-35)"),
        _s("policy_number",                    "Policy number (IN1-36)"),
        _s("policy_deductible",                "Policy deductible amount (IN1-37.1)"),
        _s("policy_limit_amount",              "Policy limit amount (IN1-38.1)"),
        _i("policy_limit_days",                "Policy limit in days (IN1-39)"),
        _s("room_rate_semi_private",           "Semi-private room rate (IN1-40.1, deprecated)"),
        _s("room_rate_private",                "Private room rate (IN1-41.1, deprecated)"),
        _s("insureds_employment_status",       "Insured's employment status (IN1-42.1)"),
        _s("insureds_administrative_sex",      "Insured's sex (IN1-43): M=Male, F=Female"),
        _s("insureds_employers_address",       "Insured's employer address (IN1-44)"),
        _s("verification_status",              "Verification status (IN1-45)"),
        _s("prior_insurance_plan_id",          "Prior insurance plan ID (IN1-46)"),
        _s("coverage_type",                    "Coverage type (IN1-47)"),
        _s("handicap",                         "Handicap code (IN1-48)"),
        _s("insureds_id_number",               "Insured's identifier (IN1-49.1)"),
        _s("signature_code",                   "Signature code (IN1-50)"),
        _s("signature_code_date",              "Signature code date (IN1-51)"),
        _s("insureds_birth_place",             "Insured's birth place (IN1-52)"),
        _s("vip_indicator",                    "VIP indicator (IN1-53)"),
        _s("external_health_plan_identifiers", "External health plan identifiers (IN1-54.1, v2.8+)"),
        _s("insurance_action_code",            "Insurance action code (IN1-55, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# GT1 — Guarantor
# ---------------------------------------------------------------------------

GT1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                           "Sequence number for this GT1 segment within the message (GT1-1)"),
        _s("guarantor_number",                     "Guarantor identifier (GT1-2.1)"),
        _s("guarantor_name",                       "Guarantor name raw (GT1-3)"),
        _s("guarantor_family_name",                "Guarantor family name (GT1-3.1)"),
        _s("guarantor_given_name",                 "Guarantor given name (GT1-3.2)"),
        _s("guarantor_spouse_name",                "Guarantor spouse name (GT1-4.1)"),
        _s("guarantor_address",                    "Guarantor address (GT1-5)"),
        _s("guarantor_ph_num_home",                "Guarantor home phone (GT1-6)"),
        _s("guarantor_ph_num_business",            "Guarantor business phone (GT1-7)"),
        _ts("guarantor_date_of_birth",             "Guarantor date of birth (GT1-8)"),
        _s("guarantor_administrative_sex",         "Guarantor sex (GT1-9): M=Male, F=Female"),
        _s("guarantor_type",                       "Guarantor type (GT1-10)"),
        _s("guarantor_relationship",               "Guarantor relationship to patient (GT1-11.1)"),
        _s("guarantor_ssn",                        "Guarantor social security number (GT1-12)"),
        _s("guarantor_date_begin",                 "Guarantor start date (GT1-13)"),
        _s("guarantor_date_end",                   "Guarantor end date (GT1-14)"),
        _i("guarantor_priority",                   "Guarantor priority (GT1-15)"),
        _s("guarantor_employer_name",              "Guarantor employer name (GT1-16.1)"),
        _s("guarantor_employer_address",           "Guarantor employer address (GT1-17)"),
        _s("guarantor_employer_phone_number",      "Guarantor employer phone (GT1-18)"),
        _s("guarantor_employee_id_number",         "Guarantor employee ID (GT1-19.1)"),
        _s("guarantor_employment_status",          "Guarantor employment status (GT1-20)"),
        _s("guarantor_organization_name",          "Guarantor organization name (GT1-21.1)"),
        _s("guarantor_billing_hold_flag",          "Billing hold flag Y/N (GT1-22)"),
        _s("guarantor_credit_rating_code",         "Credit rating code (GT1-23.1)"),
        _ts("guarantor_death_date_and_time",       "Guarantor death date/time (GT1-24)"),
        _s("guarantor_death_flag",                 "Guarantor death flag Y/N (GT1-25)"),
        _s("guarantor_charge_adjustment_code",     "Charge adjustment code (GT1-26.1)"),
        _s("guarantor_household_annual_income",    "Household annual income (GT1-27.1)"),
        _i("guarantor_household_size",             "Household size (GT1-28)"),
        _s("guarantor_employer_id_number",         "Guarantor employer ID (GT1-29.1)"),
        _s("guarantor_marital_status_code",        "Guarantor marital status (GT1-30.1)"),
        _s("guarantor_hire_effective_date",        "Guarantor hire date (GT1-31)"),
        _s("employment_stop_date",                 "Employment stop date (GT1-32)"),
        _s("living_dependency",                    "Living dependency (GT1-33)"),
        _s("ambulatory_status",                    "Ambulatory status (GT1-34)"),
        _s("citizenship",                          "Citizenship (GT1-35.1)"),
        _s("primary_language",                     "Primary language (GT1-36.1)"),
        _s("living_arrangement",                   "Living arrangement (GT1-37)"),
        _s("publicity_code",                       "Publicity code (GT1-38.1)"),
        _s("protection_indicator",                 "Protection indicator Y/N (GT1-39)"),
        _s("student_indicator",                    "Student status (GT1-40)"),
        _s("religion",                             "Religion (GT1-41.1)"),
        _s("mothers_maiden_name",                  "Mother's maiden name (GT1-42.1)"),
        _s("nationality",                          "Nationality (GT1-43.1)"),
        _s("ethnic_group",                         "Ethnic group (GT1-44.1)"),
        _s("contact_persons_name",                 "Contact person name (GT1-45.1)"),
        _s("contact_persons_telephone_number",     "Contact person phone (GT1-46)"),
        _s("contact_reason",                       "Contact reason (GT1-47.1)"),
        _s("contact_relationship",                 "Contact relationship (GT1-48)"),
        _s("job_title",                            "Job title (GT1-49)"),
        _s("job_code_class",                       "Job code/class (GT1-50.1)"),
        _s("guarantor_employers_org_name",         "Guarantor employer's org name (GT1-51.1)"),
        _s("handicap",                             "Handicap code (GT1-52)"),
        _s("job_status",                           "Job status (GT1-53)"),
        _s("guarantor_financial_class",            "Financial class (GT1-54.1)"),
        _s("guarantor_race",                       "Guarantor race (GT1-55.1)"),
        _s("guarantor_birth_place",                "Guarantor birth place (GT1-56)"),
        _s("vip_indicator",                        "VIP indicator (GT1-57)"),
    ]
)

# ---------------------------------------------------------------------------
# FT1 — Financial Transaction
# ---------------------------------------------------------------------------

FT1_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                              "Sequence number for this FT1 segment within the message (FT1-1)"),
        _s("transaction_id",                          "Unique transaction identifier (FT1-2)"),
        _s("transaction_batch_id",                    "Batch identifier (FT1-3)"),
        _s("transaction_date",                        "Transaction date/time range start (FT1-4.1)"),
        _ts("transaction_posting_date",               "Posting date/time (FT1-5)"),
        _s("transaction_type",                        "Transaction type (FT1-6): CG=Charge, CR=Credit, PA=Payment, AJ=Adjustment"),
        _s("transaction_code",                        "Transaction/charge code raw (FT1-7)"),
        _s("transaction_code_id",                     "Transaction code identifier (FT1-7.1)"),
        _s("transaction_code_text",                   "Transaction code text (FT1-7.2)"),
        _s("transaction_description",                 "Transaction description (FT1-8, deprecated)"),
        _s("transaction_description_alt",             "Alternate transaction description (FT1-9, deprecated)"),
        _i("transaction_quantity",                    "Transaction quantity (FT1-10)"),
        _s("transaction_amount_extended",             "Extended amount: quantity x unit price (FT1-11.1)"),
        _s("transaction_amount_unit",                 "Unit price (FT1-12.1)"),
        _s("department_code",                         "Department code (FT1-13.1)"),
        _s("insurance_plan_id",                       "Insurance plan identifier (FT1-14.1)"),
        _s("insurance_amount",                        "Insurance amount (FT1-15.1)"),
        _s("assigned_patient_location",               "Patient location (FT1-16)"),
        _s("fee_schedule",                            "Fee schedule (FT1-17)"),
        _s("patient_type",                            "Patient type (FT1-18)"),
        _s("diagnosis_code",                          "Diagnosis code (FT1-19.1)"),
        _s("performed_by_code",                       "Performer identifier (FT1-20.1)"),
        _s("ordered_by_code",                         "Ordering provider identifier (FT1-21.1)"),
        _s("unit_cost",                               "Unit cost (FT1-22.1)"),
        _s("filler_order_number",                     "Filler order number (FT1-23.1)"),
        _s("entered_by_code",                         "Entered by identifier (FT1-24.1)"),
        _s("procedure_code",                          "Procedure code (FT1-25.1)"),
        _s("procedure_code_modifier",                 "Procedure modifier (FT1-26.1)"),
        _s("advanced_beneficiary_notice_code",        "ABN code (FT1-27.1)"),
        _s("medically_necessary_dup_proc_reason",     "Duplicate procedure reason (FT1-28.1)"),
        _s("ndc_code",                                "National Drug Code (FT1-29.1)"),
        _s("payment_reference_id",                    "Payment reference (FT1-30.1)"),
        _s("transaction_reference_key",               "Transaction reference key (FT1-31)"),
        _s("performing_facility",                     "Performing facility (FT1-32.1, v2.6+)"),
        _s("ordering_facility",                       "Ordering facility (FT1-33.1, v2.6+)"),
        _s("item_number",                             "Item number (FT1-34.1, v2.6+)"),
        _s("model_number",                            "Model number (FT1-35, v2.6+)"),
        _s("special_processing_code",                 "Special processing code (FT1-36.1, v2.6+)"),
        _s("clinic_code",                             "Clinic code (FT1-37.1, v2.6+)"),
        _s("referral_number",                         "Referral number (FT1-38.1, v2.6+)"),
        _s("authorization_number",                    "Authorization number (FT1-39.1, v2.6+)"),
        _s("service_provider_taxonomy_code",          "Service provider taxonomy code (FT1-40.1, v2.6+)"),
        _s("revenue_code",                            "Revenue code (FT1-41.1, v2.6+)"),
        _s("prescription_number",                     "Prescription number (FT1-42, v2.6+)"),
        _s("ndc_qty_and_uom",                         "NDC quantity and unit of measure (FT1-43, v2.6+)"),
        _s("dme_certificate_of_medical_necessity_transmission_code", "DME certificate of medical necessity transmission code (FT1-44.1, v2.9+)"),
        _s("dme_certification_type_code",             "DME certification type code (FT1-45.1, v2.9+)"),
        _s("dme_duration_value",                      "DME duration value (FT1-46, v2.9+)"),
        _s("dme_certification_revision_date",         "DME certification revision date (FT1-47, v2.9+)"),
        _s("dme_initial_certification_date",          "DME initial certification date (FT1-48, v2.9+)"),
        _s("dme_last_certification_date",             "DME last certification date (FT1-49, v2.9+)"),
        _s("dme_length_of_medical_necessity_days",    "DME length of medical necessity in days (FT1-50, v2.9+)"),
        _s("dme_rental_price",                        "DME rental price (FT1-51, v2.9+)"),
        _s("dme_purchase_price",                      "DME purchase price (FT1-52, v2.9+)"),
        _s("dme_frequency_code",                      "DME frequency code (FT1-53.1, v2.9+)"),
        _s("dme_certification_condition_indicator",   "DME certification condition indicator (FT1-54, v2.9+)"),
        _s("dme_condition_indicator_code",            "DME condition indicator code (FT1-55.1, v2.9+)"),
        _s("service_reason_code",                     "Service reason code (FT1-56.1, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# RXA — Pharmacy/Treatment Administration
# ---------------------------------------------------------------------------

RXA_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _pk_i("set_id",                                "Give sub-ID counter (RXA-1)"),
        _i("administration_sub_id_counter",            "Administration sub-ID counter (RXA-2)"),
        _ts("datetime_start_of_administration",        "Administration start date/time (RXA-3)"),
        _ts("datetime_end_of_administration",          "Administration end date/time (RXA-4)"),
        _s("administered_code",                        "Drug/vaccine code raw (RXA-5)"),
        _s("administered_code_id",                     "Drug/vaccine code identifier (RXA-5.1)"),
        _s("administered_code_text",                   "Drug/vaccine code text (RXA-5.2)"),
        _s("administered_amount",                      "Amount administered (RXA-6)"),
        _s("administered_units",                       "Units of measure (RXA-7.1)"),
        _s("administered_dosage_form",                 "Dosage form (RXA-8.1)"),
        _s("administration_notes",                     "Administration notes (RXA-9.1)"),
        _s("administering_provider",                   "Provider who administered (RXA-10.1)"),
        _s("administered_at_location",                 "Administration location (RXA-11)"),
        _s("administered_per_time_unit",               "Rate time unit (RXA-12)"),
        _s("administered_strength",                    "Strength administered (RXA-13)"),
        _s("administered_strength_units",              "Strength units (RXA-14.1)"),
        _s("substance_lot_number",                     "Lot number (RXA-15)"),
        _ts("substance_expiration_date",               "Substance expiration date (RXA-16)"),
        _s("substance_manufacturer_name",              "Manufacturer name (RXA-17.1)"),
        _s("substance_treatment_refusal_reason",       "Refusal reason (RXA-18.1)"),
        _s("indication",                               "Indication for administration (RXA-19.1)"),
        _s("completion_status",                        "Completion status (RXA-20): CP=Complete, RE=Refused, NA=Not Administered, PA=Partial"),
        _s("action_code_rxa",                          "Action code (RXA-21)"),
        _ts("system_entry_datetime",                   "System entry date/time (RXA-22)"),
        _s("administered_drug_strength_volume",        "Drug strength volume (RXA-23)"),
        _s("administered_drug_strength_volume_units",  "Drug strength volume units (RXA-24.1)"),
        _s("administered_barcode_identifier",          "Barcode identifier (RXA-25.1)"),
        _s("pharmacy_order_type",                      "Pharmacy order type (RXA-26)"),
        _s("administer_at",                            "Administration location (RXA-27, v2.6+)"),
        _s("administered_at_address",                  "Administered-at address (RXA-28, v2.6+)"),
        _s("administered_tag_identifier",              "Administered tag identifier (RXA-29.1, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# SCH — Scheduling Activity Information
# ---------------------------------------------------------------------------

SCH_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _s("placer_appointment_id",        "Appointment ID from placer (SCH-1.1)"),
        _s("filler_appointment_id",        "Appointment ID from filler (SCH-2.1)"),
        _i("occurrence_number",            "Occurrence number (SCH-3)"),
        _s("placer_group_number",          "Placer group number (SCH-4.1)"),
        _s("schedule_id",                  "Schedule identifier (SCH-5.1)"),
        _s("event_reason",                 "Event reason (SCH-6.1)"),
        _s("appointment_reason",           "Reason for appointment (SCH-7.1)"),
        _s("appointment_type",             "Appointment type (SCH-8.1)"),
        _i("appointment_duration",         "Appointment duration in minutes (SCH-9, deprecated)"),
        _s("appointment_duration_units",   "Duration units (SCH-10.1, deprecated)"),
        _s("appointment_timing_quantity",  "Appointment timing quantity (SCH-11, deprecated)"),
        _s("placer_contact_person",        "Placer contact person (SCH-12.1)"),
        _s("placer_contact_phone_number",  "Placer contact phone (SCH-13)"),
        _s("placer_contact_address",       "Placer contact address (SCH-14)"),
        _s("placer_contact_location",      "Placer contact location (SCH-15)"),
        _s("filler_contact_person",        "Filler contact person (SCH-16.1)"),
        _s("filler_contact_phone_number",  "Filler contact phone (SCH-17)"),
        _s("filler_contact_address",       "Filler contact address (SCH-18)"),
        _s("filler_contact_location",      "Filler contact location (SCH-19)"),
        _s("entered_by_person",            "Person who entered the schedule (SCH-20.1)"),
        _s("entered_by_phone_number",      "Entered by phone number (SCH-21)"),
        _s("entered_by_location",          "Entered by location (SCH-22)"),
        _s("parent_placer_appointment_id", "Parent placer appointment ID (SCH-23.1)"),
        _s("parent_filler_appointment_id", "Parent filler appointment ID (SCH-24.1)"),
        _s("filler_status_code",           "Filler status code (SCH-25.1)"),
        _s("placer_order_number",          "Placer order number (SCH-26.1)"),
        _s("filler_order_number",          "Filler order number (SCH-27.1)"),
        _s("alternate_placer_order_group_number", "Alternate placer order group number (SCH-28.1, v2.9+)"),
    ]
)

# ---------------------------------------------------------------------------
# TXA — Transcription Document Header
# ---------------------------------------------------------------------------

TXA_SCHEMA = StructType(
    _METADATA_FIELDS
    + [
        _i("set_id",                              "Sequence number (TXA-1)"),
        _s("document_type",                        "Document type (TXA-2): DS=Discharge Summary, HP=History and Physical, OP=Operative Note"),
        _s("document_content_presentation",        "Content presentation type (TXA-3)"),
        _ts("activity_datetime",                   "Activity date/time (TXA-4)"),
        _s("primary_activity_provider",            "Primary activity provider (TXA-5.1)"),
        _ts("origination_datetime",                "Origination date/time (TXA-6)"),
        _ts("transcription_datetime",              "Transcription date/time (TXA-7)"),
        _ts("edit_datetime",                       "Edit date/time (TXA-8)"),
        _s("originator",                           "Originator identifier (TXA-9.1)"),
        _s("assigned_document_authenticator",      "Assigned authenticator (TXA-10.1)"),
        _s("transcriptionist",                     "Transcriptionist identifier (TXA-11.1)"),
        _s("unique_document_number",               "Unique document identifier (TXA-12.1)"),
        _s("parent_document_number",               "Parent document identifier (TXA-13.1)"),
        _s("placer_order_number",                  "Placer order number (TXA-14.1)"),
        _s("filler_order_number",                  "Filler order number (TXA-15.1)"),
        _s("unique_document_file_name",            "Document file name (TXA-16)"),
        _s("document_completion_status",           "Completion status (TXA-17): DI=Dictated, DO=Documented, AU=Authenticated, LA=Legally Authenticated"),
        _s("document_confidentiality_status",      "Confidentiality status (TXA-18)"),
        _s("document_availability_status",         "Availability status (TXA-19)"),
        _s("document_storage_status",              "Storage status (TXA-20)"),
        _s("document_change_reason",               "Reason for document change (TXA-21)"),
        _s("authentication_person_time_stamp",     "Authenticator with timestamp (TXA-22)"),
        _s("distributed_copies",                   "Recipients of distributed copies (TXA-23.1)"),
        _s("folder_assignment",                    "Folder assignment (TXA-24.1, v2.6+)"),
        _s("document_title",                       "Document title (TXA-25, v2.6+)"),
        _ts("agreed_due_datetime",                 "Agreed due date/time (TXA-26, v2.8+)"),
        _s("creating_facility",                    "Creating facility (TXA-27.1, v2.8+)"),
        _s("creating_specialty",                   "Creating specialty (TXA-28.1, v2.8+)"),
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
SEGMENT_TABLES: list[str] = [
    "msh", "evn", "pid", "pd1", "pv1", "pv2", "nk1", "mrg",
    "al1", "iam", "dg1", "pr1",
    "orc", "obr", "obx", "nte", "spm",
    "in1", "gt1", "ft1",
    "rxa", "sch", "txa",
]

#: Map from lowercase segment name → StructType.
SEGMENT_SCHEMAS: dict[str, StructType] = {
    "msh": MSH_SCHEMA,
    "evn": EVN_SCHEMA,
    "pid": PID_SCHEMA,
    "pd1": PD1_SCHEMA,
    "pv1": PV1_SCHEMA,
    "pv2": PV2_SCHEMA,
    "nk1": NK1_SCHEMA,
    "mrg": MRG_SCHEMA,
    "al1": AL1_SCHEMA,
    "iam": IAM_SCHEMA,
    "dg1": DG1_SCHEMA,
    "pr1": PR1_SCHEMA,
    "orc": ORC_SCHEMA,
    "obr": OBR_SCHEMA,
    "obx": OBX_SCHEMA,
    "nte": NTE_SCHEMA,
    "spm": SPM_SCHEMA,
    "in1": IN1_SCHEMA,
    "gt1": GT1_SCHEMA,
    "ft1": FT1_SCHEMA,
    "rxa": RXA_SCHEMA,
    "sch": SCH_SCHEMA,
    "txa": TXA_SCHEMA,
}


def get_schema(segment_type: str) -> StructType:
    """Return the StructType for *segment_type* (case-insensitive).

    Falls back to :data:`GENERIC_SEGMENT_SCHEMA` for unknown / Z-segments.
    """
    return SEGMENT_SCHEMAS.get(segment_type.lower(), GENERIC_SEGMENT_SCHEMA)
