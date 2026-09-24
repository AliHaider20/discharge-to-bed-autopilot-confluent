"""
Avro schemas for the Discharge-to-Bed Autopilot.
These get registered in Confluent Schema Registry.
"""

# ED arrival event — sourced from real MIMIC-IV-ED data
ED_ARRIVAL_SCHEMA = {
    "type": "record",
    "name": "EdArrival",
    "namespace": "hospital.ed",
    "fields": [
        {"name": "stay_id", "type": "long", "doc": "Unique ED stay identifier"},
        {"name": "subject_id", "type": "long", "doc": "Patient identifier"},
        {"name": "arrival_time", "type": "string", "doc": "ISO timestamp of ED arrival"},
        {"name": "acuity", "type": ["null", "int"], "default": None, "doc": "ESI triage acuity 1-5 (1=most urgent)"},
        {"name": "chief_complaint", "type": ["null", "string"], "default": None},
        {"name": "arrival_transport", "type": "string", "doc": "AMBULANCE, WALK IN, etc."},
        {"name": "status", "type": "string", "doc": "IN_ED, WAITING_FOR_BED, DISCHARGED, ADMITTED"},
    ]
}

# ADT (Admit/Discharge/Transfer) event
ADT_EVENT_SCHEMA = {
    "type": "record",
    "name": "AdtEvent",
    "namespace": "hospital.adt",
    "fields": [
        {"name": "event_id", "type": "string", "doc": "Unique event identifier"},
        {"name": "stay_id", "type": "long", "doc": "Links to the ED stay"},
        {"name": "subject_id", "type": "long", "doc": "Patient identifier"},
        {"name": "event_type", "type": "string", "doc": "ADMIT, DISCHARGE, TRANSFER"},
        {"name": "unit_id", "type": "string", "doc": "Hospital unit e.g. MED_SURG_4W"},
        {"name": "bed_id", "type": "string", "doc": "Specific bed e.g. 4W-12"},
        {"name": "event_time", "type": "string", "doc": "ISO timestamp"},
    ]
}

# Bed status event
BED_STATUS_SCHEMA = {
    "type": "record",
    "name": "BedStatus",
    "namespace": "hospital.bed",
    "fields": [
        {"name": "bed_id", "type": "string", "doc": "Bed identifier e.g. 4W-12"},
        {"name": "unit_id", "type": "string", "doc": "Unit e.g. MED_SURG_4W"},
        {"name": "status", "type": "string", "doc": "OCCUPIED, DIRTY, CLEAN, AVAILABLE"},
        {"name": "patient_stay_id", "type": ["null", "long"], "default": None, "doc": "Current patient if occupied"},
        {"name": "status_since", "type": "string", "doc": "ISO timestamp of status change"},
    ]
}
