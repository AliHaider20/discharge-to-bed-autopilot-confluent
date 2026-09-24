"""
Register Avro schemas with Confluent Schema Registry.
Run this once before starting the producer.

Usage:
    python register_schemas.py
"""

import json
import os
import requests
from dotenv import load_dotenv
from schemas import ED_ARRIVAL_SCHEMA, ADT_EVENT_SCHEMA, BED_STATUS_SCHEMA

load_dotenv()

SR_URL = os.getenv("SR_URL")
SR_API_KEY = os.getenv("SR_API_KEY")
SR_API_SECRET = os.getenv("SR_API_SECRET")


def register_schema(subject: str, schema_dict: dict):
    """Register an Avro schema under the given subject."""
    url = f"{SR_URL}/subjects/{subject}/versions"
    payload = {
        "schemaType": "AVRO",
        "schema": json.dumps(schema_dict),
    }
    resp = requests.post(
        url,
        json=payload,
        auth=(SR_API_KEY, SR_API_SECRET),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    if resp.status_code in (200, 201):
        schema_id = resp.json().get("id")
        print(f"  Registered '{subject}' -> schema id {schema_id}")
    else:
        print(f"  FAILED '{subject}': {resp.status_code} {resp.text}")


def main():
    print("Registering schemas with Confluent Schema Registry...")
    print(f"  SR URL: {SR_URL}\n")

    # Register value schemas (topic-name + "-value" is the convention)
    register_schema("ed_arrivals-value", ED_ARRIVAL_SCHEMA)
    register_schema("adt_events-value", ADT_EVENT_SCHEMA)
    register_schema("bed_status-value", BED_STATUS_SCHEMA)

    print("\nDone. You can verify in Confluent Cloud -> Schema Registry.")


if __name__ == "__main__":
    main()
