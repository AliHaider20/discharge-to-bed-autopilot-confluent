"""
Discharge-to-Bed Autopilot — Live Dashboard (Avro + JSON Support)
"""

import json
import os
import time
from collections import defaultdict
import streamlit as st
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config & Schema Registry Setup
# ---------------------------------------------------------------------------
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_SECRET = os.getenv("KAFKA_API_SECRET")
SR_URL = os.getenv("SR_URL")
SR_KEY = os.getenv("SR_API_KEY")
SR_SECRET = os.getenv("SR_API_SECRET")

# Initialize Schema Registry client for Avro topics
sr_conf = {"url": SR_URL, "basic.auth.user.info": f"{SR_KEY}:{SR_SECRET}"}
sr_client = SchemaRegistryClient(sr_conf)
avro_deserializer = AvroDeserializer(sr_client)


def consume_all_available(topic: str, max_messages: int = 1000, timeout: float = 6.0):
    """Consume messages from Kafka using Avro or JSON deserialization."""
    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": KAFKA_KEY,
        "sasl.password": KAFKA_SECRET,
        "group.id": f"dashboard-reader-{topic}-{int(time.time())}",
        "auto.offset.reset": "earliest",
    }
    
    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    messages = []

    deadline = time.time() + timeout
    empty_polls = 0

    while time.time() < deadline and len(messages) < max_messages:
        msg = consumer.poll(0.5)
        if msg is None:
            if len(messages) > 0:
                empty_polls += 1
                if empty_polls >= 3:  # Exit quickly once message stream ends
                    break
            continue

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.error(f"Consumer error on {topic}: {msg.error()}")
            continue

        # Try Avro Deserialization first (for producer.py topics)
        try:
            ctx = SerializationContext(topic, MessageField.VALUE)
            value = avro_deserializer(msg.value(), ctx)
            if value:
                messages.append(value)
                continue
        except Exception:
            pass

        # Fallback to JSON Deserialization (for Flink SQL JSON topics)
        try:
            val_bytes = msg.value()
            if val_bytes:
                if len(val_bytes) > 5 and val_bytes[0] == 0:
                    val_bytes = val_bytes[5:]  # Strip Schema Registry header
                value = json.loads(val_bytes.decode("utf-8"))
                messages.append(value)
        except Exception:
            pass

    consumer.close()
    return messages


# ---------------------------------------------------------------------------
# Dashboard UI
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Bed Autopilot", layout="wide")
st.title("Discharge-to-Bed Autopilot")
st.caption("Real-time bed management powered by Confluent + MIMIC-IV-ED data")

if st.button("Refresh Data"):
    st.rerun()

with st.spinner("Fetching latest stream from Confluent Cloud..."):
    bed_data = consume_all_available("bed_status", timeout=6.0)
    ed_data = consume_all_available("ed_arrivals", timeout=6.0)
    alerts = consume_all_available("escalation_alerts", timeout=4.0)

# Build latest state per bed
latest_beds = {}
for event in bed_data:
    bed_id = event.get("bed_id", "")
    if bed_id:
        latest_beds[bed_id] = event

occupied = sum(1 for b in latest_beds.values() if b.get("status") == "OCCUPIED")
dirty = sum(1 for b in latest_beds.values() if b.get("status") == "DIRTY")
available = sum(1 for b in latest_beds.values() if b.get("status") == "AVAILABLE")
total = max(len(latest_beds), 1)

waiting = sum(1 for e in ed_data if e.get("status") == "WAITING_FOR_BED")

# KPI Summary
col1, col2, col3, col4 = st.columns(4)
col1.metric("Occupied Beds", occupied, f"{occupied * 100 // total}%")
col2.metric("Dirty (Awaiting Clean)", dirty)
col3.metric("Available Beds", available)
col4.metric("ED Patients Boarding", waiting)

# Unit Summary Table
st.subheader("Bed Status by Unit")
unit_summary = defaultdict(lambda: {"OCCUPIED": 0, "DIRTY": 0, "AVAILABLE": 0})
for bed in latest_beds.values():
    unit = bed.get("unit_id", "UNKNOWN")
    status = bed.get("status", "UNKNOWN")
    if status in unit_summary[unit]:
        unit_summary[unit][status] += 1

if unit_summary:
    rows = []
    for unit, counts in sorted(unit_summary.items()):
        total_u = sum(counts.values())
        rows.append({
            "Unit": unit,
            "Occupied": counts["OCCUPIED"],
            "Dirty": counts["DIRTY"],
            "Available": counts["AVAILABLE"],
            "Occupancy %": f"{counts['OCCUPIED'] * 100 // max(total_u, 1)}%",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.info("No bed status data received yet. Confirm `producer.py` is actively producing events.")

# ED Boarding Queue Table
st.subheader("ED Boarding Queue")
boarding = [e for e in ed_data if e.get("status") == "WAITING_FOR_BED"]
if boarding:
    boarding_df = pd.DataFrame(boarding)[["stay_id", "acuity", "arrival_time", "chief_complaint"]]
    boarding_df = boarding_df.sort_values("acuity", ascending=True)
    st.dataframe(boarding_df, use_container_width=True, hide_index=True)
else:
    st.success("No patients currently boarding. All beds assigned.")

# Escalation Alerts Feed
st.subheader("Escalation Alerts")
if alerts:
    for alert in alerts[-10:]:
        msg = alert.get("message", str(alert))
        st.warning(msg)
else:
    st.success("No active alerts.")