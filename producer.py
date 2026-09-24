"""
Discharge-to-Bed Autopilot — Event Producer

Replays real MIMIC-IV-ED Demo patient arrivals as a Kafka stream,
then simulates realistic bed operations (admit, discharge, clean)
on top of the real ED data.

Usage:
    python producer.py
    python producer.py --speed 20    # 20x faster (1 real hour = 3 min)
    python producer.py --speed 60    # 60x faster (1 real hour = 1 min)
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from dotenv import load_dotenv

from schemas import ED_ARRIVAL_SCHEMA, ADT_EVENT_SCHEMA, BED_STATUS_SCHEMA

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS")
KAFKA_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_SECRET = os.getenv("KAFKA_API_SECRET")
SR_URL = os.getenv("SR_URL")
SR_KEY = os.getenv("SR_API_KEY")
SR_SECRET = os.getenv("SR_API_SECRET")

# Hospital layout — 5 units, 20 beds each = 100 beds
UNITS = {
    "MED_SURG_4W": [f"4W-{i:02d}" for i in range(1, 21)],
    "MED_SURG_5E": [f"5E-{i:02d}" for i in range(1, 21)],
    "CARDIOLOGY":  [f"CARD-{i:02d}" for i in range(1, 21)],
    "ICU":         [f"ICU-{i:02d}" for i in range(1, 21)],
    "OBSERVATION": [f"OBS-{i:02d}" for i in range(1, 21)],
}

# Average times (in minutes) for simulated bed lifecycle
AVG_LOS_MINUTES = 240         # average inpatient stay: 4 hours (compressed for demo)
AVG_CLEAN_MINUTES = 35        # average bed turnaround: 35 min
LOS_JITTER = 120              # +/- random jitter on LOS
CLEAN_JITTER = 20             # +/- random jitter on cleaning


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_mimic_data(data_dir: str = "data") -> pd.DataFrame:
    """Load and join edstays + triage from MIMIC-IV-ED Demo."""
    edstays_path = os.path.join(data_dir, "edstays.csv")
    triage_path = os.path.join(data_dir, "triage.csv")

    # Try .gz if uncompressed not found
    if not os.path.exists(edstays_path):
        edstays_path = os.path.join(data_dir, "edstays.csv.gz")
    if not os.path.exists(triage_path):
        triage_path = os.path.join(data_dir, "triage.csv.gz")

    edstays = pd.read_csv(edstays_path)
    triage = pd.read_csv(triage_path)

    # Join on stay_id
    merged = edstays.merge(triage[["stay_id", "acuity", "chiefcomplaint"]], on="stay_id", how="left")

    # Parse timestamps
    merged["intime"] = pd.to_datetime(merged["intime"])
    merged["outtime"] = pd.to_datetime(merged["outtime"])

    # Sort by arrival time
    merged = merged.sort_values("intime").reset_index(drop=True)

    print(f"Loaded {len(merged)} ED stays from MIMIC-IV-ED Demo")
    print(f"  Admitted: {(merged['disposition'] == 'ADMITTED').sum()}")
    print(f"  Discharged/Other: {(merged['disposition'] != 'ADMITTED').sum()}")
    return merged


def pick_unit_and_bed(acuity, bed_tracker: dict) -> tuple:
    """Assign a unit based on acuity, then pick a random available bed."""
    if acuity is not None and acuity <= 2:
        preferred_units = ["ICU", "CARDIOLOGY"]
    else:
        preferred_units = ["MED_SURG_4W", "MED_SURG_5E", "OBSERVATION"]

    random.shuffle(preferred_units)

    for unit_id in preferred_units:
        available = [b for b in UNITS[unit_id] if bed_tracker.get(b) == "AVAILABLE"]
        if available:
            bed_id = random.choice(available)
            return unit_id, bed_id

    # Fallback: try any unit
    all_units = list(UNITS.keys())
    random.shuffle(all_units)
    for unit_id in all_units:
        available = [b for b in UNITS[unit_id] if bed_tracker.get(b) == "AVAILABLE"]
        if available:
            return unit_id, random.choice(available)

    # No beds available — patient will board
    return None, None


class KafkaEventProducer:
    """Wraps Confluent Kafka producer with Avro serialization."""

    def __init__(self):
        sr_conf = {"url": SR_URL, "basic.auth.user.info": f"{SR_KEY}:{SR_SECRET}"}
        sr_client = SchemaRegistryClient(sr_conf)

        self.producer = Producer({
            "bootstrap.servers": BOOTSTRAP,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": KAFKA_KEY,
            "sasl.password": KAFKA_SECRET,
        })
        self.string_serializer = StringSerializer("utf_8")

        # Create Avro serializers for each topic
        self.ed_serializer = AvroSerializer(
            sr_client, json.dumps(ED_ARRIVAL_SCHEMA),
            lambda obj, ctx: obj,
        )
        self.adt_serializer = AvroSerializer(
            sr_client, json.dumps(ADT_EVENT_SCHEMA),
            lambda obj, ctx: obj,
        )
        self.bed_serializer = AvroSerializer(
            sr_client, json.dumps(BED_STATUS_SCHEMA),
            lambda obj, ctx: obj,
        )

    def _delivery_report(self, err, msg):
        if err:
            print(f"  DELIVERY FAILED: {err}")

    def send_ed_arrival(self, event: dict):
        ctx = SerializationContext("ed_arrivals", MessageField.VALUE)
        self.producer.produce(
            topic="ed_arrivals",
            key=self.string_serializer(str(event["stay_id"])),
            value=self.ed_serializer(event, ctx),
            on_delivery=self._delivery_report,
        )
        self.producer.poll(0)

    def send_adt_event(self, event: dict):
        ctx = SerializationContext("adt_events", MessageField.VALUE)
        self.producer.produce(
            topic="adt_events",
            key=self.string_serializer(str(event["stay_id"])),
            value=self.adt_serializer(event, ctx),
            on_delivery=self._delivery_report,
        )
        self.producer.poll(0)

    def send_bed_status(self, event: dict):
        ctx = SerializationContext("bed_status", MessageField.VALUE)
        self.producer.produce(
            topic="bed_status",
            key=self.string_serializer(event["bed_id"]),
            value=self.bed_serializer(event, ctx),
            on_delivery=self._delivery_report,
        )
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()


# ---------------------------------------------------------------------------
# Main simulation loop
# ---------------------------------------------------------------------------
def run(speed: int = 10):
    """
    Replay MIMIC-IV-ED arrivals as a stream, with simulated bed operations.

    Args:
        speed: Time compression factor. 10 = 1 real hour plays in 6 minutes.
    """
    df = load_mimic_data()
    kafka = KafkaEventProducer()

    # Initialize all beds as AVAILABLE
    bed_tracker = {}
    for unit_id, beds in UNITS.items():
        for bed_id in beds:
            bed_tracker[bed_id] = "AVAILABLE"

    # Track scheduled future events (discharge, cleaning)
    # Each entry: (sim_time, event_type, data_dict)
    future_events = []

    # Time compression setup
    sim_start_real = df["intime"].min()
    wall_start = datetime.now()

    print(f"\nStarting replay at {speed}x speed")
    print(f"  Sim time range: {df['intime'].min()} to {df['intime'].max()}")
    print(f"  1 real hour = {3600 / speed:.0f} seconds wall clock")
    print("-" * 60)

    arrival_idx = 0
    total_admitted = 0
    total_boarded = 0

    while arrival_idx < len(df) or future_events:
        # Current simulation time
        wall_elapsed = (datetime.now() - wall_start).total_seconds()
        sim_elapsed = timedelta(seconds=wall_elapsed * speed)
        sim_now = sim_start_real + sim_elapsed

        # Process any future events that are due
        due_events = [e for e in future_events if e[0] <= sim_now]
        future_events = [e for e in future_events if e[0] > sim_now]

        for _, etype, edata in sorted(due_events, key=lambda x: x[0]):
            if etype == "DISCHARGE":
                # Patient leaves -> bed becomes DIRTY
                kafka.send_adt_event(edata)
                bed_id = edata["bed_id"]
                unit_id = edata["unit_id"]
                bed_tracker[bed_id] = "DIRTY"

                bed_event = {
                    "bed_id": bed_id,
                    "unit_id": unit_id,
                    "status": "DIRTY",
                    "patient_stay_id": None,
                    "status_since": edata["event_time"],
                }
                kafka.send_bed_status(bed_event)

                # Schedule cleaning completion
                clean_min = AVG_CLEAN_MINUTES + random.randint(-CLEAN_JITTER, CLEAN_JITTER)
                clean_time = sim_now + timedelta(minutes=max(10, clean_min))
                future_events.append((clean_time, "CLEAN", {
                    "bed_id": bed_id,
                    "unit_id": unit_id,
                    "clean_time": clean_time.isoformat(),
                }))
                ts = edata["event_time"][:19]
                print(f"  [{ts}] DISCHARGE: bed {bed_id} -> DIRTY (clean in ~{clean_min}m)")

            elif etype == "CLEAN":
                # Bed is cleaned -> AVAILABLE
                bed_id = edata["bed_id"]
                bed_tracker[bed_id] = "AVAILABLE"

                bed_event = {
                    "bed_id": bed_id,
                    "unit_id": edata["unit_id"],
                    "status": "AVAILABLE",
                    "patient_stay_id": None,
                    "status_since": edata["clean_time"],
                }
                kafka.send_bed_status(bed_event)
                print(f"  [{edata['clean_time'][:19]}] CLEAN: bed {bed_id} -> AVAILABLE")

        # Process arrivals that are due
        while arrival_idx < len(df):
            row = df.iloc[arrival_idx]
            if row["intime"] > sim_now:
                break  # not yet time for this arrival

            stay_id = int(row["stay_id"])
            subject_id = int(row["subject_id"])
            acuity = int(row["acuity"]) if pd.notna(row.get("acuity")) else None
            complaint = str(row.get("chiefcomplaint", "")) if pd.notna(row.get("chiefcomplaint")) else None
            transport = str(row.get("arrival_transport", "UNKNOWN"))
            disposition = str(row.get("disposition", ""))

            # Determine initial status
            if disposition == "ADMITTED":
                status = "WAITING_FOR_BED"
            else:
                status = "IN_ED"

            # Send ED arrival
            ed_event = {
                "stay_id": stay_id,
                "subject_id": subject_id,
                "arrival_time": row["intime"].isoformat(),
                "acuity": acuity,
                "chief_complaint": complaint,
                "arrival_transport": transport,
                "status": status,
            }
            kafka.send_ed_arrival(ed_event)

            ts = row["intime"].isoformat()[:19]
            acuity_str = f"ESI-{acuity}" if acuity else "ESI-?"
            print(f"[{ts}] ED ARRIVAL: stay={stay_id} {acuity_str} transport={transport} -> {status}")

            # If admitted, try to assign a bed
            if disposition == "ADMITTED":
                total_admitted += 1
                unit_id, bed_id = pick_unit_and_bed(acuity, bed_tracker)

                if bed_id:
                    # Bed available -> ADMIT immediately
                    bed_tracker[bed_id] = "OCCUPIED"
                    admit_time = row["intime"] + timedelta(minutes=random.randint(5, 30))

                    adt_event = {
                        "event_id": str(uuid.uuid4()),
                        "stay_id": stay_id,
                        "subject_id": subject_id,
                        "event_type": "ADMIT",
                        "unit_id": unit_id,
                        "bed_id": bed_id,
                        "event_time": admit_time.isoformat(),
                    }
                    kafka.send_adt_event(adt_event)

                    bed_event = {
                        "bed_id": bed_id,
                        "unit_id": unit_id,
                        "status": "OCCUPIED",
                        "patient_stay_id": stay_id,
                        "status_since": admit_time.isoformat(),
                    }
                    kafka.send_bed_status(bed_event)

                    # Schedule future discharge
                    los_min = AVG_LOS_MINUTES + random.randint(-LOS_JITTER, LOS_JITTER)
                    discharge_time = admit_time + timedelta(minutes=max(60, los_min))
                    discharge_event = {
                        "event_id": str(uuid.uuid4()),
                        "stay_id": stay_id,
                        "subject_id": subject_id,
                        "event_type": "DISCHARGE",
                        "unit_id": unit_id,
                        "bed_id": bed_id,
                        "event_time": discharge_time.isoformat(),
                    }
                    future_events.append((discharge_time, "DISCHARGE", discharge_event))

                    print(f"  -> ADMIT to {bed_id} ({unit_id}), discharge in ~{los_min}m")
                else:
                    # No bed -> patient boards in ED
                    total_boarded += 1
                    print(f"  -> NO BED AVAILABLE. Patient boarding in ED. ({total_boarded} total boarded)")

                    # Update status to WAITING_FOR_BED
                    ed_update = {
                        "stay_id": stay_id,
                        "subject_id": subject_id,
                        "arrival_time": row["intime"].isoformat(),
                        "acuity": acuity,
                        "chief_complaint": complaint,
                        "arrival_transport": transport,
                        "status": "WAITING_FOR_BED",
                    }
                    kafka.send_ed_arrival(ed_update)

            arrival_idx += 1

        kafka.flush()

        # Status line
        occupied = sum(1 for s in bed_tracker.values() if s == "OCCUPIED")
        dirty = sum(1 for s in bed_tracker.values() if s == "DIRTY")
        avail = sum(1 for s in bed_tracker.values() if s == "AVAILABLE")
        pending = len(future_events)

        if arrival_idx < len(df) or future_events:
            print(f"  [Beds: {occupied} occupied, {dirty} dirty, {avail} available | "
                  f"Pending events: {pending} | Arrivals sent: {arrival_idx}/{len(df)}]")

        # Sleep a bit to not spin
        time.sleep(0.5)

    kafka.flush()
    print("\n" + "=" * 60)
    print(f"REPLAY COMPLETE")
    print(f"  Total ED arrivals: {len(df)}")
    print(f"  Total admitted: {total_admitted}")
    print(f"  Total who boarded (no bed): {total_boarded}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hospital event replay producer")
    parser.add_argument("--speed", type=int, default=10,
                        help="Time compression factor (default: 10, meaning 1 real hour = 6 min)")
    args = parser.parse_args()

    if args.speed <= 0:
        parser.error("--speed must be greater than 0")

    run(speed=args.speed)
