# Discharge-to-Bed Autopilot

**Confluent DevDay Laptop Challenge Submission**

> Every 1 hour of ED boarding is linked to worse outcomes and lost revenue (~$10K/day).
> This app turns a discharge into a cleaning task and a bed assignment within seconds,
> cutting bed turnaround time and reducing ED boarding.

## Architecture

```
MIMIC-IV-ED Demo (real hospital data)
        │
        ▼
┌─────────────────┐     ┌──────────────────────┐
│  Python Replay   │────▶│  Confluent Cloud      │
│  Producer        │     │                      │
│  (ed_arrivals,   │     │  ┌─────────────┐     │
│   adt_events,    │     │  │ Flink SQL   │     │
│   bed_status)    │     │  │ ─────────── │     │
│                  │     │  │ Occupancy   │     │
└─────────────────┘     │  │ Boarding Q  │     │
                        │  │ Bottleneck  │     │
                        │  └──────┬──────┘     │
                        │         │            │
                        │  ┌──────▼──────┐     │
                        │  │ Output      │     │
                        │  │ Topics      │─────┼──▶ HTTP Sink Connector
                        │  └──────┬──────┘     │   (cleaning tasks &
                        │         │            │    escalation alerts)
                        └─────────┼────────────┘
                                  │
                                  ▼
                        ┌─────────────────┐
                        │ Streamlit       │
                        │ Dashboard       │
                        └─────────────────┘
```

## Challenge Requirements Met

| Requirement | Implementation |
|---|---|
| Business impact | Automates bed turnover, reducing ED boarding by ~1 hour = ~$10K/day recovered |
| Customer experience | Patients wait less; housekeeping gets instant task alerts |
| Connector(s) | HTTP Sink connector fires cleaning tasks to webhook |
| Stream Processing / Flink | 3 Flink SQL queries: occupancy, boarding queue, bottleneck alerts |
| Stream Governance | Avro schemas in Schema Registry + lineage graph |

## Data Source

ED arrivals and triage data from **MIMIC-IV-ED Demo** (100 real, deidentified patients
from Beth Israel Deaconess Medical Center). Bed operations (assignment, cleaning) are
simulated with published average turnaround times layered on top of the real ED events.

Source: https://physionet.org/content/mimic-iv-ed-demo/2.2/

## Setup

### Prerequisites
- Python 3.9+
- A Confluent Cloud account (free trial: https://confluent.cloud)
- Access to the MIMIC-IV-ED Demo data (free PhysioNet account and credentialed access)

### Step 1: Install dependencies
```bash
python -m pip install -r requirements.txt
```

### Step 2: Download MIMIC-IV-ED Demo
Go to https://physionet.org/content/mimic-iv-ed-demo/2.2/, request access if needed,
and download `edstays.csv.gz` and `triage.csv.gz` into the `data/` folder.
```bash
mkdir -p data
# place edstays.csv.gz and triage.csv.gz in data/
gunzip data/*.gz
```

### Step 3: Confluent Cloud setup
See SETUP_CONFLUENT.md for the click-by-click guide.

### Step 4: Create your `.env` file
```bash
# Create .env in the project root and fill in the values from SETUP_CONFLUENT.md.
```

Do not commit `.env`, API secrets, or downloaded patient data. These files are
excluded by `.gitignore`.

### Step 5: Register schemas
```bash
python register_schemas.py
```

### Step 6: Run the producer
```bash
python producer.py
```

The `--speed` value controls time compression. Higher values replay hospital
events faster. Here are five useful replay options:

#### Standard Speed (10x Default)
Replays hospital events at 10x real-time speed. One simulated hour compresses
into 6 minutes of wall-clock time.

```bash
python producer.py
```

#### Slower Replay for Step-by-Step Debugging (2x Speed)
Runs the replay slowly so individual events can be inspected in the console or
dashboard as they arrive. One simulated hour compresses into 30 minutes.

```bash
python producer.py --speed 2
```

#### Moderate Replay Speed (20x Speed)
Accelerates the timeline so one simulated hour passes in 3 minutes. This is
helpful for quick testing of Flink SQL queries.

```bash
python producer.py --speed 20
```

#### Fast Replay Speed (60x Speed)
Compresses one simulated hour into 1 minute, allowing a full 24-hour day of
hospital arrivals to stream through in approximately 24 minutes.

```bash
python producer.py --speed 60
```

#### Ultra-Fast Replay Speed (120x Speed)
Hyper-accelerates the replay so one simulated hour plays in 30 seconds. This is
useful for stress-testing Flink aggregations and Streamlit metrics across a long
time window.

```bash
python producer.py --speed 120
```

The producer requires the three input topics and Schema Registry credentials to
be configured first. Stop a replay with `Ctrl+C`.

### Step 7: Paste Flink SQL queries
Open Confluent Cloud → Flink → SQL Workspace.
Paste the queries from `flink_queries.sql` one at a time.

### Step 8: Set up the HTTP Sink connector (optional demo integration)
In Confluent Cloud, open Connectors → Add Connector → HTTP Sink. Point it at the
`cleaning_tasks` topic and use a temporary endpoint such as https://webhook.site
for a demo. Do not use a real patient-data endpoint.

### Step 9: Run the dashboard
```bash
streamlit run dashboard.py
```

### Step 10: Verify and capture lineage
Confirm messages are arriving in the input and output topics, then open
Confluent Cloud → Stream Governance → Stream Lineage for the submission screenshot.

### Troubleshooting
- `FileNotFoundError` for the CSV files: confirm both files are in `data/` and have
  been decompressed, or leave them as `.csv.gz` for the producer to read.
- Authentication errors: check the six Confluent variables in `.env`, including the
  exact Schema Registry URL and credentials.
- Empty dashboard: start the producer and Flink statements first, then refresh the
  dashboard after output topics contain messages.

## References

- Pines et al., "The Financial Consequences of Lost Demand and Reducing Boarding
  in Hospital Emergency Departments," Annals of Emergency Medicine, 2011.
- MIMIC-IV-ED: Johnson et al., PhysioNet, 2023. doi:10.13026/jzz5-vs76
