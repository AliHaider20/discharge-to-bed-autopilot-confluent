# Confluent Cloud Setup

This guide creates the Confluent Cloud resources required by the producer,
Flink SQL statements, and Streamlit dashboard. Keep all credentials in a local
`.env` file; never paste them into source files or commit them.

## 1. Create an account
- Go to https://confluent.cloud
- Sign up and create an account with an available trial or billing plan.

## 2. Create an environment
- Left sidebar → "Environments" → "+ Add cloud environment"
- Name: `hospital-demo`
- Click "Create"

## 3. Enable Stream Governance
- Inside the `hospital-demo` environment
- You'll be prompted to enable Stream Governance (Essentials package is fine)
- Pick the SAME region you'll use for the cluster (e.g. AWS us-east-1)

## 4. Create a cluster
- Inside the environment → "Add cluster"
- Choose **Basic** (free tier, good enough for the demo)
- Pick cloud + region (e.g. AWS → us-east-1)
- Name: `hospital-cluster`
- Click "Launch cluster"

## 5. Create a Kafka API key
- Inside your cluster → "API Keys" (left sidebar)
- Click "Create key" → "My account" → "Global access"
- **COPY BOTH THE KEY AND SECRET NOW** — the secret is shown only once
- Save these in your `.env` file as `KAFKA_API_KEY` and `KAFKA_API_SECRET`

## 6. Get the bootstrap server
- Cluster → "Cluster Settings" (left sidebar)
- Copy the "Bootstrap server" URL (looks like `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`)
- Save in `.env` as `BOOTSTRAP_SERVERS`

## 7. Create a Schema Registry API key
- Go back to the environment level (click "hospital-demo" in breadcrumb)
- "Schema Registry" tab → "API credentials" → "Create key"
- **COPY BOTH** — save as `SR_API_KEY` and `SR_API_SECRET`
- Copy the Schema Registry URL (looks like `https://psrc-xxxxx.us-east-1.aws.confluent.cloud`)
- Save as `SR_URL`

## 8. Create topics
Go to your cluster → Topics → create these six topics (one partition is sufficient
for the demo; use the default retention settings):

```
ed_arrivals
adt_events
bed_status
unit_occupancy
cleaning_tasks
escalation_alerts
```

## 9. Create a Flink compute pool
- Left sidebar → "Flink" → "Compute pools" → "Create compute pool"
- **SAME region** as your cluster
- Size: 5 CFUs (smallest)
- Name: `hospital-flink`
- Note: this burns credits while running. Pause it when not testing.

## 10. Configure `.env`
Create `.env` in the project root. It should contain these names with your own
values:
```
BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_API_KEY=XXXXXXXXXX
KAFKA_API_SECRET=XXXXXXXXXXXXXXXXXXXXXXXX
SR_URL=https://psrc-xxxxx.us-east-1.aws.confluent.cloud
SR_API_KEY=XXXXXXXXXX
SR_API_SECRET=XXXXXXXXXXXXXXXXXXXXXXXX
```

## 11. Verify the connection
From the project root, run:

```bash
python register_schemas.py
```

You should see all three value schemas registered. Then start the producer and
dashboard as described in the README.

## Common mistakes
- Cluster, Schema Registry, and Flink pool must be in the **same region**
- API secret is shown only once — copy immediately
- Flink pool burns credits while running — pause when done
- The Schema Registry URL is different from the Kafka bootstrap server URL
- Do not commit `.env`, downloaded MIMIC data, or any API-key text files
