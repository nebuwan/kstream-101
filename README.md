# kstream-101 — Python + Kafka basics

A minimal tutorial. Two scripts. One concept at a time.

---

## What is Kafka (in one paragraph)

Kafka is a **distributed log**. Producers write messages to a named **topic**;
consumers read them back. Unlike a queue, messages aren't deleted after being
read — they stay in the log for a configurable period. This means many
independent consumers can read the same topic, each tracking their own
position (called an **offset**).

---

## Setup

**Prerequisites:** Docker Desktop running, [uv](https://docs.astral.sh/uv/) installed.

```bash
# 1. Create the virtual environment and install dependencies
uv sync

# 2. Start Kafka (runs in the background)
docker compose up -d
```

---

## Step 1 — Send some messages (producer)

```bash
uv run python producer.py
```

The producer connects to Kafka and sends 5 messages to a topic called
`greetings`. The topic is created automatically on first use.

**Key idea:** `.send()` queues a message locally; `.flush()` waits until Kafka
has actually received them. Always call `flush()` before your script exits.

---

## Step 2 — Read them back (consumer)

Open a second terminal:

```bash
uv run python consumer.py
```

You'll see all 5 messages printed with their `partition` and `offset`.

**Key idea:** The consumer remembers its position (`offset`) so if you stop and
restart it, it picks up where it left off — it won't re-read old messages.

Try it: stop the consumer (Ctrl+C), run the producer again, restart the consumer.
It will only show the *new* 5 messages.

**Key idea:** `auto_offset_reset="earliest"` only applies the *first* time a
`group_id` connects. After that, the saved offset takes over.

To force a re-read from the beginning, change `group_id` to a new value.

---

## Step 3 — Run the producer while the consumer is running

Leave the consumer running, then in another terminal:

```bash
uv run python producer.py
```

Messages appear in the consumer in real time. This is the core of stream
processing — one process produces, another consumes, continuously.

---

## Core concepts recap

| Concept | What it means |
|---|---|
| **Topic** | A named log. Think of it like a table in a database, but append-only. |
| **Message** | A key + value (both are just bytes). The value is your payload. |
| **Offset** | The position of a message in a partition. Monotonically increasing. |
| **Producer** | Writes messages to a topic. |
| **Consumer** | Reads messages from a topic, tracking its offset. |
| **group_id** | Labels where a consumer left off. Change it to start fresh. |
| **auto_offset_reset** | What to do on first connect: `"earliest"` or `"latest"`. |

---

## Stop Kafka

```bash
docker compose down
```
