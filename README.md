# kstream-101 — Learning Kafka with Python

A beginner-friendly project for understanding how Kafka works using a real-world cell tower example.

---

## What is Kafka and why should I care?

Imagine a busy radio station. Towers are constantly sending signals, and multiple people want to listen at the same time — engineers, managers, analysts. Kafka is like the broadcast system in the middle that receives everything and lets anyone tune in whenever they want, without missing anything.

In more practical terms — Kafka sits between systems that produce data and systems that need to consume it. It stores everything in order, so if a consumer goes offline and comes back later, it picks up exactly where it left off.

---

## What this project does

We simulate 5 cell towers constantly sending metrics (signal strength, connected devices, network type) to Kafka. A consumer listens and prints everything as it arrives.

```
cell towers → producer.py → Kafka → consumer.py → your terminal
```

---

## Before you start

You'll need:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — runs Kafka locally on your machine
- [uv](https://docs.astral.sh/uv/getting-started/installation/) — manages your Python environment

---

## Getting started

**1. Install dependencies**
```bash
uv sync
```

**2. Start Kafka**
```bash
docker compose up -d
```
Wait about 15 seconds for it to fully start.

**3. Open two terminal windows**

Terminal 1 — start listening:
```bash
uv run python consumer.py
```

Terminal 2 — start sending:
```bash
uv run python producer.py
```

Watch Terminal 1 — messages from the towers will start appearing in real time.

---

## What you'll see

```
partition=0  offset=1  key=TOWER_EAST  |  signal=-69 dBm  devices=46  network=4G
partition=2  offset=2  key=TOWER_NORTH |  signal=-98 dBm  devices=315  network=5G
```

- **partition** — which "lane" this message was stored in
- **offset** — the position of this message (like a line number, starts at 0)
- **key** — which tower sent it (also controls which partition it goes to)

---

## Things to try

**Stop and restart the consumer** — it picks up exactly where it left off, no messages missed.

**Change the group_id in consumer.py** to a new name like `"my-new-group"` — it'll start reading from the very beginning again.

**Open a second consumer in a third terminal** with the same `group_id` — Kafka will split the towers between the two consumers automatically.

---

## Visual dashboard

You can see everything happening inside Kafka visually at:

**http://localhost:8080**

Browse your topics, read individual messages, and see how far behind your consumer is.

---

## Stop everything

```bash
docker compose down
```

---

## Key concepts

| Term | Plain English |
|---|---|
| **Topic** | A named stream of messages. Like a TV channel. |
| **Partition** | A lane within a topic. More lanes = more parallelism. |
| **Offset** | The position of a message. Kafka remembers where you left off. |
| **Producer** | The script that sends messages. |
| **Consumer** | The script that reads messages. |
| **Group ID** | Your bookmark name. Kafka uses it to remember your position. |
| **Broker** | The Kafka server itself (running in Docker). |
