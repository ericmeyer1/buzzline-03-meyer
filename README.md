# buzzline-03-meyer: Manufacturing Streaming Pipelines

Streaming data does not have to be simple text. Many of us are familiar with streaming video content and audio (e.g., music) files.

Streaming data can be **structured** (CSV) or **semi-structured** (JSON). This project demonstrates both types using custom Kafka producers and consumers focused on **manufacturing data**. Each type uses its own Kafka topic.

See [.env](.env) for topic names and environment variables.

---

## First, Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in:

- [buzzline-01-case](https://github.com/denisecase/buzzline-01-case)
- [buzzline-02-case](https://github.com/denisecase/buzzline-02-case)

**Python 3.11 is required.**

---

## Second, Copy This Example Project & Rename

1. Fork or copy this project into your GitHub account.
2. Create a unique version of the project to run and experiment with.
3. Name it `buzzline-03-meyer`

---

## Task 0: Start WSL (Windows Only)

If using Windows, ensure WSL is installed. Open a **PowerShell terminal** in VS Code and run:

```powershell```
wsl

You are now in a Linux shell. All Kafka commands should run in this terminal.

---

## Task 1: Start Kafka

Prepare and start Kafka:

chmod +x scripts/prepare_kafka.sh
scripts/prepare_kafka.sh
cd ~/kafka
bin/kafka-server-start.sh config/kraft/server.properties

Keep this terminal open as you need Kafka running.

---

## Task 2: Manager local python environment

Windows
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install --upgrade pip wheel setuptools
py -m pip install --upgrade -r requirements.txt


If you get an execution policy error, run:

Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

Mac / Linux
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install --upgrade -r requirements.txt

---

## Task 3: Custom JSON producer

he json_producer_meyer.py producer streams manufacturing machine statuses as JSON messages.

Example JSON messages:

{"machine_id": "M001", "status": "running", "temperature": 75.2, "timestamp": "2025-09-07T08:00:00"}
{"machine_id": "M002", "status": "idle", "temperature": 70.0, "timestamp": "2025-09-07T08:01:00"}
{"machine_id": "M003", "status": "error", "temperature": 102.5, "timestamp": "2025-09-07T08:02:00", "error_code": "E101"}
{"machine_id": "M004", "status": "maintenance", "temperature": 68.5, "timestamp": "2025-09-07T08:03:00"}

Run JSON Producer

Windows:

.venv\Scripts\activate
py -m producers.json_producer_meyer


Mac / Linux:

source .venv/bin/activate
python3 -m producers.json_producer_meyer


Kafka topic: manufacturing_json_topic.

---

## Task 4: Custom JSON Consumer

The json_consumer_meyer.py consumer reads JSON messages and counts occurrences by machine status. It performs real-time analytics and logs results.

Run JSON Consumer

Windows:

.venv\Scripts\activate
py -m consumers.json_consumer_meyer


Mac / Linux:

source .venv/bin/activate
python3 -m consumers.json_consumer_meyer


Kafka topic: manufacturing_json_topic.

---

## Task 5: Custom CSV Producer:

The csv_producer_meyer.py producer streams sensor readings (temperature and vibration) from manufacturing machines as CSV messages.

Example CSV messages:

timestamp,temperature,vibration
2025-09-07 08:00:00,75.2,0.5
2025-09-07 08:01:00,70.0,0.2
2025-09-07 08:02:00,102.5,1.8
2025-09-07 08:03:00,68.5,0.1

Run CSV Producer

Windows:

.venv\Scripts\activate
py -m producers.csv_producer_meyer


Mac / Linux:

source .venv/bin/activate
python3 -m producers.csv_producer_meyer


Kafka topic: manufacturing_csv_topic.

---

## Task 6: Customer CSV Consumer

The csv_consumer_meyer.py consumer monitors CSV messages. It maintains a sliding window of recent readings and raises alerts if temperature spikes or vibration thresholds are exceeded.

Run CSV Consumer

Windows:

.venv\Scripts\activate
py -m consumers.csv_consumer_meyer


Mac / Linux:

source .venv/bin/activate
python3 -m consumers.csv_consumer_meyer


Kafka topic: manufacturing_csv_topic.

Stopping Producers or Consumers

Press CTRL + C in the terminal to stop a continuous process.