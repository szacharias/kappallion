# Docker Resource & Memory Profiles

This document outlines the memory and CPU resource tiers for the Cold-Chain Streaming Lakehouse platform.

---

## ⚠️ Important: Default Limited Resource Profile (Lean Mode)

By default, the platform runs in **Lean Mode** (Limited Resource Profile) to prevent memory exhaustion and disk thrashing on developer workstations (especially 16 GB RAM laptops running Windows/WSL2).

Unless you explicitly run the startup script with the `--full` attribute, the platform **will not allocate maximum memory** and will default to this trimmed footprint:

```powershell
# Default startup (Runs in Lean Mode ~2.4 GB max footprint):
python docker_flow/start_flow.py

# Or via unified CLI:
python docker_flow/flow.py start
```

---

## ⚡ How to Unlock Full Memory & Performance Mode (`--full`)

To grant the platform maximum JVM heaps, multi-threaded Spark execution (`local[*]`), and higher container memory limits, pass the `--full` flag:

```powershell
# Unrestricted Performance Mode (~4.5 GB+ footprint):
python docker_flow/start_flow.py --full

# Or via unified CLI:
python docker_flow/flow.py start --full
```

If you are running raw Docker Compose directly from the terminal without Python, you can apply the full profile using the override file:

```bash
docker compose -f docker-compose-2.yml -f docker/docker-compose.full.yml up -d
```

---

## 📊 Profile Comparison Matrix

| Component | Setting / Parameter | Lean Profile (Default) 🌱 | Full Profile (`--full`) ⚡ | Purpose of Trimming |
| :--- | :--- | :--- | :--- | :--- |
| **Spark Streaming** | `SPARK_MASTER` | `local[2]` | `local[*]` | Eliminates 8–16 duplicate worker thread buffers |
| **Spark Streaming** | `_JAVA_OPTIONS` (JVM) | `-Xms256m -Xmx768m` | `-Xms512m -Xmx1024m` | Prevents JVM garbage collection bloat |
| **Spark Streaming** | Memory Limit | `1024M` (1 GB) | `1536M` (1.5 GB) | Hard ceiling on container memory |
| **Kafka Broker** | `KAFKA_JVM_PERFORMANCE_OPTS` | `-Xms128m -Xmx256m` | `-Xms256m -Xmx512m` | KRaft dev broker requires minimal heap |
| **Kafka Broker** | Memory Limit | `384M` | `768M` | Hard ceiling on broker container |
| **MinIO S3** | Memory Limit | `384M` | `1024M` | Prevents unbounded S3 chunk caching |
| **Streamlit UI** | `_JAVA_OPTIONS` (JVM) | `-Xms128m -Xmx256m` | `-Xms256m -Xmx512m` | Trims dashboard PySpark driver heap |
| **Streamlit UI** | Memory Limit | `512M` | `1024M` | Caps dashboard container |
| **Simulator** | Memory Limit | `192M` | `256M` | Python telemetry publisher process |
| **Total Max RAM** | **Estimated Ceiling** | **~2.4 GB** | **~4.5 GB+** | **Leaves 13+ GB free for Windows & IDE** |

---

## 🛠️ Architecture Rationale (Clean Code & System Stability)
* **Single Responsibility**: Telemetry ingestion and delta compaction in development do not require 16 parallel threads. Limiting Spark to `local[2]` provides concurrent ingestion and compaction while slashing CPU and heap pressure.
* **Deterministic Resource Boundaries**: Every container has a hard memory ceiling (`deploy.resources.limits.memory`) to guarantee that Docker cannot starve the host operating system.
