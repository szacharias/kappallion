# Docker Resource & Memory Profiles

This document outlines the memory, CPU, and cluster deployment tiers for the Cold-Chain Streaming Lakehouse platform.

---

## ⚠️ Important: Default Limited Resource Profile (Lean Mode)

By default, the platform runs in **Lean Mode** (Single-Node Local Profile) to prevent memory exhaustion and disk thrashing on developer workstations (especially 16 GB RAM laptops running Windows/WSL2).

Unless you explicitly run the startup script with the `--full` attribute, the platform **will not allocate maximum memory or spin up redundant cluster daemons** and will default to this trimmed footprint:

```powershell
# Default startup (Runs in Lean Mode ~2.4 GB max footprint):
python docker_flow/start_flow.py

# Or via unified CLI:
python docker_flow/flow.py start
```

* **Cluster Topology:** Single container (`lakehouse-spark-delta`) running local Spark worker threads (`local[2]`).
* **Web UI:** Application streaming jobs view on **`http://localhost:4040`**.

---

## ⚡ How to Unlock Full Cluster & Performance Mode (`--full`)

To grant the platform maximum JVM heaps and **spin up a true distributed Spark Standalone Cluster** (with Master and Worker / Slave nodes visible in Docker Desktop), pass the `--full` flag:

```powershell
# Standalone Cluster Mode (~4.5 GB+ footprint):
python docker_flow/start_flow.py --full

# Or via unified CLI:
python docker_flow/flow.py start --full
```

If you are running raw Docker Compose directly from the terminal without Python, you can apply the full cluster profile using the override file:

```bash
docker compose -f docker-compose-2.yml -f docker/docker-compose.full.yml up -d
```

* **Cluster Topology:** 
  * `lakehouse-spark-master` (Cluster Coordinator on port `7077`)
  * `lakehouse-spark-worker-1` (Worker / Slave Node on port `8081`)
  * `lakehouse-spark-delta` (Streaming Driver submitting to `spark://spark-master:7077`)
* **Web UIs:** 
  * **Master & Slaves/Workers Dashboard:** **`http://localhost:8080`**
  * **Streaming Application Jobs View:** **`http://localhost:4040`**

---

## 📊 Profile Comparison Matrix

| Component | Setting / Parameter | Lean Profile (Default) 🌱 | Full Profile (`--full`) ⚡ | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **Spark Architecture** | Cluster Deployment | Single Container (`local[2]`) | **Master + Worker Containers** | Shows Master/Slaves on port 8080 |
| **Spark Master UI** | Cluster Overview | N/A (Local mode) | **`http://localhost:8080`** | Visual Master & Slaves table |
| **Spark App UI** | Jobs / Stages / Stream | `http://localhost:4040` | `http://localhost:4040` | Structured Streaming metrics |
| **Spark Master Node** | Memory Limit | None (Local mode) | `768M` | Coordinator daemon |
| **Spark Worker Node** | Memory Limit | None (Local mode) | `1024M` | Dedicated worker container |
| **Spark Driver** | Memory Limit | `1024M` (1 GB) | `1536M` (1.5 GB) | Streaming ingestion driver |
| **Kafka Broker** | Memory Limit | `384M` | `768M` | KRaft dev broker |
| **MinIO S3** | Memory Limit | `384M` | `1024M` | Prevents unbounded caching |
| **Streamlit Dashboard**| Memory Limit | `768M` (JVM 512m) | `1536M` (JVM 1024m) | Eliminates `INVALID_DRIVER_MEMORY` |
| **Simulator** | Memory Limit | `192M` | `256M` | Python telemetry publisher |
| **Total Stack RAM** | **Estimated Ceiling** | **~2.4 GB** | **~4.5 GB+** | **Keeps 13+ GB free on Lean mode** |

---

## 🛠️ Architecture Rationale (Clean Code & System Stability)
* **Single Responsibility**: Telemetry ingestion in development doesn't require 3 separate Java VMs by default. Running single-node local mode saves ~2 GB of base Java memory overhead.
* **Turnkey Scalability**: When you want to see the distributed cluster topology and inspect Master & Slaves, `--full` seamlessly layers `docker/docker-compose.full.yml` to spin up the cluster and expose port 8080 without manual YAML hacking.
