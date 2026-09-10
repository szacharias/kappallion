# Docker Environment Setup Guide

To run this Unified Streaming Lakehouse locally, you must set up Docker Desktop with enough system resources to handle concurrent JVM workloads (Kafka, MinIO, Spark, and Streamlit).

---

## 1. System Requirements

*   **Docker Desktop**: Install the latest version of Docker Desktop for Windows.
*   **WSL 2 Backend**: Ensure that Docker Desktop is configured to use the **WSL 2 based engine** (this is the default and provides the best performance on Windows).

---

## 2. Resource Allocation

By default, Docker Desktop may not allocate enough memory, causing Spark jobs to fail with `Out Of Memory` (OOM) errors or causing Kafka brokers to freeze.

### Recommended Allocations
*   **CPUs**: At least **4 Cores**.
*   **Memory**: At least **6 GB** (8 GB recommended).
*   **Swap**: 2 GB.

### How to configure on Windows (WSL 2)
For WSL 2, resource allocation is managed via a global configuration file on your host machine.

1. Open your Windows User Profile directory (`C:\Users\<Your-Username>\`).
2. Create or edit a file named `.wslconfig`.
3. Add the following configuration limits:
   ```ini
   [wsl2]
   memory=8GB      # Limit memory allocated to WSL2/Docker to 8GB
   processors=4    # Limit CPUs to 4 Cores
   ```
4. Save the file and restart WSL in PowerShell:
   ```powershell
   wsl --shutdown
   ```
5. Restart Docker Desktop.

---

## 3. WSL 2 Networking Troubleshooting

If your container applications are unable to talk to each other (e.g., Spark cannot resolve the Kafka broker endpoint `kafka:9092`), verify that:
*   Docker is running on the default `bridge` or custom compose network.
*   No other local services (such as a local PostgreSQL or Kafka instance) are binding to ports `9092` or `9000` on your host machine.
