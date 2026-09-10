# Running and Managing the Delta Lakehouse Stack

This guide provides the commands required to deploy, monitor, and query your Delta Lake platform.

---

## 1. Startup & Shutdown Commands

Run these commands inside your project root folder:

*   **Build the services** (fetches jars and installs Python packages):
    ```powershell
    docker compose -f docker-compose-2.yml build
    ```
*   **Start the stack** (launches simulator, Kafka, MinIO, Spark, and Streamlit):
    ```powershell
    docker compose -f docker-compose-2.yml up -d
    ```
*   **Stop the services** (graceful shutdown, preserves state):
    ```powershell
    docker compose -f docker-compose-2.yml stop
    ```
*   **Tear down the stack** (removes containers and network buffers):
    ```powershell
    docker compose -f docker-compose-2.yml down
    ```

---

## 2. Resource Management (Pause/Unpause)

To temporarily freeze resources without terminating JVM running processes:

*   **Pause container execution**:
    ```powershell
    docker compose -f docker-compose-2.yml pause
    ```
*   **Unpause/resume execution**:
    ```powershell
    docker compose -f docker-compose-2.yml unpause
    ```

---

## 3. Platform Monitoring & CLI Interaction

*   **Follow Spark Streaming Logs**:
    ```powershell
    docker logs -f lakehouse-spark-delta
    ```
*   **Run SQL CLI Verification**:
    Runs standard queries to inspect Bronze, Silver, and Gold Delta tables:
    ```powershell
    docker exec -it lakehouse-spark-delta /opt/spark/bin/spark-submit /opt/spark/apps/query_gold.py
    ```
*   **Trigger Delta Compaction & Vacuuming**:
    Launches optimization procedures to compact parquet files and purge transaction logs:
    ```powershell
    docker exec -it lakehouse-spark-delta /opt/spark/bin/spark-submit /opt/spark/apps/maintenance.py
    ```

---

## 4. UI Gateways

*   **Streamlit Dashboard**: [http://localhost:8501](http://localhost:8501)
*   **MinIO Console (S3 Browser)**: [http://localhost:9001](http://localhost:9001) (User: `admin` | Password: `password`)
