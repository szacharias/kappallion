"""
flow_service.py
Core orchestration service for the Cold-Chain Streaming Lakehouse platform.
Adheres strictly to GEMINI.md Clean Code guidelines (SLAP, typed, no magic numbers).
"""

import contextlib
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

# Ensure standard output can print Unicode characters reliably across Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    with contextlib.suppress(Exception):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = logging.getLogger("docker_flow")

# Named constants eliminating magic literals
DEFAULT_COMPOSE_FILE: str = "docker-compose-2.yml"
FALLBACK_COMPOSE_FILE: str = "docker-compose.yml"
DEFAULT_POLL_INTERVAL_SEC: int = 2
DEFAULT_HEALTH_TIMEOUT_SEC: int = 40
STREAMLIT_URL: str = "http://localhost:8501"
MINIO_URL: str = "http://localhost:9001"
KAFKA_PORT: int = 9092
SPARK_UI_URL: str = "http://localhost:4040"
SPARK_MASTER_URL: str = "http://localhost:8080"
FULL_COMPOSE_OVERRIDE: str = os.path.join("docker", "docker-compose.full.yml")


@dataclass(frozen=True)
class ResourceProfile:
    """Encapsulates CPU, JVM heap, and container RAM ceilings for Docker services."""
    name: str
    description: str
    spark_master: str
    spark_java_opts: str
    spark_mem_limit: str
    kafka_jvm_opts: str
    kafka_mem_limit: str
    minio_mem_limit: str
    dashboard_java_opts: str
    dashboard_mem_limit: str
    simulator_mem_limit: str

    def to_env(self) -> dict[str, str]:
        """Convert resource profile parameters into environment variables for Docker Compose."""
        return {
            "SPARK_MASTER": self.spark_master,
            "SPARK_JAVA_OPTS": self.spark_java_opts,
            "SPARK_MEM_LIMIT": self.spark_mem_limit,
            "KAFKA_JVM_OPTS": self.kafka_jvm_opts,
            "KAFKA_MEM_LIMIT": self.kafka_mem_limit,
            "MINIO_MEM_LIMIT": self.minio_mem_limit,
            "DASHBOARD_JAVA_OPTS": self.dashboard_java_opts,
            "DASHBOARD_MEM_LIMIT": self.dashboard_mem_limit,
            "SIMULATOR_MEM_LIMIT": self.simulator_mem_limit,
        }


# Lean Profile (Default): Tailored for 16GB laptops (~2.4GB max RAM footprint)
# Enforces Spark's minimum driver requirement (512m heap / 768M container limit)
LEAN_PROFILE = ResourceProfile(
    name="LEAN (Single-Node Local Mode - Default)",
    description="Optimized for 16GB developer laptops (~2.4GB max RAM, Spark local[2], App UI: 4040).",
    spark_master="local[2]",
    spark_java_opts="-Xms256m -Xmx768m",
    spark_mem_limit="1024M",
    kafka_jvm_opts="-Xms128m -Xmx256m",
    kafka_mem_limit="384M",
    minio_mem_limit="384M",
    dashboard_java_opts="-Xms256m -Xmx512m",
    dashboard_mem_limit="768M",
    simulator_mem_limit="192M",
)

# Full Profile: Standalone Cluster Mode with Master on 8080 & Worker Slaves (~4.5GB+ RAM footprint)
FULL_PROFILE = ResourceProfile(
    name="FULL (Spark Standalone Cluster Mode - Master & Worker Slaves)",
    description="Full Standalone Cluster (Master Web UI on port 8080, Worker nodes, ~4.5GB+ RAM).",
    spark_master="spark://spark-master:7077",
    spark_java_opts="-Xms512m -Xmx1024m",
    spark_mem_limit="1536M",
    kafka_jvm_opts="-Xms256m -Xmx512m",
    kafka_mem_limit="768M",
    minio_mem_limit="1024M",
    dashboard_java_opts="-Xms512m -Xmx1024m",
    dashboard_mem_limit="1536M",
    simulator_mem_limit="256M",
)





def resolve_compose_file(custom_path: str | None = None) -> str:
    """Resolve active docker compose file, checking preferred and fallback paths."""
    if custom_path and os.path.exists(custom_path):
        return custom_path
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    preferred = os.path.join(repo_root, DEFAULT_COMPOSE_FILE)
    if os.path.exists(preferred):
        return preferred
    return os.path.join(repo_root, FALLBACK_COMPOSE_FILE)


def resolve_compose_files(custom_path: str | None = None, full: bool = False) -> list[str]:
    """Resolve active compose files, layering full-mode cluster overlay when requested."""
    base = resolve_compose_file(custom_path)
    if full:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        full_override = os.path.join(repo_root, FULL_COMPOSE_OVERRIDE)
        if os.path.exists(full_override):
            return [base, full_override]
    return [base]


def resolve_all_compose_files(custom_path: str | None = None) -> list[str]:
    """Resolve base and full compose files for teardown/stop operations across all containers."""
    return resolve_compose_files(custom_path, full=True)


def build_compose_cmd(compose_files: list[str], action: str, extra_args: list[str] | None = None) -> list[str]:
    """Construct a clean docker compose command list with all specified config files."""
    cmd = ["docker", "compose"]
    for f in compose_files:
        cmd.extend(["-f", f])
    cmd.append(action)
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def check_docker_cli() -> bool:
    """Verify that the 'docker' command is available and executable in the CLI."""
    try:
        res = subprocess.run(
            ["docker", "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if res.returncode == 0:
            return True
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    print("\n" + "=" * 65)
    print("❌ ERROR: 'docker' command is not available in CLI / PATH!")
    print("=" * 65)
    print("Troubleshooting Steps:")
    print("  1. Verify Docker CLI is installed.")
    print("  2. Ensure the 'docker' executable is present in your system PATH.")
    print("  3. Re-run this flow script.")
    print("=" * 65 + "\n")
    return False


# Backward compatibility alias
check_docker_daemon = check_docker_cli


def run_command(cmd: list[str], cwd: str | None = None) -> subprocess.CompletedProcess:
    """Execute a subprocess command cleanly and return the completed process."""
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def build_stack(
    compose_file: str | list[str] | None = None,
    env: dict[str, str] | None = None,
) -> bool:
    """Build or rebuild container images for simulator, dashboard, and Spark."""
    if isinstance(compose_file, str):
        files = [compose_file]
    elif isinstance(compose_file, list):
        files = compose_file
    else:
        files = [resolve_compose_file()]

    cfg_name = ", ".join(os.path.basename(f) for f in files)
    print(f"📦 Building unified Lakehouse service images using {cfg_name}...")
    cmd = build_compose_cmd(files, "build")
    res = subprocess.run(cmd, env=env)
    if res.returncode != 0:
        print("❌ Image build failed.")
        return False
    print("✔ Image build completed successfully.\n")
    return True


def init_minio_buckets(compose_file: str, env: dict[str, str] | None = None) -> bool:
    """Execute minio-init container to provision warehouse and lakehouse buckets."""
    print("🪣 Initializing MinIO storage buckets (warehouse, lakehouse)...")
    res = subprocess.run(["docker", "compose", "-f", compose_file, "up", "minio-init"], env=env)
    return res.returncode == 0


def poll_container_health(container_name: str, max_wait: int = DEFAULT_HEALTH_TIMEOUT_SEC) -> bool:
    """Poll container until healthy or timeout expires."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        res = run_command(["docker", "inspect", "--format", "{{.State.Health.Status}}", container_name])
        status = res.stdout.strip()
        if status == "healthy":
            return True
        # If no health check is defined, check if state is running
        if not status or status == "<no value>":
            state_res = run_command(["docker", "inspect", "--format", "{{.State.Status}}", container_name])
            if state_res.stdout.strip() == "running":
                return True
        time.sleep(DEFAULT_POLL_INTERVAL_SEC)
    return False


def start_unified_flow(
    build: bool = True,
    wait_ready: bool = True,
    compose_file: str | None = None,
    full: bool = False,
) -> bool:
    """Start the single unified Lakehouse platform with automated readiness verification."""
    if not check_docker_cli():
        return False

    profile = FULL_PROFILE if full else LEAN_PROFILE
    env = os.environ.copy()
    env.update(profile.to_env())

    print(f"\n⚙️  Active Resource Profile: {profile.name}")
    print(f"   ℹ️  {profile.description}")
    if not full:
        print("   💡 Tip: Pass '--full' to launch Spark Standalone Cluster (Master on 8080 & Workers).\n")
    else:
        print()

    compose_files = resolve_compose_files(compose_file, full=full)

    if build and not build_stack(compose_files, env=env):
        return False

    print("🚀 Starting unified Cold-Chain Lakehouse service...")
    up_cmd = build_compose_cmd(compose_files, "up", ["-d"])
    up_res = subprocess.run(up_cmd, env=env)
    if up_res.returncode != 0:
        print("❌ Failed to start containers.")
        return False

    if wait_ready:
        print("⏳ Verifying service dependencies...")
        print("   • Waiting for Kafka broker...")
        poll_container_health("lakehouse-kafka", max_wait=30)

        print("   • Waiting for MinIO S3 object store...")
        poll_container_health("lakehouse-minio", max_wait=20)

        # Bootstrap MinIO buckets
        init_minio_buckets(compose_files[0], env=env)

        if full:
            print("   • Waiting for Spark Master (Cluster Web UI on 8080)...")
            poll_container_health("lakehouse-spark-master", max_wait=30)
            print("   • Waiting for Spark Worker-1 (Slave Node)...")
            poll_container_health("lakehouse-spark-worker-1", max_wait=25)

        print("   • Waiting for Spark streaming and Streamlit dashboard...")
        poll_container_health("lakehouse-dashboard", max_wait=25)

    print("\n" + "=" * 65)
    print("✅ UNIFIED COLD-CHAIN LAKEHOUSE PLATFORM IS RUNNING!")
    print("=" * 65)
    print(f"  ❄️  Streamlit Dashboard : {STREAMLIT_URL}")
    if full:
        print(f"  👑  Spark Master Web UI : {SPARK_MASTER_URL} (Master & Worker Slaves)")
    print(f"  ⚡  Spark App Web UI    : {SPARK_UI_URL} (Streaming Jobs)")
    print(f"  🪣  MinIO Object Browser: {MINIO_URL} (User: admin / password)")
    print(f"  📡  Kafka Broker Host   : localhost:{KAFKA_PORT}")
    print(f"  ⚡  Execution Mode      : {'FULL (Spark Standalone Cluster)' if full else 'LEAN (Single-Node Local)'}")
    print("=" * 65 + "\n")
    return True


def stop_unified_flow(compose_file: str | None = None) -> bool:
    """Gracefully stop all services, preserving Delta Lake storage state."""
    if not check_docker_cli():
        return False
    files = resolve_all_compose_files(compose_file)
    print("🛑 Gracefully stopping unified Lakehouse service...")
    res = subprocess.run(build_compose_cmd(files, "stop"))
    if res.returncode == 0:
        print("✔ All services stopped cleanly. Delta Lake storage preserved on disk.\n")
        return True
    return False


def pause_unified_flow(compose_file: str | None = None) -> bool:
    """Freeze all container execution (100% CPU savings) while preserving memory state."""
    if not check_docker_cli():
        return False
    files = resolve_all_compose_files(compose_file)
    print("⏸️  Pausing execution of unified Lakehouse containers...")
    res = subprocess.run(build_compose_cmd(files, "pause"))
    if res.returncode == 0:
        print("✔ Unified service paused. CPU usage reduced to zero.\n")
        return True
    return False


def unpause_unified_flow(compose_file: str | None = None) -> bool:
    """Resume execution of previously paused containers."""
    if not check_docker_cli():
        return False
    files = resolve_all_compose_files(compose_file)
    print("▶️  Resuming execution of unified Lakehouse containers...")
    res = subprocess.run(build_compose_cmd(files, "unpause"))
    if res.returncode == 0:
        print("✔ Unified service execution resumed.\n")
        return True
    return False


def down_unified_flow(compose_file: str | None = None) -> bool:
    """Tear down all containers and remove bridge network."""
    if not check_docker_cli():
        return False
    files = resolve_all_compose_files(compose_file)
    print("⚠️  Tearing down unified Lakehouse containers and network...")
    res = subprocess.run(build_compose_cmd(files, "down"))
    return res.returncode == 0


def get_unified_status(compose_file: str | None = None) -> list[dict[str, Any]]:
    """Query live container statuses and return list of service metrics."""
    if not check_docker_cli():
        return []
    res = run_command(["docker", "ps", "-a", "--filter", "name=lakehouse-", "--format", "{{json .}}"])
    containers: list[dict[str, Any]] = []
    for line in res.stdout.strip().splitlines():
        if line:
            with contextlib.suppress(Exception):
                containers.append(json.loads(line))
    return containers
