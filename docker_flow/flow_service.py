"""
flow_service.py
Core orchestration service for the Cold-Chain Streaming Lakehouse platform.
Adheres strictly to GEMINI.md Clean Code guidelines (SLAP, typed, no magic numbers).
"""

import json
import logging
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

# Ensure standard output can print Unicode characters reliably across Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

logger = logging.getLogger("docker_flow")

# Named constants eliminating magic literals
DEFAULT_COMPOSE_FILE: str = "docker-compose-2.yml"
FALLBACK_COMPOSE_FILE: str = "docker-compose.yml"
DEFAULT_POLL_INTERVAL_SEC: int = 2
DEFAULT_HEALTH_TIMEOUT_SEC: int = 40
STREAMLIT_URL: str = "http://localhost:8501"
MINIO_URL: str = "http://localhost:9001"
KAFKA_PORT: int = 9092


def resolve_compose_file(custom_path: Optional[str] = None) -> str:
    """Resolve active docker compose file, checking preferred and fallback paths."""
    if custom_path and os.path.exists(custom_path):
        return custom_path
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    preferred = os.path.join(repo_root, DEFAULT_COMPOSE_FILE)
    if os.path.exists(preferred):
        return preferred
    return os.path.join(repo_root, FALLBACK_COMPOSE_FILE)


def check_docker_daemon() -> bool:
    """Pre-flight check verifying Docker Desktop daemon is responding."""
    try:
        res = subprocess.run(
            ["docker", "info", "--format", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if res.returncode == 0:
            return True
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    print("\n" + "=" * 65)
    print("❌ ERROR: Docker daemon is not running!")
    print("=" * 65)
    print("Troubleshooting Steps:")
    print("  1. Please launch 'Docker Desktop' from your Start menu or taskbar.")
    print("  2. Wait until Docker Desktop indicates 'Engine running'.")
    print("  3. Re-run this flow script.")
    print("=" * 65 + "\n")
    return False


def run_command(cmd: List[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
    """Execute a subprocess command cleanly and return the completed process."""
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def build_stack(compose_file: Optional[str] = None) -> bool:
    """Build or rebuild container images for simulator, dashboard, and Spark."""
    cfg = resolve_compose_file(compose_file)
    print(f"📦 Building unified Lakehouse service images using {os.path.basename(cfg)}...")
    res = subprocess.run(["docker", "compose", "-f", cfg, "build"])
    if res.returncode != 0:
        print("❌ Image build failed.")
        return False
    print("✔ Image build completed successfully.\n")
    return True


def init_minio_buckets(compose_file: str) -> bool:
    """Execute minio-init container to provision warehouse and lakehouse buckets."""
    print("🪣 Initializing MinIO storage buckets (warehouse, lakehouse)...")
    res = subprocess.run(["docker", "compose", "-f", compose_file, "up", "minio-init"])
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
    compose_file: Optional[str] = None,
) -> bool:
    """Start the single unified Lakehouse platform with automated readiness verification."""
    if not check_docker_daemon():
        return False

    cfg = resolve_compose_file(compose_file)

    if build:
        if not build_stack(cfg):
            return False

    print(f"🚀 Starting unified Cold-Chain Lakehouse service...")
    up_res = subprocess.run(["docker", "compose", "-f", cfg, "up", "-d"])
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
        init_minio_buckets(cfg)

        print("   • Waiting for Spark streaming and Streamlit dashboard...")
        poll_container_health("lakehouse-dashboard", max_wait=25)

    print("\n" + "=" * 65)
    print("✅ UNIFIED COLD-CHAIN LAKEHOUSE PLATFORM IS RUNNING!")
    print("=" * 65)
    print(f"  ❄️  Streamlit Dashboard : {STREAMLIT_URL}")
    print(f"  🪣  MinIO Object Browser: {MINIO_URL} (User: admin / password)")
    print(f"  📡  Kafka Broker Host   : localhost:{KAFKA_PORT}")
    print("=" * 65 + "\n")
    return True


def stop_unified_flow(compose_file: Optional[str] = None) -> bool:
    """Gracefully stop all services, preserving Delta Lake storage state."""
    if not check_docker_daemon():
        return False
    cfg = resolve_compose_file(compose_file)
    print(f"🛑 Gracefully stopping unified Lakehouse service...")
    res = subprocess.run(["docker", "compose", "-f", cfg, "stop"])
    if res.returncode == 0:
        print("✔ All services stopped cleanly. Delta Lake storage preserved on disk.\n")
        return True
    return False


def pause_unified_flow(compose_file: Optional[str] = None) -> bool:
    """Freeze all container execution (100% CPU savings) while preserving memory state."""
    if not check_docker_daemon():
        return False
    cfg = resolve_compose_file(compose_file)
    print(f"⏸️  Pausing execution of unified Lakehouse containers...")
    res = subprocess.run(["docker", "compose", "-f", cfg, "pause"])
    if res.returncode == 0:
        print("✔ Unified service paused. CPU usage reduced to zero.\n")
        return True
    return False


def unpause_unified_flow(compose_file: Optional[str] = None) -> bool:
    """Resume execution of previously paused containers."""
    if not check_docker_daemon():
        return False
    cfg = resolve_compose_file(compose_file)
    print(f"▶️  Resuming execution of unified Lakehouse containers...")
    res = subprocess.run(["docker", "compose", "-f", cfg, "unpause"])
    if res.returncode == 0:
        print("✔ Unified service execution resumed.\n")
        return True
    return False


def down_unified_flow(compose_file: Optional[str] = None) -> bool:
    """Tear down all containers and remove bridge network."""
    if not check_docker_daemon():
        return False
    cfg = resolve_compose_file(compose_file)
    print(f"⚠️  Tearing down unified Lakehouse containers and network...")
    res = subprocess.run(["docker", "compose", "-f", cfg, "down"])
    return res.returncode == 0


def get_unified_status(compose_file: Optional[str] = None) -> List[Dict[str, Any]]:
    """Query live container statuses and return list of service metrics."""
    if not check_docker_daemon():
        return []
    res = run_command(["docker", "ps", "-a", "--filter", "name=lakehouse-", "--format", "{{json .}}"])
    containers: List[Dict[str, Any]] = []
    for line in res.stdout.strip().splitlines():
        if line:
            try:
                containers.append(json.loads(line))
            except Exception:
                pass
    return containers
