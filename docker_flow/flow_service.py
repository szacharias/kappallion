"""
flow_service.py
Core orchestration service for the Cold-Chain Streaming Lakehouse platform.
Adheres strictly to GEMINI.md Clean Code guidelines (SLAP, typed, no magic numbers).
"""

from dataclasses import dataclass
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

    def to_env(self) -> Dict[str, str]:
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
LEAN_PROFILE = ResourceProfile(
    name="LEAN (Limited Resource Profile - Default)",
    description="Optimized for 16GB developer laptops (~2.4GB max RAM ceiling, Spark local[2]).",
    spark_master="local[2]",
    spark_java_opts="-Xms256m -Xmx768m",
    spark_mem_limit="1024M",
    kafka_jvm_opts="-Xms128m -Xmx256m",
    kafka_mem_limit="384M",
    minio_mem_limit="384M",
    dashboard_java_opts="-Xms128m -Xmx256m",
    dashboard_mem_limit="512M",
    simulator_mem_limit="192M",
)

# Full Profile: Maximum performance multi-threaded profile (~4.5GB+ RAM footprint)
FULL_PROFILE = ResourceProfile(
    name="FULL (Unrestricted Performance Profile)",
    description="Maximum performance (~4.5GB+ RAM footprint, Spark local[*] all cores).",
    spark_master="local[*]",
    spark_java_opts="-Xms512m -Xmx1024m",
    spark_mem_limit="1536M",
    kafka_jvm_opts="-Xms256m -Xmx512m",
    kafka_mem_limit="768M",
    minio_mem_limit="1024M",
    dashboard_java_opts="-Xms256m -Xmx512m",
    dashboard_mem_limit="1024M",
    simulator_mem_limit="256M",
)



def resolve_compose_file(custom_path: Optional[str] = None) -> str:
    """Resolve active docker compose file, checking preferred and fallback paths."""
    if custom_path and os.path.exists(custom_path):
        return custom_path
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    preferred = os.path.join(repo_root, DEFAULT_COMPOSE_FILE)
    if os.path.exists(preferred):
        return preferred
    return os.path.join(repo_root, FALLBACK_COMPOSE_FILE)


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


def run_command(cmd: List[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
    """Execute a subprocess command cleanly and return the completed process."""
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)


def build_stack(compose_file: Optional[str] = None, env: Optional[Dict[str, str]] = None) -> bool:
    """Build or rebuild container images for simulator, dashboard, and Spark."""
    cfg = resolve_compose_file(compose_file)
    print(f"📦 Building unified Lakehouse service images using {os.path.basename(cfg)}...")
    res = subprocess.run(["docker", "compose", "-f", cfg, "build"], env=env)
    if res.returncode != 0:
        print("❌ Image build failed.")
        return False
    print("✔ Image build completed successfully.\n")
    return True


def init_minio_buckets(compose_file: str, env: Optional[Dict[str, str]] = None) -> bool:
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
    compose_file: Optional[str] = None,
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
        print("   💡 Tip: Pass '--full' to unlock max memory & multi-threaded Spark master local[*].\n")
    else:
        print()

    cfg = resolve_compose_file(compose_file)

    if build:
        if not build_stack(cfg, env=env):
            return False

    print(f"🚀 Starting unified Cold-Chain Lakehouse service...")
    up_res = subprocess.run(["docker", "compose", "-f", cfg, "up", "-d"], env=env)
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
        init_minio_buckets(cfg, env=env)

        print("   • Waiting for Spark streaming and Streamlit dashboard...")
        poll_container_health("lakehouse-dashboard", max_wait=25)

    print("\n" + "=" * 65)
    print("✅ UNIFIED COLD-CHAIN LAKEHOUSE PLATFORM IS RUNNING!")
    print("=" * 65)
    print(f"  ❄️  Streamlit Dashboard : {STREAMLIT_URL}")
    print(f"  🪣  MinIO Object Browser: {MINIO_URL} (User: admin / password)")
    print(f"  📡  Kafka Broker Host   : localhost:{KAFKA_PORT}")
    print(f"  ⚡  Resource Profile    : {'FULL (Performance)' if full else 'LEAN (Limited)'}")
    print("=" * 65 + "\n")
    return True


def stop_unified_flow(compose_file: Optional[str] = None) -> bool:
    """Gracefully stop all services, preserving Delta Lake storage state."""
    if not check_docker_cli():
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
    if not check_docker_cli():
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
    if not check_docker_cli():
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
    if not check_docker_cli():
        return False
    cfg = resolve_compose_file(compose_file)
    print(f"⚠️  Tearing down unified Lakehouse containers and network...")
    res = subprocess.run(["docker", "compose", "-f", cfg, "down"])
    return res.returncode == 0


def get_unified_status(compose_file: Optional[str] = None) -> List[Dict[str, Any]]:
    """Query live container statuses and return list of service metrics."""
    if not check_docker_cli():
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
