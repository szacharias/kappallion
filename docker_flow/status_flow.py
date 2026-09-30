"""
status_flow.py
Displays the live health, operational state, and exposed ports of the unified Lakehouse service.
"""

import argparse
import sys

try:
    from docker_flow.flow_service import (
        KAFKA_PORT,
        MINIO_URL,
        SPARK_MASTER_URL,
        SPARK_UI_URL,
        STREAMLIT_URL,
        get_unified_status,
    )
except ImportError:
    from flow_service import (
        KAFKA_PORT,
        MINIO_URL,
        SPARK_UI_URL,
        STREAMLIT_URL,
        get_unified_status,
    )



def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect health of the unified Cold-Chain Lakehouse platform."
    )
    parser.add_argument(
        "--compose-file",
        type=str,
        default=None,
        help="Custom path to docker-compose file.",
    )
    args = parser.parse_args()

    containers = get_unified_status(compose_file=args.compose_file)
    if not containers:
        print("\nℹ️  No Lakehouse containers are currently running.")
        print("   To start the unified platform, run: python docker_flow/start_flow.py\n")
        sys.exit(0)

    print("\n" + "=" * 80)
    print("❄️  COLD-CHAIN LAKEHOUSE PLATFORM STATUS")
    print("=" * 80)
    print(f"{'CONTAINER NAME':<26} {'STATE':<12} {'HEALTH':<12} {'STATUS'}")
    print("-" * 80)

    for c in sorted(containers, key=lambda x: str(x.get("Names", ""))):
        name = str(c.get("Names", c.get("ID", "Unknown")))
        state = str(c.get("State", "unknown"))
        status = str(c.get("Status", ""))
        health = "n/a"
        if "healthy" in status:
            health = "healthy"
        elif "unhealthy" in status:
            health = "unhealthy"
        elif "starting" in status:
            health = "starting"

        print(f"{name:<26} {state:<12} {health:<12} {status}")

    print("=" * 80)
    print("Gateways:")
    print(f"  • Dashboard    : {STREAMLIT_URL}")
    print(f"  • Spark App UI : {SPARK_UI_URL}")
    if any("spark-master" in str(c.get("Names", "")) for c in containers):
        print(f"  • Spark Master : {SPARK_MASTER_URL}")
    print(f"  • MinIO S3     : {MINIO_URL}")
    print(f"  • Kafka        : localhost:{KAFKA_PORT}\n")



if __name__ == "__main__":
    main()
