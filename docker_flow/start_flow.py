"""
start_flow.py
Turnkey startup script for the single unified Cold-Chain Lakehouse platform.
Builds images, provisions MinIO buckets, and brings up the entire streaming service.
"""

import argparse
import sys
from flow_service import start_unified_flow


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and start the unified Cold-Chain Lakehouse platform."
    )
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip image rebuild and use existing images.",
    )
    parser.add_argument(
        "--no-wait",
        action="store_true",
        help="Skip synchronous readiness and health polling.",
    )
    parser.add_argument(
        "--compose-file",
        type=str,
        default=None,
        help="Custom path to docker-compose file.",
    )
    args = parser.parse_args()

    success = start_unified_flow(
        build=not args.no_build,
        wait_ready=not args.no_wait,
        compose_file=args.compose_file,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
