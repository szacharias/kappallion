"""
pause_flow.py
Freezes execution of all Lakehouse containers (100% CPU savings) while preserving memory.
"""

import argparse
import sys

try:
    from docker_flow.flow_service import pause_unified_flow
except ImportError:
    from flow_service import pause_unified_flow


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pause execution of the unified Cold-Chain Lakehouse platform."
    )
    parser.add_argument(
        "--compose-file",
        type=str,
        default=None,
        help="Custom path to docker-compose file.",
    )
    args = parser.parse_args()

    success = pause_unified_flow(compose_file=args.compose_file)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
