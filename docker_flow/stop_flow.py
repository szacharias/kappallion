"""
stop_flow.py
Gracefully shuts down the unified Lakehouse platform, preserving all Delta Lake storage.
"""

import argparse
import sys
from flow_service import stop_unified_flow


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Gracefully stop the unified Cold-Chain Lakehouse platform."
    )
    parser.add_argument(
        "--compose-file",
        type=str,
        default=None,
        help="Custom path to docker-compose file.",
    )
    args = parser.parse_args()

    success = stop_unified_flow(compose_file=args.compose_file)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
