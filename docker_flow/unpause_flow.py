"""
unpause_flow.py
Resumes execution of previously paused Lakehouse containers.
"""

import argparse
import sys

try:
    from docker_flow.flow_service import unpause_unified_flow
except ImportError:
    from flow_service import unpause_unified_flow


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resume execution of the unified Cold-Chain Lakehouse platform."
    )
    parser.add_argument(
        "--compose-file",
        type=str,
        default=None,
        help="Custom path to docker-compose file.",
    )
    args = parser.parse_args()

    success = unpause_unified_flow(compose_file=args.compose_file)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
