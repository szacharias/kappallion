"""
flow.py
Unified command-line gateway for the Cold-Chain Lakehouse platform.
Allows running start, stop, pause, unpause, status, or build from a single command.
"""

import argparse
import sys
from flow_service import (
    build_stack,
    down_unified_flow,
    pause_unified_flow,
    start_unified_flow,
    stop_unified_flow,
    unpause_unified_flow,
)
from status_flow import main as status_main


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="flow.py",
        description="Unified lifecycle CLI for the Cold-Chain Lakehouse platform.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Lifecycle action to execute")

    # start
    p_start = subparsers.add_parser("start", help="Build and start the complete unified Lakehouse platform")
    p_start.add_argument("--no-build", action="store_true", help="Skip image rebuild")
    p_start.add_argument("--no-wait", action="store_true", help="Skip readiness polling")
    p_start.add_argument("--compose-file", type=str, default=None, help="Custom compose file")
    p_start.add_argument(
        "--full",
        action="store_true",
        help="Launch in unrestricted performance mode (default is lean mode ~2.4GB max RAM).",
    )

    # stop
    p_stop = subparsers.add_parser("stop", help="Gracefully stop all services (preserves data)")
    p_stop.add_argument("--compose-file", type=str, default=None, help="Custom compose file")

    # pause
    p_pause = subparsers.add_parser("pause", help="Freeze container execution (zero CPU usage)")
    p_pause.add_argument("--compose-file", type=str, default=None, help="Custom compose file")

    # unpause
    p_unpause = subparsers.add_parser("unpause", help="Resume frozen container execution")
    p_unpause.add_argument("--compose-file", type=str, default=None, help="Custom compose file")

    # status
    p_status = subparsers.add_parser("status", help="Inspect container health and states")
    p_status.add_argument("--compose-file", type=str, default=None, help="Custom compose file")

    # build
    p_build = subparsers.add_parser("build", help="Build or rebuild all service images")
    p_build.add_argument("--compose-file", type=str, default=None, help="Custom compose file")

    # down
    p_down = subparsers.add_parser("down", help="Tear down all containers and networks")
    p_down.add_argument("--compose-file", type=str, default=None, help="Custom compose file")

    args = parser.parse_args()

    # Default to 'start' if no subcommand provided
    if not args.command or args.command == "start":
        no_build = getattr(args, "no_build", False)
        no_wait = getattr(args, "no_wait", False)
        compose_file = getattr(args, "compose_file", None)
        full = getattr(args, "full", False)
        success = start_unified_flow(
            build=not no_build,
            wait_ready=not no_wait,
            compose_file=compose_file,
            full=full,
        )
        sys.exit(0 if success else 1)

    if args.command == "stop":
        success = stop_unified_flow(compose_file=args.compose_file)
        sys.exit(0 if success else 1)

    if args.command == "pause":
        success = pause_unified_flow(compose_file=args.compose_file)
        sys.exit(0 if success else 1)

    if args.command == "unpause":
        success = unpause_unified_flow(compose_file=args.compose_file)
        sys.exit(0 if success else 1)

    if args.command == "build":
        success = build_stack(compose_file=args.compose_file)
        sys.exit(0 if success else 1)

    if args.command == "down":
        success = down_unified_flow(compose_file=args.compose_file)
        sys.exit(0 if success else 1)

    if args.command == "status":
        status_main()


if __name__ == "__main__":
    main()
