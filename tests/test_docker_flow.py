"""
test_docker_flow.py
Unit tests for the unified Docker flow management service.
Target module: docker_flow/flow_service.py
Adheres strictly to GEMINI.md Clean Code guidelines and FIRST principles.
"""

import os
import subprocess
import sys
from unittest.mock import MagicMock, patch
import pytest

# Add repo root to path for imports
_repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from docker_flow.flow_service import (
    DEFAULT_COMPOSE_FILE,
    FALLBACK_COMPOSE_FILE,
    build_stack,
    check_docker_cli,
    check_docker_daemon,
    down_unified_flow,
    get_unified_status,
    pause_unified_flow,
    resolve_compose_file,
    start_unified_flow,
    stop_unified_flow,
    unpause_unified_flow,
)


@pytest.mark.unit
class TestDockerFlowService:
    """Unit test suite for flow_service orchestration logic."""

    def test_resolve_compose_file_custom_existing(self, tmp_path):
        """Custom compose file path is preferred if it exists."""
        # Arrange
        custom = tmp_path / "custom-compose.yml"
        custom.write_text("version: '3.8'", encoding="utf-8")

        # Act
        resolved = resolve_compose_file(str(custom))

        # Assert
        assert resolved == str(custom)

    def test_resolve_compose_file_defaults(self):
        """Resolves to DEFAULT_COMPOSE_FILE or FALLBACK_COMPOSE_FILE in repo root."""
        resolved = resolve_compose_file()
        assert os.path.basename(resolved) in [DEFAULT_COMPOSE_FILE, FALLBACK_COMPOSE_FILE]

    @patch("subprocess.run")
    def test_check_docker_cli_available(self, mock_run):
        """Returns True when docker --version succeeds with returncode 0."""
        # Arrange
        mock_run.return_value = MagicMock(returncode=0, stdout="Docker version 29.1.3")

        # Act & Assert
        assert check_docker_cli() is True
        assert check_docker_daemon() is True
        mock_run.assert_called_with(
            ["docker", "--version"],
            capture_output=True,
            text=True,
            timeout=5,
        )

    @patch("subprocess.run")
    def test_check_docker_cli_missing_returns_false(self, mock_run):
        """Returns False cleanly when docker CLI is not found or raises error."""
        # Arrange
        mock_run.side_effect = FileNotFoundError("docker not found")

        # Act & Assert
        assert check_docker_cli() is False
        assert check_docker_daemon() is False

    @patch("subprocess.run")
    def test_build_stack_invokes_compose_build(self, mock_run):
        """build_stack executes docker compose build with resolved config."""
        # Arrange
        mock_run.return_value = MagicMock(returncode=0)

        # Act
        result = build_stack()

        # Assert
        assert result is True
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd[:2] == ["docker", "compose"]
        assert "build" in cmd

    @patch("docker_flow.flow_service.check_docker_daemon", return_value=False)
    def test_start_unified_flow_aborts_when_daemon_offline(self, mock_daemon):
        """start_unified_flow aborts immediately without subprocess calls when daemon is offline."""
        assert start_unified_flow() is False

    @patch("docker_flow.flow_service.check_docker_daemon", return_value=True)
    @patch("docker_flow.flow_service.build_stack", return_value=True)
    @patch("docker_flow.flow_service.poll_container_health", return_value=True)
    @patch("docker_flow.flow_service.init_minio_buckets", return_value=True)
    @patch("subprocess.run")
    def test_start_unified_flow_success(
        self,
        mock_run,
        mock_minio,
        mock_health,
        mock_build,
        mock_daemon,
    ):
        """start_unified_flow orchestrates build, up -d, health checks, and returns True."""
        # Arrange
        mock_run.return_value = MagicMock(returncode=0)

        # Act
        result = start_unified_flow(build=True, wait_ready=True)

        # Assert
        assert result is True
        mock_build.assert_called_once()
        mock_minio.assert_called_once()
        assert mock_health.call_count >= 2

    @patch("docker_flow.flow_service.check_docker_daemon", return_value=True)
    @patch("subprocess.run")
    def test_stop_unified_flow(self, mock_run, mock_daemon):
        """stop_unified_flow executes docker compose stop."""
        mock_run.return_value = MagicMock(returncode=0)
        assert stop_unified_flow() is True
        cmd = mock_run.call_args[0][0]
        assert "stop" in cmd

    @patch("docker_flow.flow_service.check_docker_daemon", return_value=True)
    @patch("subprocess.run")
    def test_pause_and_unpause_unified_flow(self, mock_run, mock_daemon):
        """pause and unpause execute corresponding docker compose commands."""
        mock_run.return_value = MagicMock(returncode=0)

        assert pause_unified_flow() is True
        assert "pause" in mock_run.call_args[0][0]

        assert unpause_unified_flow() is True
        assert "unpause" in mock_run.call_args[0][0]

    @patch("docker_flow.flow_service.check_docker_daemon", return_value=True)
    @patch("subprocess.run")
    def test_down_unified_flow(self, mock_run, mock_daemon):
        """down_unified_flow executes docker compose down."""
        mock_run.return_value = MagicMock(returncode=0)
        assert down_unified_flow() is True
        assert "down" in mock_run.call_args[0][0]

    @patch("docker_flow.flow_service.check_docker_daemon", return_value=True)
    @patch("docker_flow.flow_service.run_command")
    def test_get_unified_status_parses_json_lines(self, mock_cmd, mock_daemon):
        """get_unified_status parses JSON output from docker ps."""
        mock_cmd.return_value = MagicMock(
            stdout='{"Names": "lakehouse-dashboard", "State": "running", "Status": "Up 2 hours"}\n'
                   '{"Names": "lakehouse-kafka", "State": "running", "Status": "Up 2 hours (healthy)"}\n'
        )
        status_list = get_unified_status()
        assert len(status_list) == 2
        assert status_list[0]["Names"] == "lakehouse-dashboard"
        assert status_list[1]["Names"] == "lakehouse-kafka"
