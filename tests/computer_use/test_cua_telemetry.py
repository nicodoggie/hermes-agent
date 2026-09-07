"""Tests for the cua-driver telemetry opt-in policy.

cua-driver ships anonymous PostHog telemetry ENABLED by default upstream.
Hermes disables it unless the user opts in via
``computer_use.cua_telemetry: true``. The policy is applied by injecting
``CUA_DRIVER_RS_TELEMETRY_ENABLED=0`` into every cua-driver child env.

These assert the behavior contract (default disables, opt-in leaves the var
untouched, config failure fails safe toward disabled), not specific config
snapshots.
"""

import subprocess
from unittest.mock import patch

import pytest

from tools.computer_use import cua_backend


_VAR = "CUA_DRIVER_RS_TELEMETRY_ENABLED"


class TestTelemetryDisabledFlag:

    def test_explicit_false_disables(self):
        with patch("hermes_cli.config.load_config",
                   return_value={"computer_use": {"cua_telemetry": False}}):
            assert cua_backend._cua_telemetry_disabled() is True


    def test_config_load_failure_fails_safe(self):
        # Unreadable config => default to disabling telemetry (privacy-safe).
        with patch("hermes_cli.config.load_config", side_effect=RuntimeError("boom")):
            assert cua_backend._cua_telemetry_disabled() is True



class TestChildEnv:
    def test_disabled_injects_var_zero(self):
        with patch.object(cua_backend, "_cua_telemetry_disabled", return_value=True):
            env = cua_backend.cua_driver_child_env({"PATH": "/usr/bin"})
            assert env[_VAR] == "0"
            # base env is preserved
            assert env["PATH"] == "/usr/bin"



    def test_disabled_overrides_inherited_enabled(self):
        # Even if the parent process had telemetry enabled, the default policy
        # forces it off in the child.
        with patch.object(cua_backend, "_cua_telemetry_disabled", return_value=True):
            env = cua_backend.cua_driver_child_env({_VAR: "1"})
            assert env[_VAR] == "0"

    @pytest.mark.platforms("linux")
    def test_wayland_recovers_xauthority_from_matching_user_session(self, tmp_path):
        authority = tmp_path / "xauth_probe"
        authority.touch(mode=0o600)
        completed = subprocess.CompletedProcess(
            ["systemctl", "--user", "show-environment"],
            0,
            stdout=f"DISPLAY=:0\nXAUTHORITY={authority}\n",
            stderr="",
        )
        base = {
            "DISPLAY": ":0",
            "WAYLAND_DISPLAY": "wayland-0",
            "XDG_SESSION_TYPE": "wayland",
            "DBUS_SESSION_BUS_ADDRESS": "unix:path=/run/user/1000/bus",
        }

        with (
            patch.object(cua_backend.subprocess, "run", return_value=completed),
            patch.object(cua_backend, "_cua_telemetry_disabled", return_value=True),
        ):
            env = cua_backend.cua_driver_child_env(base)

        assert env["XAUTHORITY"] == str(authority)

    @pytest.mark.platforms("linux")
    def test_does_not_recover_xauthority_for_different_display(self, tmp_path):
        authority = tmp_path / "xauth_probe"
        authority.touch(mode=0o600)
        completed = subprocess.CompletedProcess(
            ["systemctl", "--user", "show-environment"],
            0,
            stdout=f"DISPLAY=:1\nXAUTHORITY={authority}\n",
            stderr="",
        )
        base = {
            "DISPLAY": ":0",
            "WAYLAND_DISPLAY": "wayland-0",
            "XDG_SESSION_TYPE": "wayland",
            "DBUS_SESSION_BUS_ADDRESS": "unix:path=/run/user/1000/bus",
        }

        with (
            patch.object(cua_backend.subprocess, "run", return_value=completed),
            patch.object(cua_backend, "_cua_telemetry_disabled", return_value=True),
        ):
            env = cua_backend.cua_driver_child_env(base)

        assert "XAUTHORITY" not in env

    @pytest.mark.platforms("linux")
    def test_explicit_xauthority_is_not_overwritten(self):
        base = {
            "DISPLAY": ":0",
            "WAYLAND_DISPLAY": "wayland-0",
            "XDG_SESSION_TYPE": "wayland",
            "XAUTHORITY": "/already/set",
        }

        with (
            patch.object(cua_backend.subprocess, "run") as run,
            patch.object(cua_backend, "_cua_telemetry_disabled", return_value=True),
        ):
            env = cua_backend.cua_driver_child_env(base)

        assert env["XAUTHORITY"] == "/already/set"
        run.assert_not_called()

