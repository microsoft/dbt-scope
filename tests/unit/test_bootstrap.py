from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def bootstrap(tmp_path):
    repo = tmp_path / "checkout with spaces"
    scripts = repo / ".scripts"
    scripts.mkdir(parents=True)
    shutil.copyfile(ROOT / ".scripts/bootstrap-dev-env.sh", scripts / "bootstrap-dev-env.sh")
    (repo / ".env").write_text("exit 91\n")
    tools = tmp_path / "tools"
    tools.mkdir()
    log = tmp_path / "calls"
    home = tmp_path / "home"
    home.mkdir()
    for tool in ("az", "uv", "jq", "python3", "gh", "code", "copilot", "sudo", "curl"):
        command = tools / tool
        command.write_text(
            "#!/bin/sh\n"
            f'printf "%s:%s:%s\\n" "{tool}" "$PWD" "$*" >> "$BOOTSTRAP_CALLS"\n'
            + ('exit "${UV_EXIT:-0}"\n' if tool == "uv" else "exit 0\n")
        )
        command.chmod(0o755)
    env = {
        **os.environ,
        "HOME": str(home),
        "PATH": f"{tools}:/usr/bin:/bin",
        "BOOTSTRAP_CALLS": str(log),
        "IS_GH_ACTION": "1",
    }
    env.pop("WSL_DISTRO_NAME", None)
    return repo, log, env


def run_bootstrap(repo, env):
    return subprocess.run(
        ["bash", str(repo / ".scripts/bootstrap-dev-env.sh")],
        cwd=repo.parent,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


class TestLinuxBootstrap:
    def test_ci_setup_installs_dev_extra_from_script_location_without_cloud_actions(
        self, bootstrap
    ):
        repo, log, env = bootstrap
        result = run_bootstrap(repo, env)
        assert result.returncode == 0, result.stderr
        assert log.read_text().splitlines() == [f"uv:{repo}:sync --extra dev"]

    def test_repeated_setup_preserves_workspace(self, bootstrap):
        repo, log, env = bootstrap
        sentinel = repo / "work-in-progress"
        sentinel.write_text("keep")
        for _ in range(2):
            result = run_bootstrap(repo, env)
            assert result.returncode == 0, result.stderr
        assert sentinel.read_text() == "keep"
        assert log.read_text().splitlines() == [f"uv:{repo}:sync --extra dev"] * 2

    def test_dependency_failure_is_propagated(self, bootstrap):
        repo, _, env = bootstrap
        result = run_bootstrap(repo, {**env, "UV_EXIT": "23"})
        assert result.returncode == 23
        assert "Dev environment ready" not in result.stdout

    def test_wsl_setup_keeps_optional_editor_support_without_logging_in(self, bootstrap):
        repo, log, env = bootstrap
        result = run_bootstrap(
            repo, {**env, "WSL_DISTRO_NAME": "Ubuntu-24.04", "IS_GH_ACTION": "0"}
        )
        assert result.returncode == 0, result.stderr
        assert log.read_text().splitlines() == [
            f"uv:{repo}:sync --extra dev",
            f"code:{repo}:--install-extension ms-python.python",
            f"code:{repo}:--install-extension ms-vscode-remote.remote-wsl",
        ]

    def test_headless_wsl_is_detected_without_distro_environment_variable(self, bootstrap):
        repo, log, env = bootstrap
        grep = repo.parent / "tools" / "grep"
        grep.write_text(
            '#!/bin/sh\nif [ "$*" = "-qi microsoft /proc/sys/kernel/osrelease" ]; then '
            'exit 0; fi\nexec /usr/bin/grep "$@"\n'
        )
        grep.chmod(0o755)
        result = run_bootstrap(repo, {**env, "IS_GH_ACTION": "0"})
        assert result.returncode == 0, result.stderr
        assert "code:" in log.read_text()


class TestRunEnvironment:
    @pytest.mark.parametrize("newline", ["\n", "\r\n"])
    def test_env_accepts_linux_and_windows_line_endings_without_modifying_file(
        self, tmp_path, newline
    ):
        scripts = tmp_path / ".scripts"
        scripts.mkdir()
        shutil.copyfile(ROOT / ".scripts/run.sh", scripts / "run.sh")
        python = tmp_path / ".venv/bin/python"
        python.parent.mkdir(parents=True)
        python.touch()
        env_file = tmp_path / ".env"
        content = newline.join(
            ["# configuration", "", "BOOTSTRAP_ENV_TEST='value with spaces'", ""]
        )
        env_file.write_bytes(content.encode())
        uv = tmp_path / "uv"
        uv.write_text('#!/bin/sh\n[ "$BOOTSTRAP_ENV_TEST" = "value with spaces" ]\n')
        uv.chmod(0o755)
        result = subprocess.run(
            ["bash", str(scripts / "run.sh"), "install"],
            cwd=tmp_path,
            env={**os.environ, "PATH": f"{tmp_path}:/usr/bin:/bin"},
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert env_file.read_bytes() == content.encode()


class TestBootstrapTaskfile:
    def test_full_and_up_are_separate_and_do_not_publish(self):
        config = yaml.safe_load((ROOT / ".scripts/bootstrap-dev-env.yaml").read_text())
        assert config["vars"] == {"DISTRO": "Ubuntu-24.04", "REPO": "/workspaces/dbt-scope"}
        tasks = config["tasks"]
        assert set(tasks) == {"default", "up"}
        for task in tasks.values():
            assert task["platforms"] == ["windows"]
            assert task["dir"] == "{{.TASKFILE_DIR}}"
        assert tasks["default"]["cmds"] == [
            "pwsh -NoProfile -NonInteractive -File bootstrap-dev-env.ps1",
            {"task": "up"},
        ]
        assert tasks["up"]["cmds"] == [
            "wsl -d {{.DISTRO}} --cd {{.REPO}} --exec bash ./.scripts/bootstrap-dev-env.sh",
            "code --folder-uri 'vscode-remote://wsl+{{.DISTRO}}{{.REPO}}'",
        ]
