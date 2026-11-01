#!/bin/bash
# Prepare the existing Linux checkout; also used by GCI on a normal Linux runner.
set -euo pipefail

is_wsl() {
    [[ "${IS_GH_ACTION:-0}" != "1" ]] && {
        [[ "${WSL_DISTRO_NAME:-}" == Ubuntu* ]] || grep -qi microsoft /proc/sys/kernel/osrelease
    }
}

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# WSL inherits Windows executables that do not install tools into the Linux environment.
export PATH="$HOME/.local/bin:$(printf '%s' "$PATH" | tr ':' '\n' | grep -vE '^/mnt/[a-zA-Z]/' | paste -sd: -)"

packages=()
command -v curl >/dev/null || packages+=(curl ca-certificates)
command -v jq >/dev/null || packages+=(jq)
command -v python3 >/dev/null || packages+=(python3)
if is_wsl && ! command -v gh >/dev/null; then packages+=(gh); fi
if ((${#packages[@]})); then
    sudo apt-get update -qq
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y "${packages[@]}"
fi

if ! command -v az >/dev/null; then
    curl -fsSL https://aka.ms/InstallAzureCLIDeb | sudo bash
fi
if ! command -v uv >/dev/null; then
    curl -fsSL https://astral.sh/uv/install.sh | sh
fi
hash -r
command -v az >/dev/null || { echo "Native Linux Azure CLI installation failed." >&2; exit 1; }
command -v uv >/dev/null || { echo "uv installation failed." >&2; exit 1; }

# Service-launched jobs do not read the user's shell profile.
for tool in uv uvx; do
    if [[ -x "$HOME/.local/bin/$tool" && ! -e "/usr/local/bin/$tool" ]]; then
        sudo ln -s "$HOME/.local/bin/$tool" "/usr/local/bin/$tool"
    fi
done

cd "$REPO_ROOT"
uv sync --extra dev

if is_wsl; then
    if command -v code >/dev/null; then
        code --install-extension ms-python.python
        code --install-extension ms-vscode-remote.remote-wsl
    fi
    if ! command -v copilot >/dev/null; then
        curl -fsSL https://gh.io/copilot-install | bash
    fi
fi

echo "Dev environment ready: $REPO_ROOT"
