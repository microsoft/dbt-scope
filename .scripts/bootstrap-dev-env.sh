#!/bin/bash
#
#
#       Bootstraps a Linux dev environment idempotently.
#       Layers developer tools on top of the CI environment.
#       If your Devbox restarts, rerun this script.
#
# ---------------------------------------------------------------------------------------
#
set -e

is_wsl() {
    [[ "${WSL_DISTRO_NAME:-}" == Ubuntu* ]]
}

REPO_ROOT=$(git rev-parse --show-toplevel)

PACKAGES=""
if ! command -v jq &> /dev/null; then PACKAGES="jq"; fi
if ! command -v python3 &> /dev/null; then PACKAGES="${PACKAGES:+$PACKAGES }python3"; fi
if ! command -v pip &> /dev/null; then PACKAGES="${PACKAGES:+$PACKAGES }python3-pip"; fi
if [ -n "$PACKAGES" ]; then
    echo "Installing packages from apt, this will take a couple minutes: $PACKAGES"
    sudo apt-get update > /dev/null 2>&1
    sudo DEBIAN_FRONTEND=noninteractive apt-get install -y $PACKAGES > /dev/null 2>&1
fi
command -v az &> /dev/null || curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash > /dev/null 2>&1
command -v uv &> /dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh

[[ ":$PATH:" != *":$HOME/.local/bin:"* ]] && export PATH="$PATH:$HOME/.local/bin" || true
if [ -f "$HOME/.local/bin/uv" ] && [ ! -f /usr/local/bin/uv ]; then
    sudo ln -sf "$HOME/.local/bin/uv" /usr/local/bin/uv
    sudo ln -sf "$HOME/.local/bin/uvx" /usr/local/bin/uvx 2>/dev/null || true
fi

cd "$REPO_ROOT"
uv sync --all-groups
[ -f .venv/bin/activate ] && source .venv/bin/activate

if is_wsl && command -v code &> /dev/null; then
    echo "Installing VS Code extensions..."
    code --install-extension ms-python.python                # Python
    code --install-extension ms-python.vscode-pylance        # IntelliSense
    code --install-extension ms-vscode-remote.remote-wsl     # WSL
fi

if is_wsl && ! command -v copilot &> /dev/null; then
    echo "Installing GitHub Copilot CLI..."
    curl -fsSL https://gh.io/copilot-install | bash
fi

echo ""
echo "Dev environment ready"
