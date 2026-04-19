#!/usr/bin/env bash

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_PATH="${PROJECT_ROOT}/.venv"

print_header() {
    echo ""
    echo ">>> Bootstrapping Python environment..."
    echo "----------------------------------------"
}

ensure_uv() {
    if command -v uv >/dev/null 2>&1; then
        echo "[OK] uv detected."
        return
    fi

    echo "[WARN] uv not found."

    read -r -p "Install uv automatically? [y/N]: " ans
    case "$ans" in
        y|Y)
            echo "Installing uv..."
            curl -LsSf https://astral.sh/uv/install.sh | sh
            export PATH="$HOME/.local/bin:$PATH"
            ;;
        *)
            echo "Abort. Install manually:"
            echo "  curl -LsSf https://astral.sh/uv/install.sh | sh"
            exit 1
            ;;
    esac
}

setup_python() {
    echo "-> Selecting Python runtime (3.11)"
    uv python pin 3.11
}

install_deps() {
    echo "-> Resolving & installing dependencies..."
    uv sync --python 3.11
}

activate_env() {
    if [[ -f "${ENV_PATH}/bin/activate" ]]; then
        # shellcheck disable=SC1090
        source "${ENV_PATH}/bin/activate"

        echo "[DONE] Environment ready."
        echo "Python: $(command -v python)"
        echo "Version: $(python --version)"
    else
        echo "[ERROR] Virtual environment missing at ${ENV_PATH}"
        exit 1
    fi
}

main() {
    print_header
    ensure_uv
    setup_python
    install_deps
    activate_env
}

main "$@"