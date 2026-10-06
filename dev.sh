#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Scitrera LLC
# SPDX-License-Identifier: AGPL-3.0-only
# Additional permission under AGPLv3 section 7: see LICENSE_EXCEPTION.
# Usage: source dev.sh (optional: export SPARKRUN_CHECKOUT=/path/to/sparkrun)

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    echo "dev.sh must be sourced: source dev.sh" >&2
    exit 1
fi

_oci_relay_dev_setup() {
    local script_dir checkout venv_dir config_dir base_config go_command toolchain binary temporary name definition
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)" || return 1
    checkout="${SPARKRUN_CHECKOUT:-$script_dir/../oss-sparkrun}"
    venv_dir="$script_dir/.venv"
    config_dir="$script_dir/.dev/config"
    base_config="${SPARKRUN_CONFIG_DIR:-$HOME/.config/sparkrun}"
    if [[ ! -f "$checkout/src/sparkrun/core/image_distribution.py" || ! -f "$checkout/pyproject.toml" ]]; then
        echo "Set SPARKRUN_CHECKOUT to a Sparkrun develop-next / 0.4.0 checkout with image-distribution API 1." >&2
        return 1
    fi
    checkout="$(cd "$checkout" && pwd -P)" || return 1
    if [[ -e "$checkout/src/sparkrun/plugins/oci_relay" ]]; then
        echo "Selected Sparkrun checkout still contains a vendored OCI Relay; use an unvendored checkout." >&2
        return 1
    fi
    if ! command -v uv >/dev/null 2>&1; then
        echo "uv is required to create the plugin development environment." >&2
        return 1
    fi
    go_command="${GO:-}"
    if [[ -z "$go_command" ]]; then
        go_command="$(command -v go)" || go_command=""
        if [[ -z "$go_command" ]]; then
            for name in "$HOME"/sdk/go*/bin/go; do
                [[ -x "$name" ]] && go_command="$name"
            done
        fi
    fi
    if [[ -z "$go_command" ]] || ! command -v "$go_command" >/dev/null 2>&1; then
        echo "Go is required; add it to PATH or set GO=/path/to/go." >&2
        return 1
    fi
    if [[ ! -x "$venv_dir/bin/python" ]]; then
        uv venv --python 3.12 "$venv_dir" || return 1
    fi
    echo "Installing editable Sparkrun from $checkout and the OCI Relay plugin ..."
    uv pip install --python "$venv_dir/bin/python" --editable "$checkout" \
        --editable "$script_dir[test]" 'ruff==0.15.6' || return 1
    uv pip check --python "$venv_dir/bin/python" || return 1
    "$venv_dir/bin/python" -c 'import sys; from pathlib import Path; import sparkrun, sparkrun.plugins as p; assert p.IMAGE_DISTRIBUTION_API_VERSION == 1 and getattr(p, "IMAGE_PULL_API_VERSION", None) == 1, "Sparkrun checkout needs image-distribution API 1 and pre-pull API 1"; assert Path(sparkrun.__file__).resolve() == Path(sys.argv[1]) / "src/sparkrun/__init__.py", "PYTHONPATH shadows the selected Sparkrun checkout"' "$checkout" || return 1
    mkdir -p "$script_dir/.dev" "$script_dir/bin" || return 1
    chmod 700 "$script_dir/.dev" || return 1
    toolchain="$("$venv_dir/bin/python" -c 'import sys,yaml; print("go" + yaml.safe_load(open(sys.argv[1]))["ci"]["go"]["go_version"])' "$script_dir/versions.yaml")" || return 1
    temporary="$(mktemp "$script_dir/.dev/oci-relay.XXXXXXXXXX")" || return 1
    echo "Building OCI Relay with $toolchain ..."
    if ! (cd "$script_dir" && CGO_ENABLED=0 GOTOOLCHAIN="$toolchain" "$go_command" build -trimpath -o "$temporary" ./cmd/oci-relay); then
        rm -f -- "$temporary"
        return 1
    fi
    binary="$script_dir/bin/oci-relay"
    mv -f -- "$temporary" "$binary" || return 1
    "$venv_dir/bin/python" "$script_dir/scripts/dev-environment.py" configure \
        --destination "$config_dir" --base "$base_config" --binary "$binary" || return 1
    SPARKRUN_CONFIG_DIR="$config_dir" SPARKRUN_APPLICATION_CONFIG="$config_dir/config.yaml" \
        SPARKRUN_NO_INSTALLED_PLUGINS=0 SPARKRUN_NO_TELEMETRY=1 \
        "$venv_dir/bin/python" "$script_dir/scripts/dev-environment.py" verify || return 1

    # Activate only after successful setup. uv/virtualenv replaces deactivate
    # before calling it, so explicitly run our existing wrapper first.
    if declare -F deactivate >/dev/null; then
        deactivate || return 1
    fi
    # shellcheck disable=SC1091
    source "$venv_dir/bin/activate" || return 1
    declare -gA _OCI_RELAY_DEV_PREVIOUS=() _OCI_RELAY_DEV_PRESENT=()
    for name in SPARKRUN_CHECKOUT SPARKRUN_CONFIG_DIR SPARKRUN_APPLICATION_CONFIG SPARKRUN_NO_INSTALLED_PLUGINS OCI_RELAY_BINARY; do
        if [[ -v "$name" ]]; then
            _OCI_RELAY_DEV_PRESENT["$name"]=1
            _OCI_RELAY_DEV_PREVIOUS["$name"]="${!name}"
        fi
    done
    # Rename the trusted activation function so ordinary deactivate also
    # restores our shell-local configuration. No user text is evaluated.
    definition="$(declare -f deactivate)"
    eval "${definition/deactivate ()/_oci_relay_venv_deactivate ()}"
    deactivate() {
        _oci_relay_venv_deactivate nondestructive
        local variable
        for variable in SPARKRUN_CHECKOUT SPARKRUN_CONFIG_DIR SPARKRUN_APPLICATION_CONFIG SPARKRUN_NO_INSTALLED_PLUGINS OCI_RELAY_BINARY; do
            if [[ -v "_OCI_RELAY_DEV_PRESENT[$variable]" ]]; then
                export "$variable=${_OCI_RELAY_DEV_PREVIOUS[$variable]}"
            else
                unset "$variable"
            fi
        done
        unset _OCI_RELAY_DEV_PREVIOUS _OCI_RELAY_DEV_PRESENT
        unset -f _oci_relay_venv_deactivate
        if [[ "${1:-}" != nondestructive ]]; then
            unset -f deactivate
        fi
    }
    export SPARKRUN_CHECKOUT="$checkout" SPARKRUN_CONFIG_DIR="$config_dir"
    export SPARKRUN_APPLICATION_CONFIG="$config_dir/config.yaml" SPARKRUN_NO_INSTALLED_PLUGINS=0
    export OCI_RELAY_BINARY="$binary"
    echo "OCI Relay development environment active. Run sparkrun normally; use deactivate to restore your shell."
}

_oci_relay_dev_cleanup() {
    local status="$1"
    unset -f _oci_relay_dev_setup _oci_relay_dev_cleanup
    return "$status"
}

_oci_relay_dev_setup
_oci_relay_dev_cleanup "$?"
return $?
