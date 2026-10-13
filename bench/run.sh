#!/usr/bin/env bash
set -euo pipefail

bench_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd -- "$bench_dir"

usage() { echo "Usage: $0 [IP PORT] [Goose options]" >&2; exit 2; }

# Keep BASE_URL and Goose flags available, or accept an explicit target IP/port.
if [[ $# -gt 0 && $1 != -* ]]; then
  if [[ $# -lt 2 || ! $2 =~ ^[0-9]{1,5}$ ]] || (( 10#$2 < 1 || 10#$2 > 65535 )); then
    usage
  fi
  target_ip=$1
  if [[ $target_ip == \[*\] ]]; then
    target_ip=${target_ip:1:${#target_ip}-2}
  fi
  if [[ $target_ip == *:* ]]; then
    [[ $target_ip =~ ^[[:xdigit:].:]+$ && $target_ip == *:*:* ]] || usage
    target_ip="[$target_ip]"
  else
    [[ $target_ip =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || usage
    IFS=. read -r -a octets <<< "$target_ip"
    for octet in "${octets[@]}"; do
      [[ $octet == 0 || $octet != 0* ]] || usage
      (( 10#$octet <= 255 )) || usage
    done
  fi
  export BASE_URL="http://$target_ip:$2"
  shift 2
fi

# Linked worktrees don't contain the ignored seed data in the main checkout.
if [[ -z ${TOKENS:-} && ! -f ../seed/tokens.json ]]; then
  if common_dir=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null); then
    if [[ -f $common_dir/../seed/tokens.json ]]; then
      export TOKENS="$common_dir/../seed/tokens.json"
    fi
  fi
fi

rust_version=1.99.0
cargo_command=(cargo)
rustc_command=(rustc)
if ! command -v cargo >/dev/null || ! command -v rustc >/dev/null ||
   [[ $(rustc --version) != "rustc $rust_version "* || $(cargo --version) != "cargo $rust_version "* ]]; then
  if ! command -v rustup >/dev/null; then
    export PATH="${CARGO_HOME:-$HOME/.cargo}/bin:$PATH"
    if ! command -v rustup >/dev/null; then
      curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
        sh -s -- -y --no-modify-path --default-toolchain none
    fi
  fi
  if ! rustup run "$rust_version" rustc --version >/dev/null 2>&1; then
    rustup toolchain install "$rust_version" --profile minimal
  fi
  cargo_command=(rustup run "$rust_version" cargo)
  rustc_command=(rustup run "$rust_version" rustc)
fi

# Select the host explicitly so inherited Cargo targets cannot select a stale binary.
rust_details=$("${rustc_command[@]}" -vV)
rust_host=${rust_details#*$'\nhost: '}
rust_host=${rust_host%%$'\n'*}
"${cargo_command[@]}" build --locked --release --target "$rust_host" --manifest-path "$bench_dir/Cargo.toml" --target-dir "$bench_dir/target"
exec "$bench_dir/target/$rust_host/release/feed-load" "$@"
