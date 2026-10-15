#!/usr/bin/env bash
set -euo pipefail
ulimit -S -n "$(ulimit -H -n)"
exec "$(dirname "$0")/target/release/twelve-rust-poll"
