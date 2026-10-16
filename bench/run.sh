#!/usr/bin/env bash
set -euo pipefail

challenge_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
caller_dir=$PWD
repo_url=${BENCHMARK_REPO_URL:-https://github.com/mjc/twelve-dollar-benchmark.git}
repo_ref=${BENCHMARK_REF:-main}
tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/twelve-dollar-benchmark.XXXXXX")
trap 'rm -rf -- "$tmp_dir"' EXIT

git clone --quiet --depth 1 --branch "$repo_ref" "$repo_url" "$tmp_dir/repo"

export BENCH_CALLER_DIR="$caller_dir"
if [[ -z ${TOKENS:-} ]]; then
  export TOKENS="$challenge_root/seed/tokens.json"
elif [[ $TOKENS != /* ]]; then
  export TOKENS="$caller_dir/$TOKENS"
fi
if [[ -z ${SUMMARY:-} ]]; then
  export SUMMARY="$challenge_root/results/goose-summary.json"
elif [[ $SUMMARY != /* ]]; then
  export SUMMARY="$caller_dir/$SUMMARY"
fi

args=(--output-dir "$challenge_root/results/run-$(date -u +%Y%m%dT%H%M%SZ)-$$")
if [[ $# -gt 0 && ${1:-} != -* ]]; then
  args=("${@:1:2}" "${args[@]}" "${@:3}")
else
  args+=("$@")
fi

"$tmp_dir/repo/run.sh" "${args[@]}"
