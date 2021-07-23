#!/usr/bin/env bash

cd "$(dirname "$0")/.."

export CI_LOCAL_RUN=true

set -e

case $(uname -o) in
  */Linux)
    export CI_OS_NAME=linux
    ;;
  *)
    echo "local CI runs are only supported on Linux" 1>&2
    exit 1
    ;;
esac

here="$(dirname "$0")"
cargo="$(readlink -f "${here}/../cargo")"
"$cargo" audit --version || "$cargo" install cargo-audit --version 0.14.1
grcov --version || "$cargo" install grcov --version 0.8.0
ulimit -n 500000

steps=()
steps+=(test-sanity)
steps+=(shellcheck)
steps+=(test-checks)
steps+=(test-coverage)
steps+=(test-stable)
steps+=(test-stable-bpf)
steps+=(test-stable-perf)
steps+=(test-downstream-builds)
steps+=(test-bench)
steps+=(test-local-cluster)

step_index=0
if [[ -n "$1" ]]; then
  start_step="$1"
  while [[ $step_index -lt ${#steps[@]} ]]; do
    step="${steps[$step_index]}"
    if [[ "$step" = "$start_step" ]]; then
      break
    fi
    step_index=$((step_index + 1))
  done
  if [[ $step_index -eq ${#steps[@]} ]]; then
    echo "unexpected start step: \"$start_step\"" 1>&2
    exit 1
  else
    echo "** starting at step: \"$start_step\" **"
    echo
  fi
fi

while [[ $step_index -lt ${#steps[@]} ]]; do
  step="${steps[$step_index]}"
  cmd="ci/${step}.sh"
  $cmd
  step_index=$((step_index + 1))
done
