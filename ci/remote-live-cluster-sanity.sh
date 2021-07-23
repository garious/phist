#!/usr/bin/env bash

handle_error() {
  action=$1
  set +e
  kill "$validator_then_ledger_tool_pid" "$tail_pid"
  wait "$validator_then_ledger_tool_pid" "$tail_pid"
  echo "--- Error: validator failed to $action"
  exit 1
}

show_log() {
  if find cluster-sanity/log-tail -not -empty | grep ^ > /dev/null; then
    echo "##### new log:"
    timeout 0.01 cat cluster-sanity/log-tail | tail -n 3 | cut -c 1-300 || true
    truncate --size 0 cluster-sanity/log-tail
    echo
  fi
}

rm -rf cluster-sanity
mkdir cluster-sanity

cluster_label="$1"
shift

echo "--- Starting validator $cluster_label"

validator_log="cluster-sanity/validator.log"
sys_tuner_log="cluster-sanity/sys-tuner.log"
metrics_host="https://metrics.solana.com:8086"
export SOLANA_METRICS_CONFIG="host=$metrics_host,db=testnet-live-cluster,u=scratch_writer,p=topsecret"
export RUST_LOG="warn,solana_runtime::bank=info,solana_validator=info,solana_core=info,solana_ledger=info,solana_core::repair_service=warn"

# shellcheck disable=SC2024 # create log as non-root user
sudo ./solana-sys-tuner --user "$(whoami)" &> "$sys_tuner_log" &
sys_tuner_pid=$!

(
  echo "$(date): VALIDATOR STARTED." &&
  ./solana-keygen new --force --no-passphrase --silent --outfile ./identity.json &&
  ./solana-validator \
    --identity ./identity.json \
    --ledger ./cluster-sanity/ledger \
    --no-untrusted-rpc \
    --no-poh-speed-test \
    --log - \
    --init-complete-file ./cluster-sanity/init-completed \
    --private-rpc \
    --rpc-port 8899 \
    --rpc-bind-address localhost \
    --snapshot-interval-slots 0 \
    "$@" &&
  echo "$(date): VALIDATOR FINISHED AND LEDGER-TOOL STARTED." &&
  ./solana-ledger-tool \
    --ledger cluster-sanity/ledger \
    verify &&
  echo "$(date): LEDGER-TOOL FINISHED."
) &> "$validator_log" &

validator_then_ledger_tool_pid=$!
tail -F "$validator_log" > cluster-sanity/log-tail 2> /dev/null &
tail_pid=$!

attempts=200
while ! [[ -f cluster-sanity/init-completed ]]; do
  attempts=$((attempts - 1))
  if [[ (($attempts == 0)) || ! -d "/proc/$validator_then_ledger_tool_pid" ]]; then
    handle_error "start"
  fi

  sleep 3
  echo "##### validator is starting... (until timeout: $attempts) #####"
  show_log
done
echo "##### validator finished starting! #####"

echo "--- Monitoring validator $cluster_label"

# shellcheck disable=SC2012 # ls here is handy for sorted snapshots
snapshot_slot=$(ls -t cluster-sanity/ledger/snapshot-*.tar.* |
  head -n 1 |
  grep -o 'snapshot-[0-9]*-' |
  grep -o '[0-9]*'
)
current_root=$snapshot_slot
goal_root=$((snapshot_slot + 50))

attempts=200
while [[ $current_root -le $goal_root ]]; do
  attempts=$((attempts - 1))
  if [[ (($attempts == 0)) || ! -d "/proc/$validator_then_ledger_tool_pid" ]]; then
    handle_error "root new slots"
  fi

  sleep 3
  current_root=$(./solana --url http://localhost:8899 slot --commitment root)
  echo "##### validator is running ($current_root/$goal_root)... (until timeout: $attempts) #####"
  show_log
done
echo "##### validator finished running! #####"

./solana-validator \
  --ledger cluster-sanity/ledger \
  exit --force

attempts=4000
while [[ -d "/proc/$validator_then_ledger_tool_pid" ]]; do
  attempts=$((attempts - 1))
  if [[ (($attempts == 0)) ]]; then
    handle_error "ledger tool"
  fi

  sleep 3
  echo "##### ledger-tool is running... (until timeout: $attempts) #####"
  show_log
done
echo "##### ledger-tool finished running! #####"

# well, kill $sys_tuner_pid didn't work for some reason, maybe sudo doen't relay signals?
(set -x && sleep 3 && kill "$tail_pid" && sudo pkill -f solana-sys-tuner) &
kill_pid=$!

wait "$validator_then_ledger_tool_pid" "$sys_tuner_pid" "$tail_pid" "$kill_pid"
