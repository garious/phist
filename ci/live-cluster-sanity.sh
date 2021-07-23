#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

source ci/_
source ci/rust-version.sh stable

if [[ -n $CI ]]; then
  escaped_branch=$(echo "$BUILDKITE_BRANCH" | tr -c "[:alnum:]" - | sed -r "s#(^-*|-*head-*|-*$)##g")
  instance_prefix="testnet-live-sanity-$escaped_branch"
else
  instance_prefix="testnet-live-sanity-$(whoami)"
fi

# ensure to delete leftover cluster
_ ./net/gce.sh delete -p "$instance_prefix" || true
# only bootstrap, no normal validator
_ ./net/gce.sh create -p "$instance_prefix" -n 0 --self-destruct-hours 1
instance_ip=$(./net/gce.sh info | grep bootstrap-validator | awk '{print $3}')

on_trap() {
  set +e
  _ ./net/gce.sh delete -p "$instance_prefix"
}
trap on_trap INT TERM EXIT

_ cargo +"$rust_stable" build --bins --release
_ ./net/scp.sh \
  ./ci/remote-live-cluster-sanity.sh \
  ./target/release/{solana,solana-keygen,solana-validator,solana-ledger-tool,solana-sys-tuner} \
  "$instance_ip:."

test_with_live_cluster() {
  cluster_label="$1"
  rm -rf "./$cluster_label"
  mkdir "./$cluster_label"

  validator_failed=
  _ ./net/ssh.sh "$instance_ip" ./remote-live-cluster-sanity.sh "$@" || validator_failed=$?

  # let's collect logs for profit!
  for log in $(./net/ssh.sh "$instance_ip" ls 'cluster-sanity/*.log'); do
    _ ./net/scp.sh "$instance_ip:$log" "./$cluster_label"
  done

  if [[ -n $validator_failed ]]; then
    # let's even collect snapshot for diagnostics
    for log in $(./net/ssh.sh "$instance_ip" ls 'cluster-sanity/ledger/snapshot-*.tar.*'); do
      _ ./net/scp.sh "$instance_ip:$log" "./$cluster_label"
    done

    (exit "$validator_failed")
  fi
}

# UPDATE docs/src/clusters.md TOO!!
test_with_live_cluster "mainnet-beta" \
    --entrypoint mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint2.mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint3.mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint4.mainnet-beta.solana.com:8001 \
    --entrypoint entrypoint5.mainnet-beta.solana.com:8001 \
    --trusted-validator 7Np41oeYqPefeNQEHSv1UDhYrehxin3NStELsSKCT4K2 \
    --trusted-validator GdnSyH3YtwcxFvQrVVJMm1JhTS4QVX7MFsX56uJLUfiZ \
    --trusted-validator DE1bawNcRJB9rVm3buyMVfr8mBEoyyu73NBovf2oXJsJ \
    --trusted-validator CakcnaRDHka2gXyfbEd2d3xsvkJkqsLw2akB3zsN1D2S \
    --expected-genesis-hash 5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d \
    # for your pain-less copy-paste

# UPDATE docs/src/clusters.md TOO!!
test_with_live_cluster "testnet" \
    --entrypoint entrypoint.testnet.solana.com:8001 \
    --trusted-validator 5D1fNXzvv5NjV1ysLjirC4WY92RNsVH18vjmcszZd8on \
    --trusted-validator ta1Uvfb7W5BRPrdGnhP9RmeCGKzBySGM1hTE4rBRy6T \
    --trusted-validator Ft5fbkqNa76vnsjYNwjDZUXoTWpP7VYm3mtsaQckQADN \
    --trusted-validator 9QxCLckBiJc783jnMvXZubK4wH86Eqqvashtrwvcsgkv \
    --expected-genesis-hash 4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY \
    # for your pain-less copy-paste
