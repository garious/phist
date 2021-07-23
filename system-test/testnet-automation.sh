#!/usr/bin/env bash
set -e

# shellcheck disable=SC1090
# shellcheck disable=SC1091
source "$(dirname "$0")"/automation_utils.sh

function cleanup_testnet {
  RC=$?
  if [[ $RC != 0 ]]; then
    RESULT_DETAILS="
Test failed during step:
${STEP}

Failure occured when running the following command:
$(eval echo "$@")"
  fi

# shellcheck disable=SC2034
  TESTNET_FINISH_UNIX_MSECS="$(($(date +%s%N)/1000000))"
  if [[ "$UPLOAD_RESULTS_TO_SLACK" = "true" ]]; then
    upload_results_to_slack
  fi

  (
    execution_step "Collecting Logfiles from Nodes"
    collect_logs
  ) || echo "Error from collecting logs"

  (
    execution_step "Stop Network Software"
    "${REPO_ROOT}"/net/net.sh stop
  ) || echo "Error from stopping nodes"

  (
    analyze_packet_loss
  ) || echo "Error from packet loss analysis"

  execution_step "Deleting Testnet"
  "${REPO_ROOT}"/net/"${CLOUD_PROVIDER}".sh delete -p "${TESTNET_TAG}"

}
trap 'cleanup_testnet $BASH_COMMAND' EXIT

function launch_testnet() {
  set -x

  # shellcheck disable=SC2068
  execution_step "Create ${NUMBER_OF_VALIDATOR_NODES} ${CLOUD_PROVIDER} nodes"

  case $CLOUD_PROVIDER in
    gce)
      if [[ -z $VALIDATOR_NODE_MACHINE_TYPE ]]; then
        echo VALIDATOR_NODE_MACHINE_TYPE not defined
        exit 1
      fi
    # shellcheck disable=SC2068
    # shellcheck disable=SC2086
      "${REPO_ROOT}"/net/gce.sh create \
        -d pd-ssd \
        -n "$NUMBER_OF_VALIDATOR_NODES" -c "$NUMBER_OF_CLIENT_NODES" \
        $maybeCustomMachineType "$VALIDATOR_NODE_MACHINE_TYPE" $maybeEnableGpu \
        -p "$TESTNET_TAG" $maybeCreateAllowBootFailures $maybePublicIpAddresses \
        ${TESTNET_CLOUD_ZONES[@]/#/"-z "} \
        --self-destruct-hours 0 \
        ${ADDITIONAL_FLAGS[@]/#/" "}
      ;;
    ec2)
    # shellcheck disable=SC2068
    # shellcheck disable=SC2086
      "${REPO_ROOT}"/net/ec2.sh create \
        -n "$NUMBER_OF_VALIDATOR_NODES" -c "$NUMBER_OF_CLIENT_NODES" \
        $maybeCustomMachineType "$VALIDATOR_NODE_MACHINE_TYPE" $maybeEnableGpu \
        -p "$TESTNET_TAG" $maybeCreateAllowBootFailures $maybePublicIpAddresses \
        ${TESTNET_CLOUD_ZONES[@]/#/"-z "} \
        ${ADDITIONAL_FLAGS[@]/#/" "}
      ;;
    azure)
    # shellcheck disable=SC2068
    # shellcheck disable=SC2086
      "${REPO_ROOT}"/net/azure.sh create \
        -n "$NUMBER_OF_VALIDATOR_NODES" -c "$NUMBER_OF_CLIENT_NODES" \
        $maybeCustomMachineType "$VALIDATOR_NODE_MACHINE_TYPE" $maybeEnableGpu \
        -p "$TESTNET_TAG" $maybeCreateAllowBootFailures $maybePublicIpAddresses \
        ${TESTNET_CLOUD_ZONES[@]/#/"-z "} \
        ${ADDITIONAL_FLAGS[@]/#/" "}
      ;;
    colo)
      "${REPO_ROOT}"/net/colo.sh delete --reclaim-preemptible-reservations
    # shellcheck disable=SC2068
    # shellcheck disable=SC2086
      "${REPO_ROOT}"/net/colo.sh create \
        -n "$NUMBER_OF_VALIDATOR_NODES" -c "$NUMBER_OF_CLIENT_NODES" $maybeEnableGpu \
        -p "$TESTNET_TAG" $maybePublicIpAddresses --dedicated \
        ${ADDITIONAL_FLAGS[@]/#/" "}
      ;;
    *)
      echo "Error: Unsupported cloud provider: $CLOUD_PROVIDER"
      ;;
    esac

  execution_step "Configure database"
  "${REPO_ROOT}"/net/init-metrics.sh -e

  execution_step "Fetch reusable testnet keypairs"
  if [[ ! -d "${REPO_ROOT}"/net/keypairs ]]; then
    git clone git@github.com:solana-labs/testnet-keypairs.git "${REPO_ROOT}"/net/keypairs
    # If we have provider-specific keys (CoLo*, GCE*, etc) use them instead of generic val*
    if [[ -d "${REPO_ROOT}"/net/keypairs/"${CLOUD_PROVIDER}" ]]; then
      cp "${REPO_ROOT}"/net/keypairs/"${CLOUD_PROVIDER}"/* "${REPO_ROOT}"/net/keypairs/
    fi
  fi

  if [[ "$CLOUD_PROVIDER" = "colo" ]]; then
    execution_step "Stopping Colo nodes before we start"
    "${REPO_ROOT}"/net/net.sh stop
  fi

  execution_step "Starting bootstrap node and ${NUMBER_OF_VALIDATOR_NODES} validator nodes"

  declare -g version_args
  get_net_launch_software_version_launch_args "$CHANNEL" "solana-release" version_args

  declare maybeWarpSlot
  if [[ -n "$WARP_SLOT" ]]; then
    maybeWarpSlot="--warp-slot $WARP_SLOT"
  fi

  declare maybeAsyncNodeInit
  if [[ "$ASYNC_NODE_INIT" = "true" ]]; then
    maybeAsyncNodeInit="--async-node-init"
  fi

  declare maybeAllowPrivateAddr
  if [[ "$ALLOW_PRIVATE_ADDR" = "true" ]]; then
    maybeAllowPrivateAddr="--allow-private-addr"
  fi

  declare maybeExtraPrimordialStakes
  if [[ -n "$EXTRA_PRIMORDIAL_STAKES" ]]; then
    maybeExtraPrimordialStakes="--extra-primordial-stakes $EXTRA_PRIMORDIAL_STAKES"
  fi

  # shellcheck disable=SC2068
  # shellcheck disable=SC2086
  "${REPO_ROOT}"/net/net.sh start $version_args \
    -c idle=$NUMBER_OF_CLIENT_NODES $maybeStartAllowBootFailures \
    --gpu-mode $startGpuMode $maybeWarpSlot $maybeAsyncNodeInit \
    $maybeExtraPrimordialStakes $maybeAllowPrivateAddr

  if [[ -n "$WAIT_FOR_EQUAL_STAKE" ]]; then
    wait_for_equal_stake
  else
    execution_step "Waiting for bootstrap validator's stake to fall below ${BOOTSTRAP_VALIDATOR_MAX_STAKE_THRESHOLD}%"
    wait_for_max_stake "$BOOTSTRAP_VALIDATOR_MAX_STAKE_THRESHOLD"
  fi

  if [[ $NUMBER_OF_CLIENT_NODES -gt 0 ]]; then
    execution_step "Starting ${NUMBER_OF_CLIENT_NODES} client nodes"
    "${REPO_ROOT}"/net/net.sh startclients "$maybeClientOptions" "$CLIENT_OPTIONS"
    # It takes roughly 3 minutes from the time the client nodes return from starting to when they have finished loading the
    # accounts file and actually start sending transactions
    sleep 180
  fi

  if [[ -n "$WARMUP_SLOTS_BEFORE_TEST" ]]; then
    # Allow the network to run for a bit before beginning the test
    while [[ "$WARMUP_SLOTS_BEFORE_TEST" -gt $(get_slot) ]]; do
      sleep 5
    done
  fi

  # Stop the specified number of nodes
  num_online_nodes=$(( NUMBER_OF_VALIDATOR_NODES + 1 ))
  if [[ -n "$NUMBER_OF_OFFLINE_NODES" ]]; then
    execution_step "Stopping $NUMBER_OF_OFFLINE_NODES nodes"
    for (( i=NUMBER_OF_VALIDATOR_NODES; i>$(( NUMBER_OF_VALIDATOR_NODES - NUMBER_OF_OFFLINE_NODES )); i-- )); do
      # shellcheck disable=SC2154
      "${REPO_ROOT}"/net/net.sh stopnode -i "${validatorIpList[$i]}"
    done
    num_online_nodes=$(( num_online_nodes - NUMBER_OF_OFFLINE_NODES ))
  fi

  SECONDS=0
  START_SLOT=$(get_slot)
  SLOT_COUNT_START_SECONDS=$SECONDS
  execution_step "Marking beginning of slot rate test - Slot: $START_SLOT, Seconds: $SLOT_COUNT_START_SECONDS"

  case $TEST_TYPE in
    fixed_duration)
      execution_step "Wait ${TEST_DURATION_SECONDS} seconds to complete test"
      sleep "$TEST_DURATION_SECONDS"
      ;;
    partition)
      STATS_START_SECONDS=$SECONDS
      execution_step "Wait $PARTITION_INACTIVE_DURATION before beginning to apply partitions"
      sleep "$PARTITION_INACTIVE_DURATION"
      for (( i=1; i<=PARTITION_ITERATION_COUNT; i++ )); do
        execution_step "Partition Iteration $i of $PARTITION_ITERATION_COUNT"
        execution_step "Applying netem config $NETEM_CONFIG_FILE for $PARTITION_ACTIVE_DURATION seconds"
        "${REPO_ROOT}"/net/net.sh netem --config-file "$NETEM_CONFIG_FILE" -n $num_online_nodes
        sleep "$PARTITION_ACTIVE_DURATION"

        execution_step "Resolving partitions for $PARTITION_INACTIVE_DURATION seconds"
        "${REPO_ROOT}"/net/net.sh netem --config-file "$NETEM_CONFIG_FILE" --netem-cmd cleanup -n $num_online_nodes
        sleep "$PARTITION_INACTIVE_DURATION"
      done
      STATS_FINISH_SECONDS=$SECONDS
      TEST_DURATION_SECONDS=$((STATS_FINISH_SECONDS - STATS_START_SECONDS))
      ;;
    script)
      execution_step "Running custom script: ${REPO_ROOT}/${CUSTOM_SCRIPT}"
      "$REPO_ROOT"/"$CUSTOM_SCRIPT" "$RESULT_FILE"
      ;;
    *)
      echo "Error: Unsupported test type: $TEST_TYPE"
      ;;
  esac

  END_SLOT=$(get_slot)
  SLOT_COUNT_END_SECONDS=$SECONDS
  execution_step "Marking end of slot rate test - Slot: $END_SLOT, Seconds: $SLOT_COUNT_END_SECONDS"

  SLOTS_PER_SECOND="$(bc <<< "scale=3; ($END_SLOT - $START_SLOT)/($SLOT_COUNT_END_SECONDS - $SLOT_COUNT_START_SECONDS)")"
  execution_step "Average slot rate: $SLOTS_PER_SECOND slots/second over $((SLOT_COUNT_END_SECONDS - SLOT_COUNT_START_SECONDS)) seconds"

  if [[ "$SKIP_PERF_RESULTS" = "false" ]]; then
    declare -g dropped_vote_hash_count

    collect_performance_statistics
    echo "slots_per_second: $SLOTS_PER_SECOND" >>"$RESULT_FILE"

    if [[ $dropped_vote_hash_count -gt 0 ]]; then
      execution_step "Checking for dropped vote hash count"
      exit 1
    fi
  fi

  RESULT_DETAILS=$(<"$RESULT_FILE")
  upload-ci-artifact "$RESULT_FILE"
}

# shellcheck disable=SC2034
RESULT_DETAILS=
STEP=
execution_step "Initialize Environment"

[[ -n $TESTNET_TAG ]] || TESTNET_TAG=${CLOUD_PROVIDER}-testnet-automation
[[ -n $INFLUX_HOST ]] || INFLUX_HOST=https://metrics.solana.com:8086
[[ -n $BOOTSTRAP_VALIDATOR_MAX_STAKE_THRESHOLD ]] || BOOTSTRAP_VALIDATOR_MAX_STAKE_THRESHOLD=66
[[ -n $SKIP_PERF_RESULTS ]] || SKIP_PERF_RESULTS=false

if [[ -z $NUMBER_OF_VALIDATOR_NODES ]]; then
  echo NUMBER_OF_VALIDATOR_NODES not defined
  exit 1
fi

startGpuMode="off"
if [[ -z $ENABLE_GPU ]]; then
  ENABLE_GPU=false
fi
if [[ "$ENABLE_GPU" = "true" ]]; then
  maybeEnableGpu="--enable-gpu"
  startGpuMode="on"
fi

if [[ -z $NUMBER_OF_CLIENT_NODES ]]; then
  echo NUMBER_OF_CLIENT_NODES not defined
  exit 1
fi

if [[ -z $SOLANA_METRICS_CONFIG ]]; then
  if [[ -z $SOLANA_METRICS_PARTIAL_CONFIG ]]; then
    echo SOLANA_METRICS_PARTIAL_CONFIG not defined
    exit 1
  fi
  export SOLANA_METRICS_CONFIG="db=$TESTNET_TAG,host=$INFLUX_HOST,$SOLANA_METRICS_PARTIAL_CONFIG"
fi
echo "SOLANA_METRICS_CONFIG: $SOLANA_METRICS_CONFIG"

if [[ -z $ALLOW_BOOT_FAILURES ]]; then
  ALLOW_BOOT_FAILURES=false
fi
if [[ "$ALLOW_BOOT_FAILURES" = "true" ]]; then
  maybeCreateAllowBootFailures="--allow-boot-failures"
  maybeStartAllowBootFailures="-F"
fi

if [[ -z $USE_PUBLIC_IP_ADDRESSES ]]; then
  USE_PUBLIC_IP_ADDRESSES=false
fi
if [[ "$USE_PUBLIC_IP_ADDRESSES" = "true" ]]; then
  maybePublicIpAddresses="-P"
fi

execution_step "Checking for required parameters"
testTypeRequiredParameters=
case $TEST_TYPE in
  fixed_duration)
    testTypeRequiredParameters=(
      TEST_DURATION_SECONDS \
      )
    ;;
  partition)
    testTypeRequiredParameters=(
      NETEM_CONFIG_FILE \
      PARTITION_ACTIVE_DURATION \
      PARTITION_INACTIVE_DURATION \
      PARTITION_ITERATION_COUNT \
    )
    ;;
  script)
    testTypeRequiredParameters=(
      CUSTOM_SCRIPT \
    )
    ;;
  *)
    echo "Error: Unsupported test type: $TEST_TYPE"
    ;;
esac

missingParameters=
for i in "${testTypeRequiredParameters[@]}"; do
  if [[ -z ${!i} ]]; then
    missingParameters+="${i}, "
  fi
done

if [[ -n $missingParameters ]]; then
  echo "Error: For test type $TEST_TYPE, the following required parameters are missing: ${missingParameters[*]}"
  exit 1
fi

maybeClientOptions=${CLIENT_OPTIONS:+"-c"}
maybeCustomMachineType=${VALIDATOR_NODE_MACHINE_TYPE:+"--custom-machine-type"}

IFS=, read -r -a TESTNET_CLOUD_ZONES <<<"${TESTNET_ZONES}"

RESULT_FILE="$TESTNET_TAG"_SUMMARY_STATS_"$NUMBER_OF_VALIDATOR_NODES".log
rm -f "$RESULT_FILE"

TEST_PARAMS_TO_DISPLAY=(CLOUD_PROVIDER \
                        NUMBER_OF_VALIDATOR_NODES \
                        ENABLE_GPU \
                        VALIDATOR_NODE_MACHINE_TYPE \
                        NUMBER_OF_CLIENT_NODES \
                        CLIENT_OPTIONS \
                        CLIENT_DELAY_START \
                        TESTNET_ZONES \
                        TEST_DURATION_SECONDS \
                        USE_PUBLIC_IP_ADDRESSES \
                        ALLOW_BOOT_FAILURES \
                        ADDITIONAL_FLAGS \
                        APPLY_PARTITIONS \
                        NETEM_CONFIG_FILE \
                        WAIT_FOR_EQUAL_STAKE \
                        WARMUP_SLOTS_BEFORE_TEST \
                        NUMBER_OF_OFFLINE_NODES \
                        PARTITION_ACTIVE_DURATION \
                        PARTITION_INACTIVE_DURATION \
                        PARTITION_ITERATION_COUNT \
                        TEST_TYPE \
                        CUSTOM_SCRIPT \
                        )

TEST_CONFIGURATION=
for i in "${TEST_PARAMS_TO_DISPLAY[@]}"; do
  if [[ -n ${!i} ]]; then
    TEST_CONFIGURATION+="${i} = ${!i} | "
  fi
done

# shellcheck disable=SC2034
TESTNET_START_UNIX_MSECS="$(($(date +%s%N)/1000000))"

launch_testnet
