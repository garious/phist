#!/usr/bin/env bash
set -e

here=$(dirname "$0")
SOLANA_ROOT="$(cd "$here"/..; pwd)"

# shellcheck source=net/common.sh
source "$here"/common.sh

usage() {
  exitcode=0
  if [[ -n "$1" ]]; then
    exitcode=1
    echo "Error: $*"
  fi
  CLIENT_OPTIONS=$(cat << EOM
-c clientType=numClients=extraArgs - Number of clientTypes to start.  This options can be specified
                                     more than once.  Defaults to bench-tps for all clients if not
                                     specified.
                                     Valid client types are:
                                         idle
                                         bench-tps
                                     User can optionally provide extraArgs that are transparently
                                     supplied to the client program as command line parameters.
                                     For example,
                                         -c bench-tps=2="--tx_count 25000"
                                     This will start 2 bench-tps clients, and supply "--tx_count 25000"
                                     to the bench-tps client.
EOM
)
  cat <<EOF
usage: $0 [start|stop|restart|sanity] [command-specific options]

Operate a configured testnet

 start        - Start the network
 sanity       - Sanity check the network
 stop         - Stop the network
 restart      - Shortcut for stop then start
 logs         - Fetch remote logs from each network node
 startnode    - Start an individual node (previously stopped with stopNode)
 stopnode     - Stop an individual node
 startclients - Start client nodes only
 prepare      - Prepare software deployment. (Build/download the software release)
 update       - Deploy a new software update to the cluster
 upgrade      - Upgrade software on bootstrap validator. (Restart bootstrap validator manually to run it)

 start-specific options:
   -T [tarFilename]                   - Deploy the specified release tarball
   -t edge|beta|stable|vX.Y.Z         - Deploy the latest tarball release for the
                                        specified release channel (edge|beta|stable) or release tag
                                        (vX.Y.Z)
   -r / --skip-setup                  - Reuse existing node/ledger configuration from a
                                        previous |start| (ie, don't run ./multinode-demo/setup.sh).
   -d / --debug                       - Build/deploy the testnet with debug binaries
   $CLIENT_OPTIONS
   --client-delay-start
                                      - Number of seconds to wait after validators have finished starting before starting client programs
                                        (default: $clientDelayStart)
   -n NUM_VALIDATORS                  - Number of validators to apply command to.
   --gpu-mode GPU_MODE                - Specify GPU mode to launch validators with (default: $gpuMode).
                                        MODE must be one of
                                          on - GPU *required*, any vendor *
                                          off - No GPU, CPU-only
                                          auto - Use GPU if available, any vendor *
                                          cuda - GPU *required*, Nvidia CUDA only
                                          *  Currently, Nvidia CUDA is the only supported GPU vendor
   --hashes-per-tick NUM_HASHES|sleep|auto
                                      - Override the default --hashes-per-tick for the cluster
   --no-airdrop
                                      - If set, disables the faucet keypair.  Nodes must be funded in genesis config
   --faucet-lamports NUM_LAMPORTS_TO_MINT
                                      - Override the default 500000000000000000 lamports minted in genesis
   --extra-primordial-stakes NUM_EXTRA_PRIMORDIAL_STAKES
                                      - Number of extra nodes to be initially staked in genesis.
                                        Implies --wait-for-supermajority 1 --async-node-init and the supermajority
                                        wait slot may be overridden with the corresponding flag
   --internal-nodes-stake-lamports NUM_LAMPORTS_PER_NODE
                                      - Amount to stake internal nodes.
   --internal-nodes-lamports NUM_LAMPORTS_PER_NODE
                                      - Amount to fund internal nodes in genesis config.
   --external-accounts-file FILE_PATH
                                      - A YML file with a list of account pubkeys and corresponding lamport balances
                                        in genesis config for external nodes
   --no-snapshot-fetch
                                      - If set, disables booting validators from a snapshot
   --skip-poh-verify
                                      - If set, validators will skip verifying
                                        the ledger they already have saved to disk at
                                        boot (results in a much faster boot)
   --no-deploy
                                      - Don't deploy new software, use the
                                        existing deployment
   --no-build
                                      - Don't build new software, deploy the
                                        existing binaries

   --deploy-if-newer                  - Only deploy if newer software is
                                        available (requires -t or -T)

   --cluster-type development|devnet|testnet|mainnet-beta
                                      - Specify whether or not to launch the cluster in "development" mode with all features enabled at epoch 0,
                                        or various other live clusters' feature set (default: development)
   --slots-per-epoch SLOTS
                                      - Override the number of slots in an epoch
   --warp-slot WARP_SLOT
                                      - Boot from a snapshot that has warped ahead to WARP_SLOT rather than a slot 0 genesis.
 sanity/start-specific options:
   -F                   - Discard validator nodes that didn't bootup successfully
   -o noInstallCheck    - Skip solana-install sanity
   -o rejectExtraNodes  - Require the exact number of nodes

 stop-specific options:
   none

 logs-specific options:
   none

 netem-specific options:
   --config            - Netem configuration (as a double quoted string)
   --parition          - Percentage of network that should be configured with netem
   --config-file       - Configuration file for partition and netem configuration
   --netem-cmd         - Optional command argument to netem. Default is "add". Use "cleanup" to remove rules.

 update-specific options:
   --platform linux|osx|windows       - Deploy the tarball using 'solana-install deploy ...' for the
                                        given platform (multiple platforms may be specified)
                                        (-t option must be supplied as well)

 startnode/stopnode-specific options:
   -i [ip address]                    - IP Address of the node to start or stop

 startclients-specific options:
   $CLIENT_OPTIONS

Note: if RUST_LOG is set in the environment it will be propogated into the
      network nodes.
EOF
  exit $exitcode
}

initLogDir() { # Initializes the netLogDir global variable.  Idempotent
  [[ -z $netLogDir ]] || return 0

  netLogDir="$netDir"/log
  declare netLogDateDir
  netLogDateDir="$netDir"/log-$(date +"%Y-%m-%d_%H_%M_%S")
  if [[ -d $netLogDir && ! -L $netLogDir ]]; then
    echo "Warning: moving $netLogDir to make way for symlink."
    mv "$netLogDir" "$netDir"/log.old
  elif [[ -L $netLogDir ]]; then
    rm "$netLogDir"
  fi
  mkdir -p "$netConfigDir" "$netLogDateDir"
  ln -sf "$netLogDateDir" "$netLogDir"
  echo "Log directory: $netLogDateDir"
}

annotate() {
  [[ -z $BUILDKITE ]] || {
    buildkite-agent annotate "$@"
  }
}

annotateBlockexplorerUrl() {
  declare blockstreamer=${blockstreamerIpList[0]}

  if [[ -n $blockstreamer ]]; then
    annotate --style info --context blockexplorer-url "Block explorer: http://$blockstreamer/"
  fi
}

build() {
  supported=("20.04")
  declare MAYBE_DOCKER=
  if [[ $(uname) != Linux || ! " ${supported[*]} " =~ $(lsb_release -sr) ]]; then
    # shellcheck source=ci/rust-version.sh
    source "$SOLANA_ROOT"/ci/rust-version.sh
    MAYBE_DOCKER="ci/docker-run.sh $rust_stable_docker_image"
  fi
  SECONDS=0
  (
    cd "$SOLANA_ROOT"
    echo "--- Build started at $(date)"

    set -x
    rm -rf farf

    buildVariant=
    if $debugBuild; then
      buildVariant=--debug
    fi

    $MAYBE_DOCKER bash -c "
      set -ex
      scripts/cargo-install-all.sh farf $buildVariant --validator-only
    "
  )

  (
    set +e
    COMMIT="$(git rev-parse HEAD)"
    BRANCH="$(git rev-parse --abbrev-ref HEAD)"
    TAG="$(git describe --exact-match --tags HEAD 2>/dev/null)"
    if [[ $TAG =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]; then
      NOTE=$TAG
    else
      NOTE=$BRANCH
    fi
    (
      echo "channel: devbuild $NOTE"
      echo "commit: $COMMIT"
    ) > "$SOLANA_ROOT"/farf/version.yml
  )
  echo "Build took $SECONDS seconds"
}

SOLANA_HOME="\$HOME/solana"
CARGO_BIN="\$HOME/.cargo/bin"

startCommon() {
  declare ipAddress=$1
  test -d "$SOLANA_ROOT"
  if $skipSetup; then
    # shellcheck disable=SC2029
    ssh "${sshOptions[@]}" "$ipAddress" "
      set -x;
      mkdir -p $SOLANA_HOME/config;
      rm -rf ~/config;
      mv $SOLANA_HOME/config ~;
      rm -rf $SOLANA_HOME;
      mkdir -p $SOLANA_HOME $CARGO_BIN;
      mv ~/config $SOLANA_HOME/
    "
  else
    # shellcheck disable=SC2029
    ssh "${sshOptions[@]}" "$ipAddress" "
      set -x;
      rm -rf $SOLANA_HOME;
      mkdir -p $CARGO_BIN
    "
  fi
  [[ -z "$externalNodeSshKey" ]] || ssh-copy-id -f -i "$externalNodeSshKey" "${sshOptions[@]}" "solana@$ipAddress"
  syncScripts "$ipAddress"
}

syncScripts() {
  echo "rsyncing scripts... to $ipAddress"
  declare ipAddress=$1
  rsync -vPrc -e "ssh ${sshOptions[*]}" \
    --exclude 'net/log*' \
    "$SOLANA_ROOT"/{fetch-perf-libs.sh,fetch-spl.sh,scripts,net,multinode-demo} \
    "$ipAddress":"$SOLANA_HOME"/ > /dev/null
}

# Deploy local binaries to bootstrap validator.  Other validators and clients later fetch the
# binaries from it
deployBootstrapValidator() {
  declare ipAddress=$1

  echo "Deploying software to bootstrap validator ($ipAddress)"
  case $deployMethod in
  tar)
    rsync -vPrc -e "ssh ${sshOptions[*]}" "$SOLANA_ROOT"/solana-release/bin/* "$ipAddress:$CARGO_BIN/"
    rsync -vPrc -e "ssh ${sshOptions[*]}" "$SOLANA_ROOT"/solana-release/version.yml "$ipAddress:~/"
    ;;
  local)
    rsync -vPrc -e "ssh ${sshOptions[*]}" "$SOLANA_ROOT"/farf/bin/* "$ipAddress:$CARGO_BIN/"
    rsync -vPrc -e "ssh ${sshOptions[*]}" "$SOLANA_ROOT"/farf/version.yml "$ipAddress:~/"
    ;;
  skip)
    ;;
  *)
    usage "Internal error: invalid deployMethod: $deployMethod"
    ;;
  esac
}

startBootstrapLeader() {
  declare ipAddress=$1
  declare nodeIndex="$2"
  declare logFile="$3"
  echo "--- Starting bootstrap validator: $ipAddress"
  echo "start log: $logFile"

  (
    set -x
    startCommon "$ipAddress" || exit 1
    [[ -z "$externalPrimordialAccountsFile" ]] || rsync -vPrc -e "ssh ${sshOptions[*]}" "$externalPrimordialAccountsFile" \
      "$ipAddress:$remoteExternalPrimordialAccountsFile"

    deployBootstrapValidator "$ipAddress"

    ssh "${sshOptions[@]}" -n "$ipAddress" \
      "./solana/net/remote/remote-node.sh \
         $deployMethod \
         bootstrap-validator \
         $entrypointIp \
         $((${#validatorIpList[@]} + ${#blockstreamerIpList[@]})) \
         \"$RUST_LOG\" \
         $skipSetup \
         $failOnValidatorBootupFailure \
         \"$remoteExternalPrimordialAccountsFile\" \
         \"$maybeDisableAirdrops\" \
         \"$internalNodesStakeLamports\" \
         \"$internalNodesLamports\" \
         $nodeIndex \
         ${#clientIpList[@]} \"$benchTpsExtraArgs\" \
         \"$genesisOptions\" \
         \"$maybeNoSnapshot $maybeSkipLedgerVerify $maybeLimitLedgerSize $maybeWaitForSupermajority $maybeAllowPrivateAddr\" \
         \"$gpuMode\" \
         \"$maybeWarpSlot\" \
         \"$waitForNodeInit\" \
         \"$extraPrimordialStakes\" \
         \"$TMPFS_ACCOUNTS\" \
      "

  ) >> "$logFile" 2>&1 || {
    cat "$logFile"
    echo "^^^ +++"
    exit 1
  }
}

startNode() {
  declare ipAddress=$1
  declare nodeType=$2
  declare nodeIndex="$3"

  initLogDir
  declare logFile="$netLogDir/validator-$ipAddress.log"

  if [[ -z $nodeType ]]; then
    echo nodeType not specified
    exit 1
  fi

  if [[ -z $nodeIndex ]]; then
    echo nodeIndex not specified
    exit 1
  fi

  echo "--- Starting $nodeType: $ipAddress"
  echo "start log: $logFile"
  (
    set -x
    startCommon "$ipAddress"

    if [[ $nodeType = blockstreamer ]] && [[ -n $letsEncryptDomainName ]]; then
      #
      # Create/renew TLS certificate
      #
      declare localArchive=~/letsencrypt-"$letsEncryptDomainName".tgz
      if [[ -r "$localArchive" ]]; then
        timeout 30s scp "${sshOptions[@]}" "$localArchive" "$ipAddress:letsencrypt.tgz"
      fi
      ssh "${sshOptions[@]}" -n "$ipAddress" \
        "sudo -H /certbot-restore.sh $letsEncryptDomainName maintainers@solana.foundation"
      rm -f letsencrypt.tgz
      timeout 30s scp "${sshOptions[@]}" "$ipAddress:/letsencrypt.tgz" letsencrypt.tgz
      test -s letsencrypt.tgz # Ensure non-empty before overwriting $localArchive
      cp letsencrypt.tgz "$localArchive"
    fi

    ssh "${sshOptions[@]}" -n "$ipAddress" \
      "./solana/net/remote/remote-node.sh \
         $deployMethod \
         $nodeType \
         $entrypointIp \
         $((${#validatorIpList[@]} + ${#blockstreamerIpList[@]})) \
         \"$RUST_LOG\" \
         $skipSetup \
         $failOnValidatorBootupFailure \
         \"$remoteExternalPrimordialAccountsFile\" \
         \"$maybeDisableAirdrops\" \
         \"$internalNodesStakeLamports\" \
         \"$internalNodesLamports\" \
         $nodeIndex \
         ${#clientIpList[@]} \"$benchTpsExtraArgs\" \
         \"$genesisOptions\" \
         \"$maybeNoSnapshot $maybeSkipLedgerVerify $maybeLimitLedgerSize $maybeWaitForSupermajority $maybeAllowPrivateAddr\" \
         \"$gpuMode\" \
         \"$maybeWarpSlot\" \
         \"$waitForNodeInit\" \
         \"$extraPrimordialStakes\" \
         \"$TMPFS_ACCOUNTS\" \
      "
  ) >> "$logFile" 2>&1 &
  declare pid=$!
  ln -sf "validator-$ipAddress.log" "$netLogDir/validator-$pid.log"
  pids+=("$pid")
}

startClient() {
  declare ipAddress=$1
  declare clientToRun="$2"
  declare clientIndex="$3"

  initLogDir
  declare logFile="$netLogDir/client-$clientToRun-$ipAddress.log"

  echo "--- Starting client: $ipAddress - $clientToRun"
  echo "start log: $logFile"
  (
    set -x
    startCommon "$ipAddress"
    ssh "${sshOptions[@]}" -f "$ipAddress" \
      "./solana/net/remote/remote-client.sh $deployMethod $entrypointIp \
      $clientToRun \"$RUST_LOG\" \"$benchTpsExtraArgs\" $clientIndex"
  ) >> "$logFile" 2>&1 || {
    cat "$logFile"
    echo "^^^ +++"
    exit 1
  }
}

startClients() {
  for ((i=0; i < "$numClients" && i < "$numClientsRequested"; i++)) do
    if [[ $i -lt "$numBenchTpsClients" ]]; then
      startClient "${clientIpList[$i]}" "solana-bench-tps" "$i"
    else
      startClient "${clientIpList[$i]}" "idle"
    fi
  done
}

sanity() {
  declare skipBlockstreamerSanity=$1

  $metricsWriteDatapoint "testnet-deploy net-sanity-begin=1"

  declare ok=true
  declare bootstrapLeader=${validatorIpList[0]}
  declare blockstreamer=${blockstreamerIpList[0]}

  annotateBlockexplorerUrl

  echo "--- Sanity: $bootstrapLeader"
  (
    set -x
    # shellcheck disable=SC2029 # remote-client.sh args are expanded on client side intentionally
    ssh "${sshOptions[@]}" "$bootstrapLeader" \
      "./solana/net/remote/remote-sanity.sh $bootstrapLeader $sanityExtraArgs \"$RUST_LOG\""
  ) || ok=false
  $ok || exit 1

  if [[ -z $skipBlockstreamerSanity && -n $blockstreamer ]]; then
    # If there's a blockstreamer node run a reduced sanity check on it as well
    echo "--- Sanity: $blockstreamer"
    (
      set -x
      # shellcheck disable=SC2029 # remote-client.sh args are expanded on client side intentionally
      ssh "${sshOptions[@]}" "$blockstreamer" \
        "./solana/net/remote/remote-sanity.sh $blockstreamer $sanityExtraArgs \"$RUST_LOG\""
    ) || ok=false
    $ok || exit 1
  fi

  $metricsWriteDatapoint "testnet-deploy net-sanity-complete=1"
}

deployUpdate() {
  if [[ -z $updatePlatforms ]]; then
    echo "No update platforms"
    return
  fi
  if [[ -z $releaseChannel ]]; then
    echo "Release channel not specified (use -t option)"
    exit 1
  fi

  declare ok=true
  declare bootstrapLeader=${validatorIpList[0]}

  for updatePlatform in $updatePlatforms; do
    echo "--- Deploying solana-install update: $updatePlatform"
    (
      set -x

      scripts/solana-install-update-manifest-keypair.sh "$updatePlatform"

      timeout 30s scp "${sshOptions[@]}" \
        update_manifest_keypair.json "$bootstrapLeader:solana/update_manifest_keypair.json"

      # shellcheck disable=SC2029 # remote-deploy-update.sh args are expanded on client side intentionally
      ssh "${sshOptions[@]}" "$bootstrapLeader" \
        "./solana/net/remote/remote-deploy-update.sh $releaseChannel $updatePlatform"
    ) || ok=false
    $ok || exit 1
  done
}

getNodeType() {
  echo "getNodeType: $nodeAddress"
  [[ -n $nodeAddress ]] || {
    echo "Error: nodeAddress not set"
    exit 1
  }
  nodeIndex=0 # <-- global
  nodeType=validator # <-- global

  for ipAddress in "${validatorIpList[@]}" b "${blockstreamerIpList[@]}"; do
    if [[ $ipAddress = b ]]; then
      nodeType=blockstreamer
      continue
    fi

    if [[ $ipAddress = "$nodeAddress" ]]; then
      echo "getNodeType: $nodeType ($nodeIndex)"
      return
    fi
    ((nodeIndex = nodeIndex + 1))
  done

  echo "Error: Unknown node: $nodeAddress"
  exit 1
}

prepareDeploy() {
  case $deployMethod in
  tar)
    if [[ -n $releaseChannel ]]; then
      echo "Downloading release from channel: $releaseChannel"
      rm -f "$SOLANA_ROOT"/solana-release.tar.bz2
      declare updateDownloadUrl=https://release.solana.com/"$releaseChannel"/solana-release-x86_64-unknown-linux-gnu.tar.bz2
      (
        set -x
        curl -L -I "$updateDownloadUrl"
        curl -L --retry 5 --retry-delay 2 --retry-connrefused \
          -o "$SOLANA_ROOT"/solana-release.tar.bz2 "$updateDownloadUrl"
      )
      tarballFilename="$SOLANA_ROOT"/solana-release.tar.bz2
    fi
    (
      set -x
      rm -rf "$SOLANA_ROOT"/solana-release
      cd "$SOLANA_ROOT"; tar jfxv "$tarballFilename"
      cat "$SOLANA_ROOT"/solana-release/version.yml
    )
    ;;
  local)
    if $doBuild; then
      build
    else
      echo "Build skipped due to --no-build"
    fi
    ;;
  skip)
    ;;
  *)
    usage "Internal error: invalid deployMethod: $deployMethod"
    ;;
  esac

  if [[ -n $deployIfNewer ]]; then
    if [[ $deployMethod != tar ]]; then
      echo "Error: --deploy-if-newer only supported for tar deployments"
      exit 1
    fi

    echo "Fetching current software version"
    (
      set -x
      rsync -vPrc -e "ssh ${sshOptions[*]}" "${validatorIpList[0]}":~/version.yml current-version.yml
    )
    cat current-version.yml
    if ! diff -q current-version.yml "$SOLANA_ROOT"/solana-release/version.yml; then
      echo "Cluster software version is old.  Update required"
    else
      echo "Cluster software version is current.  No update required"
      exit 0
    fi
  fi
}

deploy() {
  initLogDir

  echo "Deployment started at $(date)"
  $metricsWriteDatapoint "testnet-deploy net-start-begin=1"

  declare bootstrapLeader=true
  for nodeAddress in "${validatorIpList[@]}" "${blockstreamerIpList[@]}"; do
    nodeType=
    nodeIndex=
    getNodeType
    if $bootstrapLeader; then
      SECONDS=0
      declare bootstrapNodeDeployTime=
      startBootstrapLeader "$nodeAddress" $nodeIndex "$netLogDir/bootstrap-validator-$ipAddress.log"
      bootstrapNodeDeployTime=$SECONDS
      $metricsWriteDatapoint "testnet-deploy net-bootnode-leader-started=1"

      bootstrapLeader=false
      SECONDS=0
      pids=()
    else
      startNode "$ipAddress" $nodeType $nodeIndex

      # Stagger additional node start time. If too many nodes start simultaneously
      # the bootstrap node gets more rsync requests from the additional nodes than
      # it can handle.
      sleep 2
    fi
  done


  for pid in "${pids[@]}"; do
    declare ok=true
    wait "$pid" || ok=false
    if ! $ok; then
      echo "+++ validator failed to start"
      cat "$netLogDir/validator-$pid.log"
      if $failOnValidatorBootupFailure; then
        exit 1
      else
        echo "Failure is non-fatal"
      fi
    fi
  done

  if ! $waitForNodeInit; then
    # Handle async init
    declare startTime=$SECONDS
    for ipAddress in "${validatorIpList[@]}" "${blockstreamerIpList[@]}"; do
      declare timeWaited=$((SECONDS - startTime))
      if [[ $timeWaited -gt 600 ]]; then
        break
      fi
      ssh "${sshOptions[@]}" -n "$ipAddress" \
        "./solana/net/remote/remote-node-wait-init.sh $((600 - timeWaited))"
    done
  fi

  $metricsWriteDatapoint "testnet-deploy net-validators-started=1"
  additionalNodeDeployTime=$SECONDS

  annotateBlockexplorerUrl

  sanity skipBlockstreamerSanity # skip sanity on blockstreamer node, it may not
                                 # have caught up to the bootstrap validator yet

  echo "--- Sleeping $clientDelayStart seconds after validators are started before starting clients"
  sleep "$clientDelayStart"

  SECONDS=0
  startClients
  clientDeployTime=$SECONDS

  $metricsWriteDatapoint "testnet-deploy net-start-complete=1"

  declare networkVersion=unknown
  case $deployMethod in
  tar)
    networkVersion="$(
      (
        set -o pipefail
        grep "^commit: " "$SOLANA_ROOT"/solana-release/version.yml | head -n1 | cut -d\  -f2
      ) || echo "tar-unknown"
    )"
    ;;
  local)
    networkVersion="$(git rev-parse HEAD || echo local-unknown)"
    ;;
  skip)
    ;;
  *)
    usage "Internal error: invalid deployMethod: $deployMethod"
    ;;
  esac
  $metricsWriteDatapoint "testnet-deploy version=\"${networkVersion:0:9}\""

  echo
  echo "--- Deployment Successful"
  echo "Bootstrap validator deployment took $bootstrapNodeDeployTime seconds"
  echo "Additional validator deployment (${#validatorIpList[@]} validators, ${#blockstreamerIpList[@]} blockstreamer nodes) took $additionalNodeDeployTime seconds"
  echo "Client deployment (${#clientIpList[@]} instances) took $clientDeployTime seconds"
  echo "Network start logs in $netLogDir"
}

stopNode() {
  local ipAddress=$1
  local block=$2

  initLogDir
  declare logFile="$netLogDir/stop-validator-$ipAddress.log"

  echo "--- Stopping node: $ipAddress"
  echo "stop log: $logFile"
  syncScripts "$ipAddress"
  (
    # Since cleanup.sh does a pkill, we cannot pass the command directly,
    # otherwise the process which is doing the killing will be killed because
    # the script itself will match the pkill pattern
    set -x
    # shellcheck disable=SC2029 # It's desired that PS4 be expanded on the client side
    ssh "${sshOptions[@]}" "$ipAddress" "PS4=\"$PS4\" ./solana/net/remote/cleanup.sh"
  ) >> "$logFile" 2>&1 &

  declare pid=$!
  ln -sf "stop-validator-$ipAddress.log" "$netLogDir/stop-validator-$pid.log"
  if $block; then
    wait $pid || true
  else
    pids+=("$pid")
  fi
}

stop() {
  SECONDS=0
  $metricsWriteDatapoint "testnet-deploy net-stop-begin=1"

  declare loopCount=0
  pids=()
  for ipAddress in "${validatorIpList[@]}" "${blockstreamerIpList[@]}" "${clientIpList[@]}"; do
    stopNode "$ipAddress" false

    # Stagger additional node stop time to avoid too many concurrent ssh
    # sessions
    ((loopCount++ % 4 == 0)) && sleep 2
  done

  echo --- Waiting for nodes to finish stopping
  for pid in "${pids[@]}"; do
    echo -n "$pid "
    wait "$pid" || true
  done
  echo

  $metricsWriteDatapoint "testnet-deploy net-stop-complete=1"
  echo "Stopping nodes took $SECONDS seconds"
}

checkPremptibleInstances() {
  # The validatorIpList nodes may be preemptible instances that can disappear at
  # any time.  Try to detect when a validator has been preempted to help the user
  # out.
  #
  # Of course this isn't airtight as an instance could always disappear
  # immediately after its successfully pinged.
  for ipAddress in "${validatorIpList[@]}"; do
    (
      timeout 5s ping -c 1 "$ipAddress" | tr - _ &>/dev/null
    ) || {
      cat <<EOF

Warning: $ipAddress may have been preempted.

Run |./gce.sh config| to restart it
EOF
      exit 1
    }
  done
}

releaseChannel=
deployMethod=local
deployIfNewer=
sanityExtraArgs=
skipSetup=false
updatePlatforms=
nodeAddress=
numIdleClients=0
numBenchTpsClients=0
benchTpsExtraArgs=
failOnValidatorBootupFailure=true
genesisOptions=
numValidatorsRequested=
externalPrimordialAccountsFile=
remoteExternalPrimordialAccountsFile=
internalNodesStakeLamports=
internalNodesLamports=
maybeNoSnapshot=""
maybeLimitLedgerSize=""
maybeSkipLedgerVerify=""
maybeDisableAirdrops=""
maybeWaitForSupermajority=""
maybeAllowPrivateAddr=""
debugBuild=false
doBuild=true
gpuMode=auto
netemPartition=""
netemConfig=""
netemConfigFile=""
netemCommand="add"
clientDelayStart=0
netLogDir=
maybeWarpSlot=
waitForNodeInit=true
extraPrimordialStakes=0

command=$1
[[ -n $command ]] || usage
shift

shortArgs=()
while [[ -n $1 ]]; do
  if [[ ${1:0:2} = -- ]]; then
    if [[ $1 = --hashes-per-tick ]]; then
      genesisOptions="$genesisOptions $1 $2"
      shift 2
    elif [[ $1 = --slots-per-epoch ]]; then
      genesisOptions="$genesisOptions $1 $2"
      shift 2
    elif [[ $1 = --target-lamports-per-signature ]]; then
      genesisOptions="$genesisOptions $1 $2"
      shift 2
    elif [[ $1 = --faucet-lamports ]]; then
      genesisOptions="$genesisOptions $1 $2"
      shift 2
    elif [[ $1 = --cluster-type ]]; then
      case "$2" in
        development|devnet|testnet|mainnet-beta)
          ;;
        *)
          echo "Unexpected cluster type: \"$2\""
          exit 1
          ;;
      esac
      genesisOptions="$genesisOptions $1 $2"
      shift 2
    elif [[ $1 = --slots-per-epoch ]]; then
      genesisOptions="$genesisOptions $1 $2"
      shift 2
    elif [[ $1 = --no-snapshot-fetch ]]; then
      maybeNoSnapshot="$1"
      shift 1
    elif [[ $1 = --deploy-if-newer ]]; then
      deployIfNewer=1
      shift 1
    elif [[ $1 = --no-deploy ]]; then
      deployMethod=skip
      shift 1
    elif [[ $1 = --no-build ]]; then
      doBuild=false
      shift 1
    elif [[ $1 = --limit-ledger-size ]]; then
      maybeLimitLedgerSize="$1 $2"
      shift 2
    elif [[ $1 = --skip-poh-verify ]]; then
      maybeSkipLedgerVerify="$1"
      shift 1
    elif [[ $1 = --skip-setup ]]; then
      skipSetup=true
      shift 1
    elif [[ $1 = --platform ]]; then
      updatePlatforms="$updatePlatforms $2"
      shift 2
    elif [[ $1 = --internal-nodes-stake-lamports ]]; then
      internalNodesStakeLamports="$2"
      shift 2
    elif [[ $1 = --internal-nodes-lamports ]]; then
      internalNodesLamports="$2"
      shift 2
    elif [[ $1 = --external-accounts-file ]]; then
      externalPrimordialAccountsFile="$2"
      remoteExternalPrimordialAccountsFile=/tmp/external-primordial-accounts.yml
      shift 2
    elif [[ $1 = --no-airdrop ]]; then
      maybeDisableAirdrops="$1"
      shift 1
    elif [[ $1 = --debug ]]; then
      debugBuild=true
      shift 1
    elif [[ $1 = --partition ]]; then
      netemPartition=$2
      shift 2
    elif [[ $1 = --config ]]; then
      netemConfig=$2
      shift 2
    elif [[ $1 == --config-file ]]; then
      netemConfigFile=$2
      shift 2
    elif [[ $1 == --netem-cmd ]]; then
      netemCommand=$2
      shift 2
    elif [[ $1 = --gpu-mode ]]; then
      gpuMode=$2
      case "$gpuMode" in
        on|off|auto|cuda)
          ;;
        *)
          echo "Unexpected GPU mode: \"$gpuMode\""
          exit 1
          ;;
      esac
      shift 2
    elif [[ $1 == --client-delay-start ]]; then
      clientDelayStart=$2
      shift 2
    elif [[ $1 == --wait-for-supermajority ]]; then
      maybeWaitForSupermajority="$1 $2"
      shift 2
    elif [[ $1 == --warp-slot ]]; then
      maybeWarpSlot="$1 $2"
      shift 2
    elif [[ $1 == --async-node-init ]]; then
      waitForNodeInit=false
      shift 1
    elif [[ $1 == --extra-primordial-stakes ]]; then
      extraPrimordialStakes=$2
      shift 2
    elif [[ $1 = --allow-private-addr ]]; then
      maybeAllowPrivateAddr="$1"
      shift 1
    else
      usage "Unknown long option: $1"
    fi
  else
    shortArgs+=("$1")
    shift
  fi
done

while getopts "h?T:t:o:f:rc:Fn:i:d" opt "${shortArgs[@]}"; do
  case $opt in
  h | \?)
    usage
    ;;
  T)
    tarballFilename=$OPTARG
    [[ -r $tarballFilename ]] || usage "File not readable: $tarballFilename"
    deployMethod=tar
    ;;
  t)
    case $OPTARG in
    edge|beta|stable|v*)
      releaseChannel=$OPTARG
      deployMethod=tar
      ;;
    *)
      usage "Invalid release channel: $OPTARG"
      ;;
    esac
    ;;
  n)
    numValidatorsRequested=$OPTARG
    ;;
  r)
    skipSetup=true
    ;;
  o)
    case $OPTARG in
    rejectExtraNodes|noInstallCheck)
      sanityExtraArgs="$sanityExtraArgs -o $OPTARG"
      ;;
    *)
      usage "Unknown option: $OPTARG"
      ;;
    esac
    ;;
  c)
    getClientTypeAndNum() {
      if ! [[ $OPTARG == *'='* ]]; then
        echo "Error: Expecting tuple \"clientType=numClientType=extraArgs\" but got \"$OPTARG\""
        exit 1
      fi
      local keyValue
      IFS='=' read -ra keyValue <<< "$OPTARG"
      local clientType=${keyValue[0]}
      local numClients=${keyValue[1]}
      local extraArgs=${keyValue[2]}
      re='^[0-9]+$'
      if ! [[ $numClients =~ $re ]] ; then
        echo "error: numClientType must be a number but got \"$numClients\""
        exit 1
      fi
      case $clientType in
        idle)
          numIdleClients=$numClients
          # $extraArgs ignored for 'idle'
        ;;
        bench-tps)
          numBenchTpsClients=$numClients
          benchTpsExtraArgs=$extraArgs
        ;;
        *)
          echo "Unknown client type: $clientType"
          exit 1
          ;;
      esac
    }
    getClientTypeAndNum
    ;;
  F)
    failOnValidatorBootupFailure=false
    ;;
  i)
    nodeAddress=$OPTARG
    ;;
  d)
    debugBuild=true
    ;;
  *)
    usage "Error: unhandled option: $opt"
    ;;
  esac
done

loadConfigFile

if [[ -n $numValidatorsRequested ]]; then
  truncatedNodeList=( "${validatorIpList[@]:0:$numValidatorsRequested}" )
  unset validatorIpList
  validatorIpList=( "${truncatedNodeList[@]}" )
fi

numClients=${#clientIpList[@]}
numClientsRequested=$((numBenchTpsClients + numIdleClients))
if [[ "$numClientsRequested" -eq 0 ]]; then
  numBenchTpsClients=$numClients
  numClientsRequested=$numClients
else
  if [[ "$numClientsRequested" -gt "$numClients" ]]; then
    echo "Error: More clients requested ($numClientsRequested) then available ($numClients)"
    exit 1
  fi
fi

if [[ -n "$maybeWaitForSupermajority" && -n "$maybeWarpSlot" ]]; then
  read -r _ waitSlot <<<"$maybeWaitForSupermajority"
  read -r _ warpSlot <<<"$maybeWarpSlot"
  if [[ $waitSlot -ne $warpSlot ]]; then
    echo "Error: When specifying both --wait-for-supermajority and --warp-slot,"
    echo "they must use the same slot. ($waitSlot != $warpSlot)"
    exit 1
  fi
fi

echo "net.sh: Primordial stakes: $extraPrimordialStakes"
if [[ $extraPrimordialStakes -gt 0 ]]; then
  # Extra primoridial stakes require that all of the validators start at
  # the same time. Force async init and wait for supermajority here.
  waitForNodeInit=false
  if [[ -z "$maybeWaitForSupermajority" ]]; then
    waitSlot=
    if [[ -n "$maybeWarpSlot" ]]; then
      read -r _ waitSlot <<<"$maybeWarpSlot"
    else
      waitSlot=1
    fi
    maybeWaitForSupermajority="--wait-for-supermajority $waitSlot"
  fi
fi

checkPremptibleInstances

case $command in
restart)
  prepareDeploy
  stop
  deploy
  ;;
start)
  prepareDeploy
  deploy
  ;;
prepare)
  prepareDeploy
  ;;
sanity)
  sanity
  ;;
stop)
  stop
  ;;
update)
  deployUpdate
  ;;
upgrade)
  bootstrapValidatorIp="${validatorIpList[0]}"
  prepareDeploy
  deployBootstrapValidator "$bootstrapValidatorIp"
  # (start|stop)Node need refactored to support restarting the bootstrap validator
  ;;
stopnode)
  if [[ -z $nodeAddress ]]; then
    usage "node address (-i) not specified"
    exit 1
  fi
  stopNode "$nodeAddress" true
  ;;
startnode)
  if [[ -z $nodeAddress ]]; then
    usage "node address (-i) not specified"
    exit 1
  fi
  nodeType=
  nodeIndex=
  getNodeType
  startNode "$nodeAddress" $nodeType $nodeIndex
  ;;
startclients)
  startClients
  ;;
logs)
  initLogDir
  fetchRemoteLog() {
    declare ipAddress=$1
    declare log=$2
    echo "--- fetching $log from $ipAddress"
    (
      set -x
      timeout 30s scp "${sshOptions[@]}" \
        "$ipAddress":solana/"$log".log "$netLogDir"/remote-"$log"-"$ipAddress".log
    ) || echo "failed to fetch log"
  }
  fetchRemoteLog "${validatorIpList[0]}" faucet
  for ipAddress in "${validatorIpList[@]}"; do
    fetchRemoteLog "$ipAddress" validator
  done
  for ipAddress in "${clientIpList[@]}"; do
    fetchRemoteLog "$ipAddress" client
  done
  for ipAddress in "${blockstreamerIpList[@]}"; do
    fetchRemoteLog "$ipAddress" validator
  done
  ;;
netem)
  if [[ -n $netemConfigFile ]]; then
    remoteNetemConfigFile="$(basename "$netemConfigFile")"
    if [[ $netemCommand = "add" ]]; then
      for ipAddress in "${validatorIpList[@]}"; do
        "$here"/scp.sh "$netemConfigFile" solana@"$ipAddress":"$SOLANA_HOME"
      done
    fi
    for i in "${!validatorIpList[@]}"; do
      "$here"/ssh.sh solana@"${validatorIpList[$i]}" 'solana/scripts/net-shaper.sh' \
      "$netemCommand" ~solana/solana/"$remoteNetemConfigFile" "${#validatorIpList[@]}" "$i"
    done
  else
    num_nodes=$((${#validatorIpList[@]}*netemPartition/100))
    if [[ $((${#validatorIpList[@]}*netemPartition%100)) -gt 0 ]]; then
      num_nodes=$((num_nodes+1))
    fi
    if [[ "$num_nodes" -gt "${#validatorIpList[@]}" ]]; then
      num_nodes=${#validatorIpList[@]}
    fi

    # Stop netem on all nodes
    for ipAddress in "${validatorIpList[@]}"; do
      "$here"/ssh.sh solana@"$ipAddress" 'solana/scripts/netem.sh delete < solana/netem.cfg || true'
    done

    # Start netem on required nodes
    for ((i=0; i<num_nodes; i++ )); do :
      "$here"/ssh.sh solana@"${validatorIpList[$i]}" "echo $netemConfig > solana/netem.cfg; solana/scripts/netem.sh add \"$netemConfig\""
    done
  fi
  ;;
*)
  echo "Internal error: Unknown command: $command"
  usage
  exit 1
esac
