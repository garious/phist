---
title: Starting a Validator
---

## Configure Solana CLI

The solana cli includes `get` and `set` configuration commands to automatically
set the `--url` argument for cli commands. For example:

```bash
solana config set --url http://api.devnet.solana.com
```

While this section demonstrates how to connect to the Devnet cluster, the steps
are similar for the other [Solana Clusters](../clusters.md).

## Confirm The Cluster Is Reachable

Before attaching a validator node, sanity check that the cluster is accessible
to your machine by fetching the transaction count:

```bash
solana transaction-count
```

View the [metrics dashboard](https://metrics.solana.com:3000/d/monitor/cluster-telemetry) for more
detail on cluster activity.

## Confirm your Installation

Try running following command to join the gossip network and view all the other
nodes in the cluster:

```bash
solana-gossip spy --entrypoint entrypoint.devnet.solana.com:8001
# Press ^C to exit
```

## Enabling CUDA

If your machine has a GPU with CUDA installed \(Linux-only currently\), include
the `--cuda` argument to `solana-validator`.

When your validator is started look for the following log message to indicate
that CUDA is enabled: `"[<timestamp> solana::validator] CUDA is enabled"`

## System Tuning

### Linux

#### Automatic

The solana repo includes a daemon to adjust system settings to optimize performance
(namely by increasing the OS UDP buffer and file mapping limits).

The daemon (`solana-sys-tuner`) is included in the solana binary release. Restart
it, _before_ restarting your validator, after each software upgrade to ensure that
the latest recommended settings are applied.

To run it:

```bash
sudo $(command -v solana-sys-tuner) --user $(whoami) > sys-tuner.log 2>&1 &
```

#### Manual

If you would prefer to manage system settings on your own, you may do so with
the following commands.

##### **Increase UDP buffers**

```bash
sudo bash -c "cat >/etc/sysctl.d/20-solana-udp-buffers.conf <<EOF
# Increase UDP buffer size
net.core.rmem_default = 134217728
net.core.rmem_max = 134217728
net.core.wmem_default = 134217728
net.core.wmem_max = 134217728
EOF"
```

```bash
sudo sysctl -p /etc/sysctl.d/20-solana-udp-buffers.conf
```

##### **Increased memory mapped files limit**

```bash
sudo bash -c "cat >/etc/sysctl.d/20-solana-mmaps.conf <<EOF
# Increase memory mapped files limit
vm.max_map_count = 700000
EOF"
```

```bash
sudo sysctl -p /etc/sysctl.d/20-solana-mmaps.conf
```

Add

```
LimitNOFILE=700000
```

to the `[Service]` section of your systemd service file, if you use one,
otherwise add

```
DefaultLimitNOFILE=700000
```

to the `[Manager]` section of `/etc/systemd/system.conf`.

```bash
sudo systemctl daemon-reload
```

```bash
sudo bash -c "cat >/etc/security/limits.d/90-solana-nofiles.conf <<EOF
# Increase process file descriptor count limit
* - nofile 700000
EOF"
```

```bash
### Close all open sessions (log out then, in again) ###
```

## Generate identity

Create an identity keypair for your validator by running:

```bash
solana-keygen new -o ~/validator-keypair.json
```

The identity public key can now be viewed by running:

```bash
solana-keygen pubkey ~/validator-keypair.json
```

> Note: The "validator-keypair.json” file is also your \(ed25519\) private key.

### Paper Wallet identity

You can create a paper wallet for your identity file instead of writing the
keypair file to disk with:

```bash
solana-keygen new --no-outfile
```

The corresponding identity public key can now be viewed by running:

```bash
solana-keygen pubkey ASK
```

and then entering your seed phrase.

See [Paper Wallet Usage](../wallet-guide/paper-wallet.md) for more info.

---

### Vanity Keypair

You can generate a custom vanity keypair using solana-keygen. For instance:

```bash
solana-keygen grind --starts-with e1v1s:1
```

You may request that the generated vanity keypair be expressed as a seed phrase
which allows recovery of the keypair from the seed phrase and an optionally
supplied passphrase (note that this is significantly slower than grinding without
a mnemonic):

```bash
solana-keygen grind --use-mnemonic --starts-with e1v1s:1
```

Depending on the string requested, it may take days to find a match...

---

Your validator identity keypair uniquely identifies your validator within the
network. **It is crucial to back-up this information.**

If you don’t back up this information, you WILL NOT BE ABLE TO RECOVER YOUR
VALIDATOR if you lose access to it. If this happens, YOU WILL LOSE YOUR
ALLOCATION OF SOL TOO.

To back-up your validator identify keypair, **back-up your
"validator-keypair.json” file or your seed phrase to a secure location.**

## More Solana CLI Configuration

Now that you have a keypair, set the solana configuration to use your validator
keypair for all following commands:

```bash
solana config set --keypair ~/validator-keypair.json
```

You should see the following output:

```text
Config File: /home/solana/.config/solana/cli/config.yml
RPC URL: http://api.devnet.solana.com
WebSocket URL: ws://api.devnet.solana.com/ (computed)
Keypair Path: /home/solana/validator-keypair.json
Commitment: confirmed
```

## Airdrop & Check Validator Balance

Airdrop yourself some SOL to get started:

```bash
solana airdrop 1
```

Note that airdrops are only available on Devnet and Testnet. Both are limited
to 1 SOL per request.

To view your current balance:

```text
solana balance
```

Or to see in finer detail:

```text
solana balance --lamports
```

Read more about the [difference between SOL and lamports here](../introduction.md#what-are-sols).

## Create Vote Account

If you haven’t already done so, create a vote-account keypair and create the
vote account on the network. If you have completed this step, you should see the
“vote-account-keypair.json” in your Solana runtime directory:

```bash
solana-keygen new -o ~/vote-account-keypair.json
```

The following command can be used to create your vote account on the blockchain
with all the default options:

```bash
solana create-vote-account ~/vote-account-keypair.json ~/validator-keypair.json
```

Read more about [creating and managing a vote account](vote-accounts.md).

## Trusted validators

If you know and trust other validator nodes, you can specify this on the command line with the `--trusted-validator <PUBKEY>`
argument to `solana-validator`. You can specify multiple ones by repeating the argument `--trusted-validator <PUBKEY1> --trusted-validator <PUBKEY2>`.
This has two effects, one is when the validator is booting with `--no-untrusted-rpc`, it will only ask that set of
trusted nodes for downloading genesis and snapshot data. Another is that in combination with the `--halt-on-trusted-validator-hash-mismatch` option,
it will monitor the merkle root hash of the entire accounts state of other trusted nodes on gossip and if the hashes produce any mismatch,
the validator will halt the node to prevent the validator from voting or processing potentially incorrect state values. At the moment, the slot that
the validator publishes the hash on is tied to the snapshot interval. For the feature to be effective, all validators in the trusted
set should be set to the same snapshot interval value or multiples of the same.

It is highly recommended you use these options to prevent malicious snapshot state download or
account state divergence.

## Connect Your Validator

Connect to the cluster by running:

```bash
solana-validator \
  --identity ~/validator-keypair.json \
  --vote-account ~/vote-account-keypair.json \
  --rpc-port 8899 \
  --entrypoint entrypoint.devnet.solana.com:8001 \
  --limit-ledger-size \
  --log ~/solana-validator.log
```

To force validator logging to the console add a `--log -` argument, otherwise
the validator will automatically log to a file.

The ledger will be placed in the `ledger/` directory by default, use the
`--ledger` argument to specify a different location.

> Note: You can use a
> [paper wallet seed phrase](../wallet-guide/paper-wallet.md)
> for your `--identity` and/or
> `--authorized-voter` keypairs. To use these, pass the respective argument as
> `solana-validator --identity ASK ... --authorized-voter ASK ...`
> and you will be prompted to enter your seed phrases and optional passphrase.

Confirm your validator connected to the network by opening a new terminal and
running:

```bash
solana-gossip spy --entrypoint entrypoint.devnet.solana.com:8001
```

If your validator is connected, its public key and IP address will appear in the list.

### Controlling local network port allocation

By default the validator will dynamically select available network ports in the
8000-10000 range, and may be overridden with `--dynamic-port-range`. For
example, `solana-validator --dynamic-port-range 11000-11010 ...` will restrict
the validator to ports 11000-11010.

### Limiting ledger size to conserve disk space

The `--limit-ledger-size` parameter allows you to specify how many ledger
[shreds](../terminology.md#shred) your node retains on disk. If you do not
include this parameter, the validator will keep the entire ledger until it runs
out of disk space.

The default value attempts to keep the ledger disk usage under 500GB. More or
less disk usage may be requested by adding an argument to `--limit-ledger-size`
if desired. Check `solana-validator --help` for the default limit value used by
`--limit-ledger-size`. More information about
selecting a custom limit value is [available
here](https://github.com/solana-labs/solana/blob/583cec922b6107e0f85c7e14cb5e642bc7dfb340/core/src/ledger_cleanup_service.rs#L15-L26).

### Systemd Unit

Running the validator as a systemd unit is one easy way to manage running in the
background.

Assuming you have a user called `sol` on your machine, create the file `/etc/systemd/system/sol.service` with
the following:

```
[Unit]
Description=Solana Validator
After=network.target
Wants=solana-sys-tuner.service
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
User=sol
LimitNOFILE=700000
LogRateLimitIntervalSec=0
Environment="PATH=/bin:/usr/bin:/home/sol/.local/share/solana/install/active_release/bin"
ExecStart=/home/sol/bin/validator.sh

[Install]
WantedBy=multi-user.target
```

Now create `/home/sol/bin/validator.sh` to include the desired
`solana-validator` command-line. Ensure that the 'exec' command is used to
start the validator process (i.e. "exec solana-validator ...").  This is
important because without it, logrotate will end up killing the validator
every time the logs are rotated.

Ensure that running `/home/sol/bin/validator.sh` manually starts
the validator as expected. Don't forget to mark it executable with `chmod +x /home/sol/bin/validator.sh`

Start the service with:

```bash
$ sudo systemctl enable --now sol
```

### Logging

#### Log output tuning

The messages that a validator emits to the log can be controlled by the `RUST_LOG`
environment variable. Details can by found in the [documentation](https://docs.rs/env_logger/latest/env_logger/#enabling-logging)
for the `env_logger` Rust crate.

Note that if logging output is reduced, this may make it difficult to debug issues
encountered later. Should support be sought from the team, any changes will need
to be reverted and the issue reproduced before help can be provided.

#### Log rotation

The validator log file, as specified by `--log ~/solana-validator.log`, can get
very large over time and it's recommended that log rotation be configured.

The validator will re-open its when it receives the `USR1` signal, which is the
basic primitive that enables log rotation.

If the validator is being started by a wrapper shell script, it is important to
launch the process with `exec` (`exec solana-validator ...`) when using logrotate.
This will prevent the `USR1` signal from being sent to the script's process
instead of the validator's, which will kill them both.

#### Using logrotate

An example setup for the `logrotate`, which assumes that the validator is
running as a systemd service called `sol.service` and writes a log file at
/home/sol/solana-validator.log:

```bash
# Setup log rotation

cat > logrotate.sol <<EOF
/home/sol/solana-validator.log {
  rotate 7
  daily
  missingok
  postrotate
    systemctl kill -s USR1 sol.service
  endscript
}
EOF
sudo cp logrotate.sol /etc/logrotate.d/sol
systemctl restart logrotate.service
```

As mentioned earlier, be sure that if you use logrotate, any script you create
which starts the solana validator process uses "exec" to do so (example: "exec
solana-validator ..."); otherwise, when logrotate sends its signal to the
validator, the enclosing script will die and take the validator process with
it.

### Disable port checks to speed up restarts

Once your validator is operating normally, you can reduce the time it takes to
restart your validator by adding the `--no-port-check` flag to your
`solana-validator` command-line.

### Using a ramdisk with spill-over into swap for the accounts database to reduce SSD wear

If your machine has plenty of RAM, a tmpfs ramdisk
([tmpfs](https://man7.org/linux/man-pages/man5/tmpfs.5.html)) may be used to hold
the accounts database

When using tmpfs it's essential to also configure swap on your machine as well to
avoid running out of tmpfs space periodically.

A 300GB tmpfs partition is recommended, with an accompanying 250GB swap
partition.

Example configuration:

1. `sudo mkdir /mnt/solana-accounts`
2. Add a 300GB tmpfs parition by adding a new line containing `tmpfs /mnt/solana-accounts tmpfs rw,size=300G,user=sol 0 0` to `/etc/fstab`
   (assuming your validator is running under the user "sol"). **CAREFUL: If you
   incorrectly edit /etc/fstab your machine may no longer boot**
3. Create at least 250GB of swap space

- Choose a device to use in place of `SWAPDEV` for the remainder of these instructions.
  Ideally select a free disk partition of 250GB or greater on a fast disk. If one is not
  available, create a swap file with `sudo dd if=/dev/zero of=/swapfile bs=1MiB count=250KiB`,
  set its permissions with `sudo chmod 0600 /swapfile` and use `/swapfile` as `SWAPDEV` for
  the remainder of these instructions
- Format the device for usage as swap with `sudo mkswap SWAPDEV`

4. Add the swap file to `/etc/fstab` with a new line containing `SWAPDEV swap swap defaults 0 0`
5. Enable swap with `sudo swapon -a` and mount the tmpfs with `sudo mount /mnt/solana-accounts/`
6. Confirm swap is active with `free -g` and the tmpfs is mounted with `mount`

Now add the `--accounts /mnt/solana-accounts` argument to your `solana-validator`
command-line arguments and restart the validator.

### Account indexing

As the number of populated accounts on the cluster grows, account-data RPC
requests that scan the entire account set -- like
[`getProgramAccounts`](developing/clients/jsonrpc-api.md#getprogramaccounts) and
[SPL-token-specific requests](developing/clients/jsonrpc-api.md#gettokenaccountsbydelegate) --
may perform poorly. If your validator needs to support any of these requests,
you can use the `--account-index` parameter to activate one or more in-memory
account indexes that significantly improve RPC performance by indexing accounts
by the key field. Currently supports the following parameter values:

- `program-id`: each account indexed by its owning program; used by [`getProgramAccounts`](developing/clients/jsonrpc-api.md#getprogramaccounts)
- `spl-token-mint`: each SPL token account indexed by its token Mint; used by [getTokenAccountsByDelegate](developing/clients/jsonrpc-api.md#gettokenaccountsbydelegate), and [getTokenLargestAccounts](developing/clients/jsonrpc-api.md#gettokenlargestaccounts)
- `spl-token-owner`: each SPL token account indexed by the token-owner address; used by [getTokenAccountsByOwner](developing/clients/jsonrpc-api.md#gettokenaccountsbyowner), and [`getProgramAccounts`](developing/clients/jsonrpc-api.md#getprogramaccounts) requests that include an spl-token-owner filter.
