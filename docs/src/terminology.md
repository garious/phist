---
title: Terminology
---

The following terms are used throughout the documentation.

## account

A record in the Solana ledger that either holds data or is an executable program.

Like an account at a traditional bank, a Solana account may hold funds called [lamports](terminology.md#lamport). Like a file in Linux, it is addressable by a [key](terminology.md#public-key) (public key or pubkey).

## account owner

The public key of the program that owns the account. Only the owning program is capable of modifying the account.

## app

A front-end application that interacts with a Solana cluster.

## bank state

The result of interpreting all programs on the ledger at a given [tick height](terminology.md#tick-height). It includes at least the set of all [accounts](terminology.md#account) holding nonzero [native tokens](terminology.md#native-tokens).

## block

A contiguous set of [entries](terminology.md#entry) on the ledger covered by a [vote](terminology.md#ledger-vote). A [leader](terminology.md#leader) produces at most one block per [slot](terminology.md#slot).

## blockhash

A unique value ([hash](terminology.md#hash)) that identifies a record (block).  Solana computes a blockhash from the last [entry id](terminology.md#entry-id) of the block.

## block height

The number of [blocks](terminology.md#block) beneath the current block. The first block after the [genesis block](terminology.md#genesis-block) has height one.

## bootstrap validator

The server node ([validator](terminology.md#validator)) that produces the genesis (first) [block](terminology.md#block) of a block chain.

## BPF loader

The Solana program that owns and loads (BPF) smart contract programs, allowing the program to interface with the runtime

## CBC block

The smallest encrypted chunk of ledger, an encrypted ledger segment would be made of many CBC blocks. `ledger_segment_size / cbc_block_size` to be exact.

## client

A computer program that accesses the Solana server network [cluster](terminology.md#cluster).

## cluster

A set of [validators](terminology.md#validator) maintaining a single [ledger](terminology.md#ledger).

## confirmation time

The wallclock duration between a [leader](terminology.md#leader) creating a [tick entry](terminology.md#tick) and creating a [confirmed block](terminology.md#confirmed-block).

## confirmed block

A [block](terminology.md#block) that has received a [supermajority](terminology.md#supermajority) of [ledger votes](terminology.md#ledger-vote).

## control plane

A gossip network connecting all [nodes](terminology.md#node) of a [cluster](terminology.md#cluster).

## cooldown period

Some number of [epochs](terminology.md#epoch) after [stake](terminology.md#stake) has been deactivated while it progressively becomes available for withdrawal. During this period, the stake is considered to be "deactivating". More info about: [warmup and cooldown](implemented-proposals/staking-rewards.md#stake-warmup-cooldown-withdrawal)

## credit

See [vote credit](terminology.md#vote-credit).

## cross-program invocation (CPI)

A call from one smart contract program to another. For more information, see [calling between programs](developing/programming-model/calling-between-programs.md).

## data plane

A multicast network used to efficiently validate [entries](terminology.md#entry) and gain consensus.

## drone

An off-chain service that acts as a custodian for a user's private key. It typically serves to validate and sign transactions.

## entry

An entry on the [ledger](terminology.md#ledger) either a [tick](terminology.md#tick) or a [transactions entry](terminology.md#transactions-entry).

## entry id

A preimage resistant [hash](terminology.md#hash) over the final contents of an entry, which acts as the [entry's](terminology.md#entry) globally unique identifier. The hash serves as evidence of:

- The entry being generated after a duration of time
- The specified [transactions](terminology.md#transaction) are those included in the entry
- The entry's position with respect to other entries in [ledger](terminology.md#ledger)

See [proof of history](terminology.md#proof-of-history).

## epoch

The time, i.e. number of [slots](terminology.md#slot), for which a [leader schedule](terminology.md#leader-schedule) is valid.

## fee account

The fee account in the transaction is the account pays for the cost of including the transaction in the ledger. This is the first account in the transaction. This account must be declared as Read-Write (writable) in the transaction since paying for the transaction reduces the account balance.

## finality

When nodes representing 2/3rd of the [stake](terminology.md#stake) have a common [root](terminology.md#root).

## fork

A [ledger](terminology.md#ledger) derived from common entries but then diverged.

## genesis block

The first [block](terminology.md#block) in the chain.

## genesis config

The configuration file that prepares the [ledger](terminology.md#ledger) for the [genesis block](terminology.md#genesis-block).

## hash

A digital fingerprint of a sequence of bytes.

## inflation

An increase in token supply over time used to fund rewards for validation and to fund continued development of Solana.

## inner instruction

See [cross-program invocation](terminology.md#cross-program-invocation).

## instruction

The smallest contiguous unit of execution logic in a [program](terminology.md#program). An instruction specifies which program it is calling, which accounts it wants to read or modify, and additional data that serves as auxiliary input to the program. A [client](terminology.md#client) can include one or multiple instructions in a [transaction](terminology.md#transaction). An instruction may contain one or more [cross-program invocations](terminology.md#cross-program-invocation).

## keypair

A [public key](terminology.md#public-key) and corresponding [private key](terminology.md#private-key) for accessing an account.

## lamport

A fractional [native token](terminology.md#native-token) with the value of 0.000000001 [sol](terminology.md#sol).

## leader

The role of a [validator](terminology.md#validator) when it is appending [entries](terminology.md#entry) to the [ledger](terminology.md#ledger).

## leader schedule

A sequence of [validator](terminology.md#validator) [public keys](terminology.md#public-key) mapped to [slots](terminology.md#slot). The cluster uses the leader schedule to determine which validator is the [leader](terminology.md#leader) at any moment in time.

## ledger

A list of [entries](terminology.md#entry) containing [transactions](terminology.md#transaction) signed by [clients](terminology.md#client).
Conceptually, this can be traced back to the [genesis block](terminology.md#genesis-block), but actual [validators](terminology.md#validator)'s ledger may have only newer [blocks](terminology.md#block) to save storage usage as older ones not needed for validation of future blocks by design.

## ledger vote

A [hash](terminology.md#hash) of the [validator's state](terminology.md#bank-state) at a given [tick height](terminology.md#tick-height). It comprises a [validator's](terminology.md#validator) affirmation that a [block](terminology.md#block) it has received has been verified, as well as a promise not to vote for a conflicting [block](terminology.md#block) \(i.e. [fork](terminology.md#fork)\) for a specific amount of time, the [lockout](terminology.md#lockout) period.

## light client

A type of [client](terminology.md#client) that can verify it's pointing to a valid [cluster](terminology.md#cluster). It performs more ledger verification than a [thin client](terminology.md#thin-client) and less than a [validator](terminology.md#validator).

## loader

A [program](terminology.md#program) with the ability to interpret the binary encoding of other on-chain programs.

## lockout

The duration of time for which a [validator](terminology.md#validator) is unable to [vote](terminology.md#ledger-vote) on another [fork](terminology.md#fork).

## native token

The [token](terminology.md#token) used to track work done by [nodes](terminology.md#node) in a cluster.

## node

A computer participating in a [cluster](terminology.md#cluster).

## node count

The number of [validators](terminology.md#validator) participating in a [cluster](terminology.md#cluster).

## PoH

See [Proof of History](terminology.md#proof-of-history).

## point

A weighted [credit](terminology.md#credit) in a rewards regime. In the [validator](terminology.md#validator) [rewards regime](cluster/stake-delegation-and-rewards.md), the number of points owed to a [stake](terminology.md#stake) during redemption is the product of the [vote credits](terminology.md#vote-credit) earned and the number of lamports staked.

## private key

The private key of a [keypair](terminology.md#keypair).

## program

The code that interprets [instructions](terminology.md#instruction).

## program derived account (PDA)

An account whose owner is a program and thus is not controlled by a private key like other accounts.   For example, associated token accounts are have a separate owner field which is the token program that controls debits to the account.

## program id

The public key of the [account](terminology.md#account) containing a [program](terminology.md#program).

## proof of history (PoH)

A stack of proofs, each which proves that some data existed before the proof was created and that a precise duration of time passed before the previous proof. Like a [VDF](terminology.md#verifiable-delay-function), a Proof of History can be verified in less time than it took to produce.

## public key (pubkey)

The public key of a [keypair](terminology.md#keypair).

## root

A [block](terminology.md#block) or [slot](terminology.md#slot) that has reached maximum [lockout](terminology.md#lockout) on a [validator](terminology.md#validator). The root is the highest block that is an ancestor of all active forks on a validator. All ancestor blocks of a root are also transitively a root. Blocks that are not an ancestor and not a descendant of the root are excluded from consideration for consensus and can be discarded.

## runtime

The component of a [validator](terminology.md#validator) responsible for [program](terminology.md#program) execution.

##Sealevel

Solana's parallel smart contracts run-time.

## shred

A fraction of a [block](terminology.md#block); the smallest unit sent between [validators](terminology.md#validator).

## signature

A 64-byte ed25519 signature of R (32-bytes) and S (32-bytes). With the requirement that R is a packed Edwards point not of small order and S is a scalar in the range of 0 <= S < L.
This requirement ensures no signature malleability. Each transaction must have at least one signature for [fee account](terminology#fee-account).
Thus, the first signature in transaction can be treated as [transacton id](terminology.md#transaction-id)

## skipped slot

A past [slot](terminology.md#slot) that did not produce a [block](terminology.md#block), because the leader was offline or the [fork](terminology.md#fork) containing the slot was abandoned for a better alternative by cluster consensus. A skipped slot will not appear as an ancestor for blocks at subsequent slots, nor increment the [block height](terminology#block-height), nor expire the oldest `recent_blockhash`.

Whether a slot has been skipped can only be determined when it becomes older than the latest [rooted](terminology.md#root) (thus not-skipped) slot.

## slot

The period of time for which each [leader](terminology.md#leader) ingests transactions and produces a [block](terminology.md#block).

Collectively, slots create a logical clock. Slots are ordered sequentially and non-overlapping, comprising roughly equal real-world time as per [PoH](terminology.md#proof-of-history).

## smart contract

A program on a blockchain that can read and modify accounts over which it has control.

## sol

The [native token](terminology.md#native-token) of a Solana [cluster](terminology.md#cluster).

## Solana Program Library (SPL)

A library of smart contracts on Solana such as spl-token that facilitates tasks such as creating and using tokens

## stake

Tokens forfeit to the [cluster](terminology.md#cluster) if malicious [validator](terminology.md#validator) behavior can be proven.

## supermajority

2/3 of a [cluster](terminology.md#cluster).

## sysvar

A system [account](terminology.md#account).  [Sysvars](developing/runtime-facilities/sysvars.md) provide cluster state information such as current tick height, rewards [points](terminology.md#point) values, etc.  Programs can access Sysvars via a Sysvar account (pubkey) or by querying via a syscall.

## thin client

A type of [client](terminology.md#client) that trusts it is communicating with a valid [cluster](terminology.md#cluster).

## tick

A ledger [entry](terminology.md#entry) that estimates wallclock duration.

## tick height

The Nth [tick](terminology.md#tick) in the [ledger](terminology.md#ledger).

## token

A digitally transferable asset.

## tps

[Transactions](terminology.md#transaction) per second.

## transaction

One or more [instructions](terminology.md#instruction) signed by a [client](terminology.md#client) using one or more [keypairs](terminology.md#keypair) and executed atomically with only two possible outcomes: success or failure.

## transaction id

The first [signature](terminology.md#signature) in a [transaction](terminology.md#transaction), which can be used to uniquely identify the transaction across the complete [ledger](terminology.md#ledger).

## transaction confirmations

The number of [confirmed blocks](terminology.md#confirmed-block) since the transaction was accepted onto the [ledger](terminology.md#ledger). A transaction is finalized when its block becomes a [root](terminology.md#root).

## transactions entry

A set of [transactions](terminology.md#transaction) that may be executed in parallel.

## validator

A full participant in a Solana network [cluster](terminology.md#cluster) that produces new [blocks](terminology.md#block).  A validator validates the transactions added to the [ledger](terminology.md#ledger)

## VDF

See [verifiable delay function](terminology.md#verifiable-delay-function).

## verifiable delay function (VDF)

A function that Solana uses to encode the passage of time.   The Solana VDF takes a fixed amount of time to execute and produces a proof that it ran, which can then be verified in less time than it took to produce.

## vote

See [ledger vote](terminology.md#ledger-vote).

## vote credit

A reward tally for [validators](terminology.md#validator). A vote credit is awarded to a validator in its vote account when the validator reaches a [root](terminology.md#root).

## wallet

A collection of [keypairs](terminology.md#keypair) that allows users to manage their funds.

## warmup period

Some number of [epochs](terminology.md#epoch) after [stake](terminology.md#stake) has been delegated while it progressively becomes effective. During this period, the stake is considered to be "activating". More info about: [warmup and cooldown](cluster/stake-delegation-and-rewards.md#stake-warmup-cooldown-withdrawal)
