# Traced transaction construction via RPC's `simulateTransaction`

## Problem

With more composition comes in the Solana's program ecosystem, it will be harder
and harder for the higher-level consuming program to correctly construct
lower-level program's instructions by precisely specifying each and every
accessed accounts with the `read_only` or `read_write` flag.

Also, there will be no way to signal the need of additional accounts for the
arbitrarily customized behavior from the lower-level program to the higher-level
one under the standardized instruction API like spl-token, even though we're
about to explore the flexibility of allowing customzed programs conforming to a
standard like the itoken.

These preceding runtime limitations are applicable both to on-chain (instruction
construction for CPI) or off-chain (transaction construction by end-user
interfaces) boundaries likewise.

Lastly, it's practically impossible to change those accessed account addresses
once deployed while retaining compatibility, given the fact the consumers are
expected to hard-code the account addresses for consumption, even if the
deployed programs can be now upgradeable. This severely hampers ongoing
consumed-program's implementation refinement in the areas of data migration and
improved concurrency, by preventing transparent account switching and
sharding with vectorized accounts, respectively.

## Proposed Solution

Extend the `simulateTransaction` RPC method to return more information like
additional account addresses to read and/or write, collected from traced
transaction execution in the simulation.

Then, client signs the final transaction after appending these additional
account addresses and submits it to the cluster via `sendTransaction`.

For that end, introduce a new syscall to trace account accesses, so that
programs can dynamically hint their _internal_ or _derivable_ account addresses
implicitly, attaning the classic implementation-detail abstraction in the
context Soalna's program ecosystem.

In general, wallets should be able to manage these transaction finalization,
agnostic to individual defi programs. Also, this extra simulation step results
in no-op and harmless if all of the executed programs aren't aware of this new
functionality.

## Traced syscalls

The runtime exposes new syscall to provide new account access mechanism which
can be traced:

```rust
fn access_as_unsigned(address: &Pubkey, flags = enum { READ, WRITE}): 
  Result<AccountInfo, Error> { ... }
// NOTE: accessing to signed account in this way will strip the is_signer bit.
// hence that's reflected in the name
```

This syscall behaves differently depending on the BPF execution mode (simulated
or executed):

simulated: load any account at the specified address from AccountsDb (if any)
and retain any written data to those accounts in the scope of single transaction
simulation.

executed: return any account if the specified address is contained in the list of
tx's account keys.

While MessageProcessor's `verify_accounts` is enforced to these accessed
accounts like normally-accessed accounts, this syscall can bypass the CPI
boundaries by always accessing to the outermost execution environment (the
whole AccountsDB account set while simulated, all of account keys while
executed). This bypass semantic is needed to work with existing CPI usage.

Then, otherwise noted, the returned `AccountInfo` should behave in the exactly
same way both in the simulated and executed mode. This is desired for
determisnism while developing and is mandated for security to lightly protect
users from bad programs pretending to be harmless only in simulation.


### example: the associated token account program (demonstrating the problem #1)

```patch
diff --git a/associated-token-account/program/src/processor.rs b/associated-token-account/program/src/processor.rs
index 98eb08b..fa8ae50 100644
--- a/associated-token-account/program/src/processor.rs
+++ b/associated-token-account/program/src/processor.rs
@@ -22,13 +22,12 @@ pub fn process_instruction(
     let account_info_iter = &mut accounts.iter();
 
     let funder_info = next_account_info(account_info_iter)?;
-    let associated_token_account_info = next_account_info(account_info_iter)?;
     let wallet_account_info = next_account_info(account_info_iter)?;
     let spl_token_mint_info = next_account_info(account_info_iter)?;
-    let system_program_info = next_account_info(account_info_iter)?;
-    let spl_token_program_info = next_account_info(account_info_iter)?;
+    let system_program_info = access_as_unsigned(funder_info.owner(), READ)?;
+    let spl_token_program_info = access_as_unsigned(spl_token_mint_info.owner(), READ)?;
     let spl_token_program_id = spl_token_program_info.key;
-    let rent_sysvar_info = next_account_info(account_info_iter)?;
+    let rent_sysvar_info = access_as_unsigned(sysvars::rent::id(), READ)?;
 
     let (associated_token_address, bump_seed) = get_associated_token_address_and_bump_seed_internal(
         &wallet_account_info.key,
@@ -36,10 +35,7 @@ pub fn process_instruction(
         program_id,
         &spl_token_program_id,
     );
-    if associated_token_address != *associated_token_account_info.key {
-        msg!("Error: Associated address does not match seed derivation");
-        return Err(ProgramError::InvalidSeeds);
-    }
+    let associated_token_account_info = access_as_unsigned(associated_token_address, READ_WRITE)?;
 
     let associated_token_account_signer_seeds: &[&[_]] = &[
         &wallet_account_info.key.to_bytes(),
```

This reduces the number of required accounts for CPI and clients from 7 to 3, reducing by 4.
(Obviously, we can't actually deploy these changes because this will break
compatibility. This is only for illustration of effective applicability to
moderately-complex program construct.)

### example: limited number of NFT transfers per day shared across program:

```rust
pub fn process_transfer(                                              
    program_id: &Pubkey,     
    accounts: &[AccountInfo],                                                                   
    amount: u64,             
    expected_decimals: Option<u8>,    
) -> ProgramResult {
    let counter_address = Pubkey::find_program_address(
        &[  
            &program_id.to_bytes(),
            "counter",
        ],
        program_id,
    );
   let transfer_counter = access_as_unsigned(counter_address, READ_WRITE)?;
   let clock_sysvar = access_as_unsigned(sysvars::clock::id(), READ_WRITE)?;
   let today = yyyymmdd(clock_sysvar);
   if today > transfer_counter.yyyymmdd {
      transfer_counter.yymmdd = today;
      transfer_counter.count = 0;
   } else if transfer_counter > 10 {
      return Err("daily quote of transfer is reached");
   }

   transfer_counter.count += 1;
   ...
}
```

### example: sharding


```rust
pub fn process_place_order(
  program_id: &Pubkey,     
  market_id: &Pubkey,
  order: &Pubkey,
) {
  let sharded_event_queue_address = let counter_address = Pubkey::find_program_address(
      &[  
          &market_id.to_bytes(),
          &[ourder_to_bytes()[0] & 0xf0],
      ],
      program_id,
  );
  let event_queue = access_as_unsigned(sharded_event_queue_address, READ_WRITE);  

  // crank tx will merge all event queues later
}
```

### example: instruction versioning across redeploy

```rust
pub fn check_instruction_version_and_load_data() {
   // this is newly added after the redeploy
   if access_as_unsigned(declare!("OldVersion111111111111111111111111111111111111"), READ).is_ok() {
     // it seems that this transaction is construction by simulation with pre-redeploy program executable.
     return Err("we changed internal data loading implementation. to avoid unexpected malfunction retry");
   }

   // this is changed from OldVersion1111
   access_as_unsigned(declare!("NewVersion111111111111111111111111111111111111"), READ)?

   access_as_unsigned(...);
}
```


## RPC changes

`pre_balances` and `post_balances` needs to be updated?

### sample return from `simulateTransaction` (or define `traceTransaction` for the compatibility??):

```yml
# these somewhat verbose formatting is for future expansion
transaction:
  instructions
    accesses:
      Bm9UhRBysjeT35ukzriheHKr76yiM1fPyEBMMw2yT9v1: {read: true, write: false}      
      J8H1Gsvnm2CY5oVa1zw1LHHbfvtone8ZGsVBzjkkPbpy: {read: true, write: true}      
    instructions:
      accesses: []
      instruction_type: spl_token_transfer
      instructions: [...]
  accesses: [...]
  pre_balances/pre_token_balances
  post_balances/post_token_balances
  token_token_approvals: ???
```



also, page-indexed account can be incorporated to find compact combination of existing account indexes to match the required account list

## downside

race condition around program deploy (= possible account loading change) => just retry also seek to the ix versioning for perfection

(traced program just load dummy addresses for tx versionsing for completeness)

## careful consideration

prevent program  detect to be simulated or really-executed


## Other Proposals / alternative implementation

### customized entrypoint?

breaks compatibility?

### Run these before replaying stage on validator

unavoidable latency.  (and scheduling difficulty).
The proposed implementation doesn't have such a downside

## Future possible expansions

- Wallet account balance delta check: append VerifyTokenBalances ix at last and pass the expected balances of spl-tokens or hash of it (also with delegation status).
- Browser-side bpf program simualtion for server-less architecture with online wasm translation
