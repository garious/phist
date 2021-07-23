import bs58 from 'bs58';
import {Buffer} from 'buffer';
import fetch, {Response} from 'node-fetch';
import {
  type as pick,
  number,
  string,
  array,
  boolean,
  literal,
  record,
  union,
  optional,
  nullable,
  coerce,
  instance,
  create,
  tuple,
  unknown,
  any,
} from 'superstruct';
import type {Struct} from 'superstruct';
import {Client as RpcWebSocketClient} from 'rpc-websockets';
import RpcClient from 'jayson/lib/client/browser';
import {IWSRequestParams} from 'rpc-websockets/dist/lib/client';

import {AgentManager} from './agent-manager';
import {EpochSchedule} from './epoch-schedule';
import {SendTransactionError} from './errors';
import {NonceAccount} from './nonce-account';
import {PublicKey} from './publickey';
import {Signer} from './keypair';
import {MS_PER_SLOT} from './timing';
import {Transaction} from './transaction';
import {Message} from './message';
import assert from './util/assert';
import {sleep} from './util/sleep';
import {promiseTimeout} from './util/promise-timeout';
import {toBuffer} from './util/to-buffer';
import {makeWebsocketUrl} from './util/url';
import type {Blockhash} from './blockhash';
import type {FeeCalculator} from './fee-calculator';
import type {TransactionSignature} from './transaction';
import type {CompiledInstruction} from './message';

const PublicKeyFromString = coerce(
  instance(PublicKey),
  string(),
  value => new PublicKey(value),
);

const RawAccountDataResult = tuple([string(), literal('base64')]);

const BufferFromRawAccountData = coerce(
  instance(Buffer),
  RawAccountDataResult,
  value => Buffer.from(value[0], 'base64'),
);

/**
 * Attempt to use a recent blockhash for up to 30 seconds
 * @internal
 */
export const BLOCKHASH_CACHE_TIMEOUT_MS = 30 * 1000;

type RpcRequest = (methodName: string, args: Array<any>) => any;

type RpcBatchRequest = (requests: RpcParams[]) => any;

/**
 * @internal
 */
export type RpcParams = {
  methodName: string;
  args: Array<any>;
};

export type TokenAccountsFilter =
  | {
      mint: PublicKey;
    }
  | {
      programId: PublicKey;
    };

/**
 * Extra contextual information for RPC responses
 */
export type Context = {
  slot: number;
};

/**
 * Options for sending transactions
 */
export type SendOptions = {
  /** disable transaction verification step */
  skipPreflight?: boolean;
  /** preflight commitment level */
  preflightCommitment?: Commitment;
};

/**
 * Options for confirming transactions
 */
export type ConfirmOptions = {
  /** disable transaction verification step */
  skipPreflight?: boolean;
  /** desired commitment level */
  commitment?: Commitment;
  /** preflight commitment level */
  preflightCommitment?: Commitment;
};

/**
 * Options for getConfirmedSignaturesForAddress2
 */
export type ConfirmedSignaturesForAddress2Options = {
  /**
   * Start searching backwards from this transaction signature.
   * @remark If not provided the search starts from the highest max confirmed block.
   */
  before?: TransactionSignature;
  /** Search until this transaction signature is reached, if found before `limit`. */
  until?: TransactionSignature;
  /** Maximum transaction signatures to return (between 1 and 1,000, default: 1,000). */
  limit?: number;
};

/**
 * Options for getSignaturesForAddress
 */
export type SignaturesForAddressOptions = {
  /**
   * Start searching backwards from this transaction signature.
   * @remark If not provided the search starts from the highest max confirmed block.
   */
  before?: TransactionSignature;
  /** Search until this transaction signature is reached, if found before `limit`. */
  until?: TransactionSignature;
  /** Maximum transaction signatures to return (between 1 and 1,000, default: 1,000). */
  limit?: number;
};

/**
 * RPC Response with extra contextual information
 */
export type RpcResponseAndContext<T> = {
  /** response context */
  context: Context;
  /** response value */
  value: T;
};

/**
 * @internal
 */
function createRpcResult<T, U>(result: Struct<T, U>) {
  return union([
    pick({
      jsonrpc: literal('2.0'),
      id: string(),
      result,
    }),
    pick({
      jsonrpc: literal('2.0'),
      id: string(),
      error: pick({
        code: unknown(),
        message: string(),
        data: optional(any()),
      }),
    }),
  ]);
}

const UnknownRpcResult = createRpcResult(unknown());

/**
 * @internal
 */
function jsonRpcResult<T, U>(schema: Struct<T, U>) {
  return coerce(createRpcResult(schema), UnknownRpcResult, value => {
    if ('error' in value) {
      return value;
    } else {
      return {
        ...value,
        result: create(value.result, schema),
      };
    }
  });
}

/**
 * @internal
 */
function jsonRpcResultAndContext<T, U>(value: Struct<T, U>) {
  return jsonRpcResult(
    pick({
      context: pick({
        slot: number(),
      }),
      value,
    }),
  );
}

/**
 * @internal
 */
function notificationResultAndContext<T, U>(value: Struct<T, U>) {
  return pick({
    context: pick({
      slot: number(),
    }),
    value,
  });
}

/**
 * The level of commitment desired when querying state
 * <pre>
 *   'processed': Query the most recent block which has reached 1 confirmation by the connected node
 *   'confirmed': Query the most recent block which has reached 1 confirmation by the cluster
 *   'finalized': Query the most recent block which has been finalized by the cluster
 * </pre>
 */
export type Commitment =
  | 'processed'
  | 'confirmed'
  | 'finalized'
  | 'recent' // Deprecated as of v1.5.5
  | 'single' // Deprecated as of v1.5.5
  | 'singleGossip' // Deprecated as of v1.5.5
  | 'root' // Deprecated as of v1.5.5
  | 'max'; // Deprecated as of v1.5.5

/**
 * A subset of Commitment levels, which are at least optimistically confirmed
 * <pre>
 *   'confirmed': Query the most recent block which has reached 1 confirmation by the cluster
 *   'finalized': Query the most recent block which has been finalized by the cluster
 * </pre>
 */
export type Finality = 'confirmed' | 'finalized';

/**
 * Filter for largest accounts query
 * <pre>
 *   'circulating':    Return the largest accounts that are part of the circulating supply
 *   'nonCirculating': Return the largest accounts that are not part of the circulating supply
 * </pre>
 */
export type LargestAccountsFilter = 'circulating' | 'nonCirculating';

/**
 * Configuration object for changing `getLargestAccounts` query behavior
 */
export type GetLargestAccountsConfig = {
  /** The level of commitment desired */
  commitment?: Commitment;
  /** Filter largest accounts by whether they are part of the circulating supply */
  filter?: LargestAccountsFilter;
};

/**
 * Configuration object for changing query behavior
 */
export type SignatureStatusConfig = {
  /** enable searching status history, not needed for recent transactions */
  searchTransactionHistory: boolean;
};

/**
 * Information describing a cluster node
 */
export type ContactInfo = {
  /** Identity public key of the node */
  pubkey: string;
  /** Gossip network address for the node */
  gossip: string | null;
  /** TPU network address for the node (null if not available) */
  tpu: string | null;
  /** JSON RPC network address for the node (null if not available) */
  rpc: string | null;
  /** Software version of the node (null if not available) */
  version: string | null;
};

/**
 * Information describing a vote account
 */
export type VoteAccountInfo = {
  /** Public key of the vote account */
  votePubkey: string;
  /** Identity public key of the node voting with this account */
  nodePubkey: string;
  /** The stake, in lamports, delegated to this vote account and activated */
  activatedStake: number;
  /** Whether the vote account is staked for this epoch */
  epochVoteAccount: boolean;
  /** Recent epoch voting credit history for this voter */
  epochCredits: Array<[number, number, number]>;
  /** A percentage (0-100) of rewards payout owed to the voter */
  commission: number;
  /** Most recent slot voted on by this vote account */
  lastVote: number;
};

/**
 * A collection of cluster vote accounts
 */
export type VoteAccountStatus = {
  /** Active vote accounts */
  current: Array<VoteAccountInfo>;
  /** Inactive vote accounts */
  delinquent: Array<VoteAccountInfo>;
};

/**
 * Network Inflation
 * (see https://docs.solana.com/implemented-proposals/ed_overview)
 */
export type InflationGovernor = {
  foundation: number;
  foundationTerm: number;
  initial: number;
  taper: number;
  terminal: number;
};

const GetInflationGovernorResult = pick({
  foundation: number(),
  foundationTerm: number(),
  initial: number(),
  taper: number(),
  terminal: number(),
});

/**
 * The inflation reward for an epoch
 */
export type InflationReward = {
  /** epoch for which the reward occurs */
  epoch: number;
  /** the slot in which the rewards are effective */
  effectiveSlot: number;
  /** reward amount in lamports */
  amount: number;
  /** post balance of the account in lamports */
  postBalance: number;
};

/**
 * Expected JSON RPC response for the "getInflationReward" message
 */
const GetInflationRewardResult = jsonRpcResult(
  array(
    nullable(
      pick({
        epoch: number(),
        effectiveSlot: number(),
        amount: number(),
        postBalance: number(),
      }),
    ),
  ),
);

/**
 * Information about the current epoch
 */
export type EpochInfo = {
  epoch: number;
  slotIndex: number;
  slotsInEpoch: number;
  absoluteSlot: number;
  blockHeight?: number;
  transactionCount?: number;
};

const GetEpochInfoResult = pick({
  epoch: number(),
  slotIndex: number(),
  slotsInEpoch: number(),
  absoluteSlot: number(),
  blockHeight: optional(number()),
  transactionCount: optional(number()),
});

const GetEpochScheduleResult = pick({
  slotsPerEpoch: number(),
  leaderScheduleSlotOffset: number(),
  warmup: boolean(),
  firstNormalEpoch: number(),
  firstNormalSlot: number(),
});

/**
 * Leader schedule
 * (see https://docs.solana.com/terminology#leader-schedule)
 */
export type LeaderSchedule = {
  [address: string]: number[];
};

const GetLeaderScheduleResult = record(string(), array(number()));

/**
 * Transaction error or null
 */
const TransactionErrorResult = nullable(union([pick({}), string()]));

/**
 * Signature status for a transaction
 */
const SignatureStatusResult = pick({
  err: TransactionErrorResult,
});

/**
 * Transaction signature received notification
 */
const SignatureReceivedResult = literal('receivedSignature');

/**
 * Version info for a node
 */
export type Version = {
  /** Version of solana-core */
  'solana-core': string;
  'feature-set'?: number;
};

const VersionResult = pick({
  'solana-core': string(),
  'feature-set': optional(number()),
});

export type SimulatedTransactionResponse = {
  err: TransactionError | string | null;
  logs: Array<string> | null;
};

const SimulatedTransactionResponseStruct = jsonRpcResultAndContext(
  pick({
    err: nullable(union([pick({}), string()])),
    logs: nullable(array(string())),
  }),
);

export type ParsedInnerInstruction = {
  index: number;
  instructions: (ParsedInstruction | PartiallyDecodedInstruction)[];
};

export type TokenBalance = {
  accountIndex: number;
  mint: string;
  uiTokenAmount: TokenAmount;
};

/**
 * Metadata for a parsed confirmed transaction on the ledger
 */
export type ParsedConfirmedTransactionMeta = {
  /** The fee charged for processing the transaction */
  fee: number;
  /** An array of cross program invoked parsed instructions */
  innerInstructions?: ParsedInnerInstruction[] | null;
  /** The balances of the transaction accounts before processing */
  preBalances: Array<number>;
  /** The balances of the transaction accounts after processing */
  postBalances: Array<number>;
  /** An array of program log messages emitted during a transaction */
  logMessages?: Array<string> | null;
  /** The token balances of the transaction accounts before processing */
  preTokenBalances?: Array<TokenBalance> | null;
  /** The token balances of the transaction accounts after processing */
  postTokenBalances?: Array<TokenBalance> | null;
  /** The error result of transaction processing */
  err: TransactionError | null;
};

export type CompiledInnerInstruction = {
  index: number;
  instructions: CompiledInstruction[];
};

/**
 * Metadata for a confirmed transaction on the ledger
 */
export type ConfirmedTransactionMeta = {
  /** The fee charged for processing the transaction */
  fee: number;
  /** An array of cross program invoked instructions */
  innerInstructions?: CompiledInnerInstruction[] | null;
  /** The balances of the transaction accounts before processing */
  preBalances: Array<number>;
  /** The balances of the transaction accounts after processing */
  postBalances: Array<number>;
  /** An array of program log messages emitted during a transaction */
  logMessages?: Array<string> | null;
  /** The token balances of the transaction accounts before processing */
  preTokenBalances?: Array<TokenBalance> | null;
  /** The token balances of the transaction accounts after processing */
  postTokenBalances?: Array<TokenBalance> | null;
  /** The error result of transaction processing */
  err: TransactionError | null;
};

/**
 * A processed transaction from the RPC API
 */
export type TransactionResponse = {
  /** The slot during which the transaction was processed */
  slot: number;
  /** The transaction */
  transaction: {
    /** The transaction message */
    message: Message;
    /** The transaction signatures */
    signatures: string[];
  };
  /** Metadata produced from the transaction */
  meta: ConfirmedTransactionMeta | null;
  /** The unix timestamp of when the transaction was processed */
  blockTime?: number | null;
};

/**
 * A confirmed transaction on the ledger
 */
export type ConfirmedTransaction = {
  /** The slot during which the transaction was processed */
  slot: number;
  /** The details of the transaction */
  transaction: Transaction;
  /** Metadata produced from the transaction */
  meta: ConfirmedTransactionMeta | null;
  /** The unix timestamp of when the transaction was processed */
  blockTime?: number | null;
};

/**
 * A partially decoded transaction instruction
 */
export type PartiallyDecodedInstruction = {
  /** Program id called by this instruction */
  programId: PublicKey;
  /** Public keys of accounts passed to this instruction */
  accounts: Array<PublicKey>;
  /** Raw base-58 instruction data */
  data: string;
};

/**
 * A parsed transaction message account
 */
export type ParsedMessageAccount = {
  /** Public key of the account */
  pubkey: PublicKey;
  /** Indicates if the account signed the transaction */
  signer: boolean;
  /** Indicates if the account is writable for this transaction */
  writable: boolean;
};

/**
 * A parsed transaction instruction
 */
export type ParsedInstruction = {
  /** Name of the program for this instruction */
  program: string;
  /** ID of the program for this instruction */
  programId: PublicKey;
  /** Parsed instruction info */
  parsed: any;
};

/**
 * A parsed transaction message
 */
export type ParsedMessage = {
  /** Accounts used in the instructions */
  accountKeys: ParsedMessageAccount[];
  /** The atomically executed instructions for the transaction */
  instructions: (ParsedInstruction | PartiallyDecodedInstruction)[];
  /** Recent blockhash */
  recentBlockhash: string;
};

/**
 * A parsed transaction
 */
export type ParsedTransaction = {
  /** Signatures for the transaction */
  signatures: Array<string>;
  /** Message of the transaction */
  message: ParsedMessage;
};

/**
 * A parsed and confirmed transaction on the ledger
 */
export type ParsedConfirmedTransaction = {
  /** The slot during which the transaction was processed */
  slot: number;
  /** The details of the transaction */
  transaction: ParsedTransaction;
  /** Metadata produced from the transaction */
  meta: ParsedConfirmedTransactionMeta | null;
  /** The unix timestamp of when the transaction was processed */
  blockTime?: number | null;
};

/**
 * A processed block fetched from the RPC API
 */
export type BlockResponse = {
  /** Blockhash of this block */
  blockhash: Blockhash;
  /** Blockhash of this block's parent */
  previousBlockhash: Blockhash;
  /** Slot index of this block's parent */
  parentSlot: number;
  /** Vector of transactions with status meta and original message */
  transactions: Array<{
    /** The transaction */
    transaction: {
      /** The transaction message */
      message: Message;
      /** The transaction signatures */
      signatures: string[];
    };
    /** Metadata produced from the transaction */
    meta: ConfirmedTransactionMeta | null;
  }>;
  /** Vector of block rewards */
  rewards?: Array<{
    /** Public key of reward recipient */
    pubkey: string;
    /** Reward value in lamports */
    lamports: number;
    /** Account balance after reward is applied */
    postBalance: number | null;
    /** Type of reward received */
    rewardType: string | null;
  }>;
  /** The unix timestamp of when the block was processed */
  blockTime: number | null;
};

/**
 * A ConfirmedBlock on the ledger
 */
export type ConfirmedBlock = {
  /** Blockhash of this block */
  blockhash: Blockhash;
  /** Blockhash of this block's parent */
  previousBlockhash: Blockhash;
  /** Slot index of this block's parent */
  parentSlot: number;
  /** Vector of transactions and status metas */
  transactions: Array<{
    transaction: Transaction;
    meta: ConfirmedTransactionMeta | null;
  }>;
  /** Vector of block rewards */
  rewards?: Array<{
    pubkey: string;
    lamports: number;
    postBalance: number | null;
    rewardType: string | null;
  }>;
  /** The unix timestamp of when the block was processed */
  blockTime: number | null;
};

/**
 * A ConfirmedBlock on the ledger with signatures only
 */
export type ConfirmedBlockSignatures = {
  /** Blockhash of this block */
  blockhash: Blockhash;
  /** Blockhash of this block's parent */
  previousBlockhash: Blockhash;
  /** Slot index of this block's parent */
  parentSlot: number;
  /** Vector of signatures */
  signatures: Array<string>;
  /** The unix timestamp of when the block was processed */
  blockTime: number | null;
};

/**
 * A performance sample
 */
export type PerfSample = {
  /** Slot number of sample */
  slot: number;
  /** Number of transactions in a sample window */
  numTransactions: number;
  /** Number of slots in a sample window */
  numSlots: number;
  /** Sample window in seconds */
  samplePeriodSecs: number;
};

export type Fees = {
  /** Recent blockhash */
  blockhash: Blockhash;
  /** Calculator for transaction fees */
  feeCalculator: FeeCalculator;
  /** @deprecated this value is inaccurate and should not be relied upon */
  lastValidSlot: number;
  /** last block height at which a blockhash will be valid */
  lastValidBlockHeight: number;
}

function createRpcClient(
  url: string,
  useHttps: boolean,
  httpHeaders?: HttpHeaders,
  fetchMiddleware?: FetchMiddleware,
  disableRetryOnRateLimit?: boolean,
): RpcClient {
  let agentManager: AgentManager | undefined;
  if (!process.env.BROWSER) {
    agentManager = new AgentManager(useHttps);
  }

  let fetchWithMiddleware: (url: string, options: any) => Promise<Response>;

  if (fetchMiddleware) {
    fetchWithMiddleware = (url: string, options: any) => {
      return new Promise<Response>((resolve, reject) => {
        fetchMiddleware(url, options, async (url: string, options: any) => {
          try {
            resolve(await fetch(url, options));
          } catch (error) {
            reject(error);
          }
        });
      });
    };
  }

  const clientBrowser = new RpcClient(async (request, callback) => {
    const agent = agentManager ? agentManager.requestStart() : undefined;
    const options = {
      method: 'POST',
      body: request,
      agent,
      headers: Object.assign(
        {
          'Content-Type': 'application/json',
        },
        httpHeaders || {},
      ),
    };

    try {
      let too_many_requests_retries = 5;
      let res: Response;
      let waitTime = 500;
      for (;;) {
        if (fetchWithMiddleware) {
          res = await fetchWithMiddleware(url, options);
        } else {
          res = await fetch(url, options);
        }

        if (res.status !== 429 /* Too many requests */) {
          break;
        }
        if (disableRetryOnRateLimit === true) {
          break;
        }
        too_many_requests_retries -= 1;
        if (too_many_requests_retries === 0) {
          break;
        }
        console.log(
          `Server responded with ${res.status} ${res.statusText}.  Retrying after ${waitTime}ms delay...`,
        );
        await sleep(waitTime);
        waitTime *= 2;
      }

      const text = await res.text();
      if (res.ok) {
        callback(null, text);
      } else {
        callback(new Error(`${res.status} ${res.statusText}: ${text}`));
      }
    } catch (err) {
      callback(err);
    } finally {
      agentManager && agentManager.requestEnd();
    }
  }, {});

  return clientBrowser;
}

function createRpcRequest(client: RpcClient): RpcRequest {
  return (method, args) => {
    return new Promise((resolve, reject) => {
      client.request(method, args, (err: any, response: any) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(response);
      });
    });
  };
}

function createRpcBatchRequest(client: RpcClient): RpcBatchRequest {
  return (requests: RpcParams[]) => {
    return new Promise((resolve, reject) => {
      // Do nothing if requests is empty
      if (requests.length === 0) resolve([]);

      const batch = requests.map((params: RpcParams) => {
        return client.request(params.methodName, params.args);
      });

      client.request(batch, (err: any, response: any) => {
        if (err) {
          reject(err);
          return;
        }
        resolve(response);
      });
    });
  };
}

/**
 * Expected JSON RPC response for the "getInflationGovernor" message
 */
const GetInflationGovernorRpcResult = jsonRpcResult(GetInflationGovernorResult);

/**
 * Expected JSON RPC response for the "getEpochInfo" message
 */
const GetEpochInfoRpcResult = jsonRpcResult(GetEpochInfoResult);

/**
 * Expected JSON RPC response for the "getEpochSchedule" message
 */
const GetEpochScheduleRpcResult = jsonRpcResult(GetEpochScheduleResult);

/**
 * Expected JSON RPC response for the "getLeaderSchedule" message
 */
const GetLeaderScheduleRpcResult = jsonRpcResult(GetLeaderScheduleResult);

/**
 * Expected JSON RPC response for the "minimumLedgerSlot" and "getFirstAvailableBlock" messages
 */
const SlotRpcResult = jsonRpcResult(number());

/**
 * Supply
 */
export type Supply = {
  /** Total supply in lamports */
  total: number;
  /** Circulating supply in lamports */
  circulating: number;
  /** Non-circulating supply in lamports */
  nonCirculating: number;
  /** List of non-circulating account addresses */
  nonCirculatingAccounts: Array<PublicKey>;
};

/**
 * Expected JSON RPC response for the "getSupply" message
 */
const GetSupplyRpcResult = jsonRpcResultAndContext(
  pick({
    total: number(),
    circulating: number(),
    nonCirculating: number(),
    nonCirculatingAccounts: array(PublicKeyFromString),
  }),
);

/**
 * Token amount object which returns a token amount in different formats
 * for various client use cases.
 */
export type TokenAmount = {
  /** Raw amount of tokens as string ignoring decimals */
  amount: string;
  /** Number of decimals configured for token's mint */
  decimals: number;
  /** Token amount as float, accounts for decimals */
  uiAmount: number | null;
  /** Token amount as string, accounts for decimals */
  uiAmountString?: string;
};

/**
 * Expected JSON RPC structure for token amounts
 */
const TokenAmountResult = pick({
  amount: string(),
  uiAmount: nullable(number()),
  decimals: number(),
  uiAmountString: optional(string()),
});

/**
 * Token address and balance.
 */
export type TokenAccountBalancePair = {
  /** Address of the token account */
  address: PublicKey;
  /** Raw amount of tokens as string ignoring decimals */
  amount: string;
  /** Number of decimals configured for token's mint */
  decimals: number;
  /** Token amount as float, accounts for decimals */
  uiAmount: number | null;
  /** Token amount as string, accounts for decimals */
  uiAmountString?: string;
};

/**
 * Expected JSON RPC response for the "getTokenLargestAccounts" message
 */
const GetTokenLargestAccountsResult = jsonRpcResultAndContext(
  array(
    pick({
      address: PublicKeyFromString,
      amount: string(),
      uiAmount: nullable(number()),
      decimals: number(),
      uiAmountString: optional(string()),
    }),
  ),
);

/**
 * Expected JSON RPC response for the "getTokenAccountsByOwner" message
 */
const GetTokenAccountsByOwner = jsonRpcResultAndContext(
  array(
    pick({
      pubkey: PublicKeyFromString,
      account: pick({
        executable: boolean(),
        owner: PublicKeyFromString,
        lamports: number(),
        data: BufferFromRawAccountData,
        rentEpoch: number(),
      }),
    }),
  ),
);

const ParsedAccountDataResult = pick({
  program: string(),
  parsed: unknown(),
  space: number(),
});

/**
 * Expected JSON RPC response for the "getTokenAccountsByOwner" message with parsed data
 */
const GetParsedTokenAccountsByOwner = jsonRpcResultAndContext(
  array(
    pick({
      pubkey: PublicKeyFromString,
      account: pick({
        executable: boolean(),
        owner: PublicKeyFromString,
        lamports: number(),
        data: ParsedAccountDataResult,
        rentEpoch: number(),
      }),
    }),
  ),
);

/**
 * Pair of an account address and its balance
 */
export type AccountBalancePair = {
  address: PublicKey;
  lamports: number;
};

/**
 * Expected JSON RPC response for the "getLargestAccounts" message
 */
const GetLargestAccountsRpcResult = jsonRpcResultAndContext(
  array(
    pick({
      lamports: number(),
      address: PublicKeyFromString,
    }),
  ),
);

/**
 * @internal
 */
const AccountInfoResult = pick({
  executable: boolean(),
  owner: PublicKeyFromString,
  lamports: number(),
  data: BufferFromRawAccountData,
  rentEpoch: number(),
});

/**
 * @internal
 */
const KeyedAccountInfoResult = pick({
  pubkey: PublicKeyFromString,
  account: AccountInfoResult,
});

const ParsedOrRawAccountData = coerce(
  union([instance(Buffer), ParsedAccountDataResult]),
  union([RawAccountDataResult, ParsedAccountDataResult]),
  value => {
    if (Array.isArray(value)) {
      return create(value, BufferFromRawAccountData);
    } else {
      return value;
    }
  },
);

/**
 * @internal
 */
const ParsedAccountInfoResult = pick({
  executable: boolean(),
  owner: PublicKeyFromString,
  lamports: number(),
  data: ParsedOrRawAccountData,
  rentEpoch: number(),
});

const KeyedParsedAccountInfoResult = pick({
  pubkey: PublicKeyFromString,
  account: ParsedAccountInfoResult,
});

/**
 * @internal
 */
const StakeActivationResult = pick({
  state: union([
    literal('active'),
    literal('inactive'),
    literal('activating'),
    literal('deactivating'),
  ]),
  active: number(),
  inactive: number(),
});

/**
 * Expected JSON RPC response for the "getConfirmedSignaturesForAddress2" message
 */

const GetConfirmedSignaturesForAddress2RpcResult = jsonRpcResult(
  array(
    pick({
      signature: string(),
      slot: number(),
      err: TransactionErrorResult,
      memo: nullable(string()),
      blockTime: optional(nullable(number())),
    }),
  ),
);

/**
 * Expected JSON RPC response for the "getSignaturesForAddress" message
 */
const GetSignaturesForAddressRpcResult = jsonRpcResult(
  array(
    pick({
      signature: string(),
      slot: number(),
      err: TransactionErrorResult,
      memo: nullable(string()),
      blockTime: optional(nullable(number())),
    }),
  ),
);

/***
 * Expected JSON RPC response for the "accountNotification" message
 */
const AccountNotificationResult = pick({
  subscription: number(),
  result: notificationResultAndContext(AccountInfoResult),
});

/**
 * @internal
 */
const ProgramAccountInfoResult = pick({
  pubkey: PublicKeyFromString,
  account: AccountInfoResult,
});

/***
 * Expected JSON RPC response for the "programNotification" message
 */
const ProgramAccountNotificationResult = pick({
  subscription: number(),
  result: notificationResultAndContext(ProgramAccountInfoResult),
});

/**
 * @internal
 */
const SlotInfoResult = pick({
  parent: number(),
  slot: number(),
  root: number(),
});

/**
 * Expected JSON RPC response for the "slotNotification" message
 */
const SlotNotificationResult = pick({
  subscription: number(),
  result: SlotInfoResult,
});

/**
 * Slot updates which can be used for tracking the live progress of a cluster.
 * - `"firstShredReceived"`: connected node received the first shred of a block.
 * Indicates that a new block that is being produced.
 * - `"completed"`: connected node has received all shreds of a block. Indicates
 * a block was recently produced.
 * - `"optimisticConfirmation"`: block was optimistically confirmed by the
 * cluster. It is not guaranteed that an optimistic confirmation notification
 * will be sent for every finalized blocks.
 * - `"root"`: the connected node rooted this block.
 * - `"createdBank"`: the connected node has started validating this block.
 * - `"frozen"`: the connected node has validated this block.
 * - `"dead"`: the connected node failed to validate this block.
 */
export type SlotUpdate =
  | {
      type: 'firstShredReceived';
      slot: number;
      timestamp: number;
    }
  | {
      type: 'completed';
      slot: number;
      timestamp: number;
    }
  | {
      type: 'createdBank';
      slot: number;
      timestamp: number;
      parent: number;
    }
  | {
      type: 'frozen';
      slot: number;
      timestamp: number;
      stats: {
        numTransactionEntries: number;
        numSuccessfulTransactions: number;
        numFailedTransactions: number;
        maxTransactionsPerEntry: number;
      };
    }
  | {
      type: 'dead';
      slot: number;
      timestamp: number;
      err: string;
    }
  | {
      type: 'optimisticConfirmation';
      slot: number;
      timestamp: number;
    }
  | {
      type: 'root';
      slot: number;
      timestamp: number;
    };

/**
 * @internal
 */
const SlotUpdateResult = union([
  pick({
    type: union([
      literal('firstShredReceived'),
      literal('completed'),
      literal('optimisticConfirmation'),
      literal('root'),
    ]),
    slot: number(),
    timestamp: number(),
  }),
  pick({
    type: literal('createdBank'),
    parent: number(),
    slot: number(),
    timestamp: number(),
  }),
  pick({
    type: literal('frozen'),
    slot: number(),
    timestamp: number(),
    stats: pick({
      numTransactionEntries: number(),
      numSuccessfulTransactions: number(),
      numFailedTransactions: number(),
      maxTransactionsPerEntry: number(),
    }),
  }),
  pick({
    type: literal('dead'),
    slot: number(),
    timestamp: number(),
    err: string(),
  }),
]);

/**
 * Expected JSON RPC response for the "slotsUpdatesNotification" message
 */
const SlotUpdateNotificationResult = pick({
  subscription: number(),
  result: SlotUpdateResult,
});

/**
 * Expected JSON RPC response for the "signatureNotification" message
 */
const SignatureNotificationResult = pick({
  subscription: number(),
  result: notificationResultAndContext(
    union([SignatureStatusResult, SignatureReceivedResult]),
  ),
});

/**
 * Expected JSON RPC response for the "rootNotification" message
 */
const RootNotificationResult = pick({
  subscription: number(),
  result: number(),
});

const ContactInfoResult = pick({
  pubkey: string(),
  gossip: nullable(string()),
  tpu: nullable(string()),
  rpc: nullable(string()),
  version: nullable(string()),
});

const VoteAccountInfoResult = pick({
  votePubkey: string(),
  nodePubkey: string(),
  activatedStake: number(),
  epochVoteAccount: boolean(),
  epochCredits: array(tuple([number(), number(), number()])),
  commission: number(),
  lastVote: number(),
  rootSlot: nullable(number()),
});

/**
 * Expected JSON RPC response for the "getVoteAccounts" message
 */
const GetVoteAccounts = jsonRpcResult(
  pick({
    current: array(VoteAccountInfoResult),
    delinquent: array(VoteAccountInfoResult),
  }),
);

const ConfirmationStatus = union([
  literal('processed'),
  literal('confirmed'),
  literal('finalized'),
]);

const SignatureStatusResponse = pick({
  slot: number(),
  confirmations: nullable(number()),
  err: TransactionErrorResult,
  confirmationStatus: optional(ConfirmationStatus),
});

/**
 * Expected JSON RPC response for the "getSignatureStatuses" message
 */
const GetSignatureStatusesRpcResult = jsonRpcResultAndContext(
  array(nullable(SignatureStatusResponse)),
);

/**
 * Expected JSON RPC response for the "getMinimumBalanceForRentExemption" message
 */
const GetMinimumBalanceForRentExemptionRpcResult = jsonRpcResult(number());

const ConfirmedTransactionResult = pick({
  signatures: array(string()),
  message: pick({
    accountKeys: array(string()),
    header: pick({
      numRequiredSignatures: number(),
      numReadonlySignedAccounts: number(),
      numReadonlyUnsignedAccounts: number(),
    }),
    instructions: array(
      pick({
        accounts: array(number()),
        data: string(),
        programIdIndex: number(),
      }),
    ),
    recentBlockhash: string(),
  }),
});

const ParsedInstructionResult = pick({
  parsed: unknown(),
  program: string(),
  programId: PublicKeyFromString,
});

const RawInstructionResult = pick({
  accounts: array(PublicKeyFromString),
  data: string(),
  programId: PublicKeyFromString,
});

const InstructionResult = union([
  RawInstructionResult,
  ParsedInstructionResult,
]);

const UnknownInstructionResult = union([
  pick({
    parsed: unknown(),
    program: string(),
    programId: string(),
  }),
  pick({
    accounts: array(string()),
    data: string(),
    programId: string(),
  }),
]);

const ParsedOrRawInstruction = coerce(
  InstructionResult,
  UnknownInstructionResult,
  value => {
    if ('accounts' in value) {
      return create(value, RawInstructionResult);
    } else {
      return create(value, ParsedInstructionResult);
    }
  },
);

/**
 * @internal
 */
const ParsedConfirmedTransactionResult = pick({
  signatures: array(string()),
  message: pick({
    accountKeys: array(
      pick({
        pubkey: PublicKeyFromString,
        signer: boolean(),
        writable: boolean(),
      }),
    ),
    instructions: array(ParsedOrRawInstruction),
    recentBlockhash: string(),
  }),
});

const TokenBalanceResult = pick({
  accountIndex: number(),
  mint: string(),
  uiTokenAmount: TokenAmountResult,
});

/**
 * @internal
 */
const ConfirmedTransactionMetaResult = pick({
  err: TransactionErrorResult,
  fee: number(),
  innerInstructions: optional(
    nullable(
      array(
        pick({
          index: number(),
          instructions: array(
            pick({
              accounts: array(number()),
              data: string(),
              programIdIndex: number(),
            }),
          ),
        }),
      ),
    ),
  ),
  preBalances: array(number()),
  postBalances: array(number()),
  logMessages: optional(nullable(array(string()))),
  preTokenBalances: optional(nullable(array(TokenBalanceResult))),
  postTokenBalances: optional(nullable(array(TokenBalanceResult))),
});

/**
 * @internal
 */
const ParsedConfirmedTransactionMetaResult = pick({
  err: TransactionErrorResult,
  fee: number(),
  innerInstructions: optional(
    nullable(
      array(
        pick({
          index: number(),
          instructions: array(ParsedOrRawInstruction),
        }),
      ),
    ),
  ),
  preBalances: array(number()),
  postBalances: array(number()),
  logMessages: optional(nullable(array(string()))),
  preTokenBalances: optional(nullable(array(TokenBalanceResult))),
  postTokenBalances: optional(nullable(array(TokenBalanceResult))),
});

/**
 * Expected JSON RPC response for the "getConfirmedBlock" message
 */
const GetConfirmedBlockRpcResult = jsonRpcResult(
  nullable(
    pick({
      blockhash: string(),
      previousBlockhash: string(),
      parentSlot: number(),
      transactions: array(
        pick({
          transaction: ConfirmedTransactionResult,
          meta: nullable(ConfirmedTransactionMetaResult),
        }),
      ),
      rewards: optional(
        array(
          pick({
            pubkey: string(),
            lamports: number(),
            postBalance: nullable(number()),
            rewardType: nullable(string()),
          }),
        ),
      ),
      blockTime: nullable(number()),
    }),
  ),
);

/**
 * Expected JSON RPC response for the "getConfirmedBlockSignatures" message
 */
const GetConfirmedBlockSignaturesRpcResult = jsonRpcResult(
  nullable(
    pick({
      blockhash: string(),
      previousBlockhash: string(),
      parentSlot: number(),
      signatures: array(string()),
      blockTime: nullable(number()),
    }),
  ),
);

/**
 * Expected JSON RPC response for the "getConfirmedTransaction" message
 */
const GetConfirmedTransactionRpcResult = jsonRpcResult(
  nullable(
    pick({
      slot: number(),
      meta: ConfirmedTransactionMetaResult,
      blockTime: optional(nullable(number())),
      transaction: ConfirmedTransactionResult,
    }),
  ),
);

/**
 * Expected JSON RPC response for the "getConfirmedTransaction" message
 */
const GetParsedConfirmedTransactionRpcResult = jsonRpcResult(
  nullable(
    pick({
      slot: number(),
      transaction: ParsedConfirmedTransactionResult,
      meta: nullable(ParsedConfirmedTransactionMetaResult),
      blockTime: optional(nullable(number())),
    }),
  ),
);

/**
 * Expected JSON RPC response for the "getRecentBlockhash" message
 */
const GetRecentBlockhashAndContextRpcResult = jsonRpcResultAndContext(
  pick({
    blockhash: string(),
    feeCalculator: pick({
      lamportsPerSignature: number(),
    }),
  }),
);

const PerfSampleResult = pick({
  slot: number(),
  numTransactions: number(),
  numSlots: number(),
  samplePeriodSecs: number(),
});

/*
 * Expected JSON RPC response for "getRecentPerformanceSamples" message
 */
const GetRecentPerformanceSamplesRpcResult = jsonRpcResult(
  array(PerfSampleResult),
);

/**
 * Expected JSON RPC response for the "getFeeCalculatorForBlockhash" message
 */
const GetFeeCalculatorRpcResult = jsonRpcResultAndContext(
  nullable(
    pick({
      feeCalculator: pick({
        lamportsPerSignature: number(),
      }),
    }),
  ),
);

/**
 * Expected JSON RPC response for the "getFees" message
 */
const GetFeesRpcResult = jsonRpcResultAndContext(
  pick({
    blockhash: string(),
    feeCalculator: pick({
      lamportsPerSignature: number(),
    }),
    lastValidSlot: number(),
    lastValidBlockHeight: number(),
  }),
);

/**
 * Expected JSON RPC response for the "requestAirdrop" message
 */
const RequestAirdropRpcResult = jsonRpcResult(string());

/**
 * Expected JSON RPC response for the "sendTransaction" message
 */
const SendTransactionRpcResult = jsonRpcResult(string());

/**
 * Information about the latest slot being processed by a node
 */
export type SlotInfo = {
  /** Currently processing slot */
  slot: number;
  /** Parent of the current slot */
  parent: number;
  /** The root block of the current slot's fork */
  root: number;
};

/**
 * Parsed account data
 */
export type ParsedAccountData = {
  /** Name of the program that owns this account */
  program: string;
  /** Parsed account data */
  parsed: any;
  /** Space used by account data */
  space: number;
};

/**
 * Stake Activation data
 */
export type StakeActivationData = {
  /** the stake account's activation state */
  state: 'active' | 'inactive' | 'activating' | 'deactivating';
  /** stake active during the epoch */
  active: number;
  /** stake inactive during the epoch */
  inactive: number;
};

/**
 * Data slice argument for getProgramAccounts
 */
export type DataSlice = {
  /** offset of data slice */
  offset: number;
  /** length of data slice */
  length: number;
};

/**
 * Memory comparison filter for getProgramAccounts
 */
export type MemcmpFilter = {
  memcmp: {
    /** offset into program account data to start comparison */
    offset: number;
    /** data to match, as base-58 encoded string and limited to less than 129 bytes */
    bytes: string;
  };
};

/**
 * Data size comparison filter for getProgramAccounts
 */
export type DataSizeFilter = {
  /** Size of data for program account data length comparison */
  dataSize: number;
};

/**
 * A filter object for getProgramAccounts
 */
export type GetProgramAccountsFilter = MemcmpFilter | DataSizeFilter;

/**
 * Configuration object for getProgramAccounts requests
 */
export type GetProgramAccountsConfig = {
  /** Optional commitment level */
  commitment?: Commitment;
  /** Optional encoding for account data (default base64)
   * To use "jsonParsed" encoding, please refer to `getParsedProgramAccounts` in connection.ts
   * */
  encoding?: 'base64';
  /** Optional data slice to limit the returned account data */
  dataSlice?: DataSlice;
  /** Optional array of filters to apply to accounts */
  filters?: GetProgramAccountsFilter[];
};

/**
 * Configuration object for getParsedProgramAccounts
 */
export type GetParsedProgramAccountsConfig = {
  /** Optional commitment level */
  commitment?: Commitment;
  /** Optional array of filters to apply to accounts */
  filters?: GetProgramAccountsFilter[];
};

/**
 * Information describing an account
 */
export type AccountInfo<T> = {
  /** `true` if this account's data contains a loaded program */
  executable: boolean;
  /** Identifier of the program that owns the account */
  owner: PublicKey;
  /** Number of lamports assigned to the account */
  lamports: number;
  /** Optional data assigned to the account */
  data: T;
};

/**
 * Account information identified by pubkey
 */
export type KeyedAccountInfo = {
  accountId: PublicKey;
  accountInfo: AccountInfo<Buffer>;
};

/**
 * Callback function for account change notifications
 */
export type AccountChangeCallback = (
  accountInfo: AccountInfo<Buffer>,
  context: Context,
) => void;

/**
 * @internal
 */
type SubscriptionId = 'subscribing' | number;

/**
 * @internal
 */
type AccountSubscriptionInfo = {
  publicKey: string; // PublicKey of the account as a base 58 string
  callback: AccountChangeCallback;
  commitment?: Commitment;
  subscriptionId: SubscriptionId | null; // null when there's no current server subscription id
};

/**
 * Callback function for program account change notifications
 */
export type ProgramAccountChangeCallback = (
  keyedAccountInfo: KeyedAccountInfo,
  context: Context,
) => void;

/**
 * @internal
 */
type ProgramAccountSubscriptionInfo = {
  programId: string; // PublicKey of the program as a base 58 string
  callback: ProgramAccountChangeCallback;
  commitment?: Commitment;
  subscriptionId: SubscriptionId | null; // null when there's no current server subscription id
  filters?: GetProgramAccountsFilter[];
};

/**
 * Callback function for slot change notifications
 */
export type SlotChangeCallback = (slotInfo: SlotInfo) => void;

/**
 * @internal
 */
type SlotSubscriptionInfo = {
  callback: SlotChangeCallback;
  subscriptionId: SubscriptionId | null; // null when there's no current server subscription id
};

/**
 * Callback function for slot update notifications
 */
export type SlotUpdateCallback = (slotUpdate: SlotUpdate) => void;

/**
 * @private
 */
type SlotUpdateSubscriptionInfo = {
  callback: SlotUpdateCallback;
  subscriptionId: SubscriptionId | null; // null when there's no current server subscription id
};

/**
 * Callback function for signature status notifications
 */
export type SignatureResultCallback = (
  signatureResult: SignatureResult,
  context: Context,
) => void;

/**
 * Signature status notification with transaction result
 */
export type SignatureStatusNotification = {
  type: 'status';
  result: SignatureResult;
};

/**
 * Signature received notification
 */
export type SignatureReceivedNotification = {
  type: 'received';
};

/**
 * Callback function for signature notifications
 */
export type SignatureSubscriptionCallback = (
  notification: SignatureStatusNotification | SignatureReceivedNotification,
  context: Context,
) => void;

/**
 * Signature subscription options
 */
export type SignatureSubscriptionOptions = {
  commitment?: Commitment;
  enableReceivedNotification?: boolean;
};

/**
 * @internal
 */
type SignatureSubscriptionInfo = {
  signature: TransactionSignature; // TransactionSignature as a base 58 string
  callback: SignatureSubscriptionCallback;
  options?: SignatureSubscriptionOptions;
  subscriptionId: SubscriptionId | null; // null when there's no current server subscription id
};

/**
 * Callback function for root change notifications
 */
export type RootChangeCallback = (root: number) => void;

/**
 * @internal
 */
type RootSubscriptionInfo = {
  callback: RootChangeCallback;
  subscriptionId: SubscriptionId | null; // null when there's no current server subscription id
};

/**
 * @internal
 */
const LogsResult = pick({
  err: TransactionErrorResult,
  logs: array(string()),
  signature: string(),
});

/**
 * Logs result.
 */
export type Logs = {
  err: TransactionError | null;
  logs: string[];
  signature: string;
};

/**
 * Expected JSON RPC response for the "logsNotification" message.
 */
const LogsNotificationResult = pick({
  result: notificationResultAndContext(LogsResult),
  subscription: number(),
});

/**
 * Filter for log subscriptions.
 */
export type LogsFilter = PublicKey | 'all' | 'allWithVotes';

/**
 * Callback function for log notifications.
 */
export type LogsCallback = (logs: Logs, ctx: Context) => void;

/**
 * @private
 */
type LogsSubscriptionInfo = {
  callback: LogsCallback;
  filter: LogsFilter;
  subscriptionId: SubscriptionId | null; // null when there's no current server subscription id
  commitment?: Commitment;
};

/**
 * Signature result
 */
export type SignatureResult = {
  err: TransactionError | null;
};

/**
 * Transaction error
 */
export type TransactionError = {} | string;

/**
 * Transaction confirmation status
 * <pre>
 *   'processed': Transaction landed in a block which has reached 1 confirmation by the connected node
 *   'confirmed': Transaction landed in a block which has reached 1 confirmation by the cluster
 *   'finalized': Transaction landed in a block which has been finalized by the cluster
 * </pre>
 */
export type TransactionConfirmationStatus =
  | 'processed'
  | 'confirmed'
  | 'finalized';

/**
 * Signature status
 */
export type SignatureStatus = {
  /** when the transaction was processed */
  slot: number;
  /** the number of blocks that have been confirmed and voted on in the fork containing `slot` */
  confirmations: number | null;
  /** transaction error, if any */
  err: TransactionError | null;
  /** cluster confirmation status, if data available. Possible responses: `processed`, `confirmed`, `finalized` */
  confirmationStatus?: TransactionConfirmationStatus;
};

/**
 * A confirmed signature with its status
 */
export type ConfirmedSignatureInfo = {
  /** the transaction signature */
  signature: string;
  /** when the transaction was processed */
  slot: number;
  /** error, if any */
  err: TransactionError | null;
  /** memo associated with the transaction, if any */
  memo: string | null;
  /** The unix timestamp of when the transaction was processed */
  blockTime?: number | null;
};

/**
 * An object defining headers to be passed to the RPC server
 */
export type HttpHeaders = {[header: string]: string};

/**
 * A callback used to augment the outgoing HTTP request
 */
export type FetchMiddleware = (
  url: string,
  options: any,
  fetch: Function,
) => void;

/**
 * Configuration for instantiating a Connection
 */
export type ConnectionConfig = {
  /** Optional commitment level */
  commitment?: Commitment;
  /** Optional endpoint URL to the fullnode JSON RPC PubSub WebSocket Endpoint */
  wsEndpoint?: string;
  /** Optional HTTP headers object */
  httpHeaders?: HttpHeaders;
  /** Optional fetch middleware callback */
  fetchMiddleware?: FetchMiddleware;
  /** Optional Disable retring calls when server responds with HTTP 429 (Too Many Requests) */
  disableRetryOnRateLimit?: boolean;
};

/**
 * A connection to a fullnode JSON RPC endpoint
 */
export class Connection {
  /** @internal */ _commitment?: Commitment;
  /** @internal */ _rpcEndpoint: string;
  /** @internal */ _rpcWsEndpoint: string;
  /** @internal */ _rpcClient: RpcClient;
  /** @internal */ _rpcRequest: RpcRequest;
  /** @internal */ _rpcBatchRequest: RpcBatchRequest;
  /** @internal */ _rpcWebSocket: RpcWebSocketClient;
  /** @internal */ _rpcWebSocketConnected: boolean = false;
  /** @internal */ _rpcWebSocketHeartbeat: ReturnType<
    typeof setInterval
  > | null = null;
  /** @internal */ _rpcWebSocketIdleTimeout: ReturnType<
    typeof setTimeout
  > | null = null;

  /** @internal */ _disableBlockhashCaching: boolean = false;
  /** @internal */ _pollingBlockhash: boolean = false;
  /** @internal */ _blockhashInfo: {
    recentBlockhash: Blockhash | null;
    lastFetch: number;
    simulatedSignatures: Array<string>;
    transactionSignatures: Array<string>;
  } = {
    recentBlockhash: null,
    lastFetch: 0,
    transactionSignatures: [],
    simulatedSignatures: [],
  };

  /** @internal */ _accountChangeSubscriptionCounter: number = 0;
  /** @internal */ _accountChangeSubscriptions: {
    [id: number]: AccountSubscriptionInfo;
  } = {};

  /** @internal */ _programAccountChangeSubscriptionCounter: number = 0;
  /** @internal */ _programAccountChangeSubscriptions: {
    [id: number]: ProgramAccountSubscriptionInfo;
  } = {};

  /** @internal */ _rootSubscriptionCounter: number = 0;
  /** @internal */ _rootSubscriptions: {
    [id: number]: RootSubscriptionInfo;
  } = {};

  /** @internal */ _signatureSubscriptionCounter: number = 0;
  /** @internal */ _signatureSubscriptions: {
    [id: number]: SignatureSubscriptionInfo;
  } = {};

  /** @internal */ _slotSubscriptionCounter: number = 0;
  /** @internal */ _slotSubscriptions: {
    [id: number]: SlotSubscriptionInfo;
  } = {};

  /** @internal */ _logsSubscriptionCounter: number = 0;
  /** @internal */ _logsSubscriptions: {
    [id: number]: LogsSubscriptionInfo;
  } = {};

  /** @internal */ _slotUpdateSubscriptionCounter: number = 0;
  /** @internal */ _slotUpdateSubscriptions: {
    [id: number]: SlotUpdateSubscriptionInfo;
  } = {};

  /**
   * Establish a JSON RPC connection
   *
   * @param endpoint URL to the fullnode JSON RPC endpoint
   * @param commitmentOrConfig optional default commitment level or optional ConnectionConfig configuration object
   */
  constructor(
    endpoint: string,
    commitmentOrConfig?: Commitment | ConnectionConfig,
  ) {
    let url = new URL(endpoint);
    const useHttps = url.protocol === 'https:';

    let wsEndpoint;
    let httpHeaders;
    let fetchMiddleware;
    let disableRetryOnRateLimit;
    if (commitmentOrConfig && typeof commitmentOrConfig === 'string') {
      this._commitment = commitmentOrConfig;
    } else if (commitmentOrConfig) {
      this._commitment = commitmentOrConfig.commitment;
      wsEndpoint = commitmentOrConfig.wsEndpoint;
      httpHeaders = commitmentOrConfig.httpHeaders;
      fetchMiddleware = commitmentOrConfig.fetchMiddleware;
      disableRetryOnRateLimit = commitmentOrConfig.disableRetryOnRateLimit;
    }

    this._rpcEndpoint = endpoint;
    this._rpcWsEndpoint = wsEndpoint || makeWebsocketUrl(endpoint);

    this._rpcClient = createRpcClient(
      url.toString(),
      useHttps,
      httpHeaders,
      fetchMiddleware,
      disableRetryOnRateLimit,
    );
    this._rpcRequest = createRpcRequest(this._rpcClient);
    this._rpcBatchRequest = createRpcBatchRequest(this._rpcClient);

    this._rpcWebSocket = new RpcWebSocketClient(this._rpcWsEndpoint, {
      autoconnect: false,
      max_reconnects: Infinity,
    });
    this._rpcWebSocket.on('open', this._wsOnOpen.bind(this));
    this._rpcWebSocket.on('error', this._wsOnError.bind(this));
    this._rpcWebSocket.on('close', this._wsOnClose.bind(this));
    this._rpcWebSocket.on(
      'accountNotification',
      this._wsOnAccountNotification.bind(this),
    );
    this._rpcWebSocket.on(
      'programNotification',
      this._wsOnProgramAccountNotification.bind(this),
    );
    this._rpcWebSocket.on(
      'slotNotification',
      this._wsOnSlotNotification.bind(this),
    );
    this._rpcWebSocket.on(
      'slotsUpdatesNotification',
      this._wsOnSlotUpdatesNotification.bind(this),
    );
    this._rpcWebSocket.on(
      'signatureNotification',
      this._wsOnSignatureNotification.bind(this),
    );
    this._rpcWebSocket.on(
      'rootNotification',
      this._wsOnRootNotification.bind(this),
    );
    this._rpcWebSocket.on(
      'logsNotification',
      this._wsOnLogsNotification.bind(this),
    );
  }

  /**
   * The default commitment used for requests
   */
  get commitment(): Commitment | undefined {
    return this._commitment;
  }

  /**
   * Fetch the balance for the specified public key, return with context
   */
  async getBalanceAndContext(
    publicKey: PublicKey,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<number>> {
    const args = this._buildArgs([publicKey.toBase58()], commitment);
    const unsafeRes = await this._rpcRequest('getBalance', args);
    const res = create(unsafeRes, jsonRpcResultAndContext(number()));
    if ('error' in res) {
      throw new Error(
        'failed to get balance for ' +
          publicKey.toBase58() +
          ': ' +
          res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch the balance for the specified public key
   */
  async getBalance(
    publicKey: PublicKey,
    commitment?: Commitment,
  ): Promise<number> {
    return await this.getBalanceAndContext(publicKey, commitment)
      .then(x => x.value)
      .catch(e => {
        throw new Error(
          'failed to get balance of account ' + publicKey.toBase58() + ': ' + e,
        );
      });
  }

  /**
   * Fetch the estimated production time of a block
   */
  async getBlockTime(slot: number): Promise<number | null> {
    const unsafeRes = await this._rpcRequest('getBlockTime', [slot]);
    const res = create(unsafeRes, jsonRpcResult(nullable(number())));
    if ('error' in res) {
      throw new Error(
        'failed to get block time for slot ' + slot + ': ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch the lowest slot that the node has information about in its ledger.
   * This value may increase over time if the node is configured to purge older ledger data
   */
  async getMinimumLedgerSlot(): Promise<number> {
    const unsafeRes = await this._rpcRequest('minimumLedgerSlot', []);
    const res = create(unsafeRes, jsonRpcResult(number()));
    if ('error' in res) {
      throw new Error(
        'failed to get minimum ledger slot: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch the slot of the lowest confirmed block that has not been purged from the ledger
   */
  async getFirstAvailableBlock(): Promise<number> {
    const unsafeRes = await this._rpcRequest('getFirstAvailableBlock', []);
    const res = create(unsafeRes, SlotRpcResult);
    if ('error' in res) {
      throw new Error(
        'failed to get first available block: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch information about the current supply
   */
  async getSupply(
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<Supply>> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getSupply', args);
    const res = create(unsafeRes, GetSupplyRpcResult);
    if ('error' in res) {
      throw new Error('failed to get supply: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the current supply of a token mint
   */
  async getTokenSupply(
    tokenMintAddress: PublicKey,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<TokenAmount>> {
    const args = this._buildArgs([tokenMintAddress.toBase58()], commitment);
    const unsafeRes = await this._rpcRequest('getTokenSupply', args);
    const res = create(unsafeRes, jsonRpcResultAndContext(TokenAmountResult));
    if ('error' in res) {
      throw new Error('failed to get token supply: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the current balance of a token account
   */
  async getTokenAccountBalance(
    tokenAddress: PublicKey,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<TokenAmount>> {
    const args = this._buildArgs([tokenAddress.toBase58()], commitment);
    const unsafeRes = await this._rpcRequest('getTokenAccountBalance', args);
    const res = create(unsafeRes, jsonRpcResultAndContext(TokenAmountResult));
    if ('error' in res) {
      throw new Error(
        'failed to get token account balance: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch all the token accounts owned by the specified account
   *
   * @return {Promise<RpcResponseAndContext<Array<{pubkey: PublicKey, account: AccountInfo<Buffer>}>>>}
   */
  async getTokenAccountsByOwner(
    ownerAddress: PublicKey,
    filter: TokenAccountsFilter,
    commitment?: Commitment,
  ): Promise<
    RpcResponseAndContext<
      Array<{pubkey: PublicKey; account: AccountInfo<Buffer>}>
    >
  > {
    let _args: any[] = [ownerAddress.toBase58()];
    if ('mint' in filter) {
      _args.push({mint: filter.mint.toBase58()});
    } else {
      _args.push({programId: filter.programId.toBase58()});
    }

    const args = this._buildArgs(_args, commitment, 'base64');
    const unsafeRes = await this._rpcRequest('getTokenAccountsByOwner', args);
    const res = create(unsafeRes, GetTokenAccountsByOwner);
    if ('error' in res) {
      throw new Error(
        'failed to get token accounts owned by account ' +
          ownerAddress.toBase58() +
          ': ' +
          res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch parsed token accounts owned by the specified account
   *
   * @return {Promise<RpcResponseAndContext<Array<{pubkey: PublicKey, account: AccountInfo<ParsedAccountData>}>>>}
   */
  async getParsedTokenAccountsByOwner(
    ownerAddress: PublicKey,
    filter: TokenAccountsFilter,
    commitment?: Commitment,
  ): Promise<
    RpcResponseAndContext<
      Array<{pubkey: PublicKey; account: AccountInfo<ParsedAccountData>}>
    >
  > {
    let _args: any[] = [ownerAddress.toBase58()];
    if ('mint' in filter) {
      _args.push({mint: filter.mint.toBase58()});
    } else {
      _args.push({programId: filter.programId.toBase58()});
    }

    const args = this._buildArgs(_args, commitment, 'jsonParsed');
    const unsafeRes = await this._rpcRequest('getTokenAccountsByOwner', args);
    const res = create(unsafeRes, GetParsedTokenAccountsByOwner);
    if ('error' in res) {
      throw new Error(
        'failed to get token accounts owned by account ' +
          ownerAddress.toBase58() +
          ': ' +
          res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch the 20 largest accounts with their current balances
   */
  async getLargestAccounts(
    config?: GetLargestAccountsConfig,
  ): Promise<RpcResponseAndContext<Array<AccountBalancePair>>> {
    const arg = {
      ...config,
      commitment: (config && config.commitment) || this.commitment,
    };
    const args = arg.filter || arg.commitment ? [arg] : [];
    const unsafeRes = await this._rpcRequest('getLargestAccounts', args);
    const res = create(unsafeRes, GetLargestAccountsRpcResult);
    if ('error' in res) {
      throw new Error('failed to get largest accounts: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the 20 largest token accounts with their current balances
   * for a given mint.
   */
  async getTokenLargestAccounts(
    mintAddress: PublicKey,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<Array<TokenAccountBalancePair>>> {
    const args = this._buildArgs([mintAddress.toBase58()], commitment);
    const unsafeRes = await this._rpcRequest('getTokenLargestAccounts', args);
    const res = create(unsafeRes, GetTokenLargestAccountsResult);
    if ('error' in res) {
      throw new Error(
        'failed to get token largest accounts: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch all the account info for the specified public key, return with context
   */
  async getAccountInfoAndContext(
    publicKey: PublicKey,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<AccountInfo<Buffer> | null>> {
    const args = this._buildArgs([publicKey.toBase58()], commitment, 'base64');
    const unsafeRes = await this._rpcRequest('getAccountInfo', args);
    const res = create(
      unsafeRes,
      jsonRpcResultAndContext(nullable(AccountInfoResult)),
    );
    if ('error' in res) {
      throw new Error(
        'failed to get info about account ' +
          publicKey.toBase58() +
          ': ' +
          res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch parsed account info for the specified public key
   */
  async getParsedAccountInfo(
    publicKey: PublicKey,
    commitment?: Commitment,
  ): Promise<
    RpcResponseAndContext<AccountInfo<Buffer | ParsedAccountData> | null>
  > {
    const args = this._buildArgs(
      [publicKey.toBase58()],
      commitment,
      'jsonParsed',
    );
    const unsafeRes = await this._rpcRequest('getAccountInfo', args);
    const res = create(
      unsafeRes,
      jsonRpcResultAndContext(nullable(ParsedAccountInfoResult)),
    );
    if ('error' in res) {
      throw new Error(
        'failed to get info about account ' +
          publicKey.toBase58() +
          ': ' +
          res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch all the account info for the specified public key
   */
  async getAccountInfo(
    publicKey: PublicKey,
    commitment?: Commitment,
  ): Promise<AccountInfo<Buffer> | null> {
    try {
      const res = await this.getAccountInfoAndContext(publicKey, commitment);
      return res.value;
    } catch (e) {
      throw new Error(
        'failed to get info about account ' + publicKey.toBase58() + ': ' + e,
      );
    }
  }

  /**
   * Fetch all the account info for multiple accounts specified by an array of public keys
   */
  async getMultipleAccountsInfo(
    publicKeys: PublicKey[],
    commitment?: Commitment,
  ): Promise<AccountInfo<Buffer>[] | null> {
    const keys = publicKeys.map(key => key.toBase58());
    const args = this._buildArgs([keys], commitment, 'base64');
    const unsafeRes = await this._rpcRequest('getMultipleAccounts', args);
    const res = create(
      unsafeRes,
      jsonRpcResultAndContext(nullable(array(AccountInfoResult))),
    );
    if ('error' in res) {
      throw new Error(
        'failed to get info for accounts ' + keys + ': ' + res.error.message,
      );
    }
    return res.result.value;
  }

  /**
   * Returns epoch activation information for a stake account that has been delegated
   */
  async getStakeActivation(
    publicKey: PublicKey,
    commitment?: Commitment,
    epoch?: number,
  ): Promise<StakeActivationData> {
    const args = this._buildArgs(
      [publicKey.toBase58()],
      commitment,
      undefined,
      epoch !== undefined ? {epoch} : undefined,
    );

    const unsafeRes = await this._rpcRequest('getStakeActivation', args);
    const res = create(unsafeRes, jsonRpcResult(StakeActivationResult));
    if ('error' in res) {
      throw new Error(
        `failed to get Stake Activation ${publicKey.toBase58()}: ${
          res.error.message
        }`,
      );
    }
    return res.result;
  }

  /**
   * Fetch all the accounts owned by the specified program id
   *
   * @return {Promise<Array<{pubkey: PublicKey, account: AccountInfo<Buffer>}>>}
   */
  async getProgramAccounts(
    programId: PublicKey,
    configOrCommitment?: GetProgramAccountsConfig | Commitment,
  ): Promise<Array<{pubkey: PublicKey; account: AccountInfo<Buffer>}>> {
    const extra: Pick<GetProgramAccountsConfig, 'dataSlice' | 'filters'> = {};

    let commitment;
    let encoding;
    if (configOrCommitment) {
      if (typeof configOrCommitment === 'string') {
        commitment = configOrCommitment;
      } else {
        commitment = configOrCommitment.commitment;
        encoding = configOrCommitment.encoding;

        if (configOrCommitment.dataSlice) {
          extra.dataSlice = configOrCommitment.dataSlice;
        }
        if (configOrCommitment.filters) {
          extra.filters = configOrCommitment.filters;
        }
      }
    }

    const args = this._buildArgs(
      [programId.toBase58()],
      commitment,
      encoding || 'base64',
      extra,
    );
    const unsafeRes = await this._rpcRequest('getProgramAccounts', args);
    const res = create(unsafeRes, jsonRpcResult(array(KeyedAccountInfoResult)));
    if ('error' in res) {
      throw new Error(
        'failed to get accounts owned by program ' +
          programId.toBase58() +
          ': ' +
          res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch and parse all the accounts owned by the specified program id
   *
   * @return {Promise<Array<{pubkey: PublicKey, account: AccountInfo<Buffer | ParsedAccountData>}>>}
   */
  async getParsedProgramAccounts(
    programId: PublicKey,
    configOrCommitment?: GetParsedProgramAccountsConfig | Commitment,
  ): Promise<
    Array<{
      pubkey: PublicKey;
      account: AccountInfo<Buffer | ParsedAccountData>;
    }>
  > {
    const extra: Pick<GetParsedProgramAccountsConfig, 'filters'> = {};

    let commitment;
    if (configOrCommitment) {
      if (typeof configOrCommitment === 'string') {
        commitment = configOrCommitment;
      } else {
        commitment = configOrCommitment.commitment;

        if (configOrCommitment.filters) {
          extra.filters = configOrCommitment.filters;
        }
      }
    }

    const args = this._buildArgs(
      [programId.toBase58()],
      commitment,
      'jsonParsed',
      extra,
    );
    const unsafeRes = await this._rpcRequest('getProgramAccounts', args);
    const res = create(
      unsafeRes,
      jsonRpcResult(array(KeyedParsedAccountInfoResult)),
    );
    if ('error' in res) {
      throw new Error(
        'failed to get accounts owned by program ' +
          programId.toBase58() +
          ': ' +
          res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Confirm the transaction identified by the specified signature.
   */
  async confirmTransaction(
    signature: TransactionSignature,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<SignatureResult>> {
    let decodedSignature;
    try {
      decodedSignature = bs58.decode(signature);
    } catch (err) {
      throw new Error('signature must be base58 encoded: ' + signature);
    }

    assert(decodedSignature.length === 64, 'signature has invalid length');

    const start = Date.now();
    const subscriptionCommitment = commitment || this.commitment;

    let subscriptionId;
    let response: RpcResponseAndContext<SignatureResult> | null = null;
    const confirmPromise = new Promise((resolve, reject) => {
      try {
        subscriptionId = this.onSignature(
          signature,
          (result: SignatureResult, context: Context) => {
            subscriptionId = undefined;
            response = {
              context,
              value: result,
            };
            resolve(null);
          },
          subscriptionCommitment,
        );
      } catch (err) {
        reject(err);
      }
    });

    let timeoutMs = 60 * 1000;
    switch (subscriptionCommitment) {
      case 'processed':
      case 'recent':
      case 'single':
      case 'confirmed':
      case 'singleGossip': {
        timeoutMs = 30 * 1000;
        break;
      }
      // exhaust enums to ensure full coverage
      case 'finalized':
      case 'max':
      case 'root':
    }

    try {
      await promiseTimeout(confirmPromise, timeoutMs);
    } finally {
      if (subscriptionId) {
        this.removeSignatureListener(subscriptionId);
      }
    }

    if (response === null) {
      const duration = (Date.now() - start) / 1000;
      throw new Error(
        `Transaction was not confirmed in ${duration.toFixed(
          2,
        )} seconds. It is unknown if it succeeded or failed. Check signature ${signature} using the Solana Explorer or CLI tools.`,
      );
    }

    return response;
  }

  /**
   * Return the list of nodes that are currently participating in the cluster
   */
  async getClusterNodes(): Promise<Array<ContactInfo>> {
    const unsafeRes = await this._rpcRequest('getClusterNodes', []);
    const res = create(unsafeRes, jsonRpcResult(array(ContactInfoResult)));
    if ('error' in res) {
      throw new Error('failed to get cluster nodes: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Return the list of nodes that are currently participating in the cluster
   */
  async getVoteAccounts(commitment?: Commitment): Promise<VoteAccountStatus> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getVoteAccounts', args);
    const res = create(unsafeRes, GetVoteAccounts);
    if ('error' in res) {
      throw new Error('failed to get vote accounts: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the current slot that the node is processing
   */
  async getSlot(commitment?: Commitment): Promise<number> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getSlot', args);
    const res = create(unsafeRes, jsonRpcResult(number()));
    if ('error' in res) {
      throw new Error('failed to get slot: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the current slot leader of the cluster
   */
  async getSlotLeader(commitment?: Commitment): Promise<string> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getSlotLeader', args);
    const res = create(unsafeRes, jsonRpcResult(string()));
    if ('error' in res) {
      throw new Error('failed to get slot leader: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch `limit` number of slot leaders starting from `startSlot`
   *
   * @param startSlot fetch slot leaders starting from this slot
   * @param limit number of slot leaders to return
   */
  async getSlotLeaders(
    startSlot: number,
    limit: number,
  ): Promise<Array<PublicKey>> {
    const args = [startSlot, limit];
    const unsafeRes = await this._rpcRequest('getSlotLeaders', args);
    const res = create(unsafeRes, jsonRpcResult(array(PublicKeyFromString)));
    if ('error' in res) {
      throw new Error('failed to get slot leaders: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the current status of a signature
   */
  async getSignatureStatus(
    signature: TransactionSignature,
    config?: SignatureStatusConfig,
  ): Promise<RpcResponseAndContext<SignatureStatus | null>> {
    const {context, value: values} = await this.getSignatureStatuses(
      [signature],
      config,
    );
    assert(values.length === 1);
    const value = values[0];
    return {context, value};
  }

  /**
   * Fetch the current statuses of a batch of signatures
   */
  async getSignatureStatuses(
    signatures: Array<TransactionSignature>,
    config?: SignatureStatusConfig,
  ): Promise<RpcResponseAndContext<Array<SignatureStatus | null>>> {
    const params: any[] = [signatures];
    if (config) {
      params.push(config);
    }
    const unsafeRes = await this._rpcRequest('getSignatureStatuses', params);
    const res = create(unsafeRes, GetSignatureStatusesRpcResult);
    if ('error' in res) {
      throw new Error('failed to get signature status: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the current transaction count of the cluster
   */
  async getTransactionCount(commitment?: Commitment): Promise<number> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getTransactionCount', args);
    const res = create(unsafeRes, jsonRpcResult(number()));
    if ('error' in res) {
      throw new Error('failed to get transaction count: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the current total currency supply of the cluster in lamports
   *
   * @deprecated Deprecated since v1.2.8. Please use {@link getSupply} instead.
   */
  async getTotalSupply(commitment?: Commitment): Promise<number> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getSupply', args);
    const res = create(unsafeRes, GetSupplyRpcResult);
    if ('error' in res) {
      throw new Error('failed to get total supply: ' + res.error.message);
    }
    return res.result.value.total;
  }

  /**
   * Fetch the cluster InflationGovernor parameters
   */
  async getInflationGovernor(
    commitment?: Commitment,
  ): Promise<InflationGovernor> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getInflationGovernor', args);
    const res = create(unsafeRes, GetInflationGovernorRpcResult);
    if ('error' in res) {
      throw new Error('failed to get inflation: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the inflation reward for a list of addresses for an epoch
   */
  async getInflationReward(
    addresses: PublicKey[],
    epoch?: number,
    commitment?: Commitment,
  ): Promise<(InflationReward | null)[]> {
    const args = this._buildArgs(
      [addresses.map(pubkey => pubkey.toBase58())],
      commitment,
      undefined,
      {
        epoch,
      },
    );
    const unsafeRes = await this._rpcRequest('getInflationReward', args);
    const res = create(unsafeRes, GetInflationRewardResult);
    if ('error' in res) {
      throw new Error('failed to get inflation reward: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the Epoch Info parameters
   */
  async getEpochInfo(commitment?: Commitment): Promise<EpochInfo> {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getEpochInfo', args);
    const res = create(unsafeRes, GetEpochInfoRpcResult);
    if ('error' in res) {
      throw new Error('failed to get epoch info: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the Epoch Schedule parameters
   */
  async getEpochSchedule(): Promise<EpochSchedule> {
    const unsafeRes = await this._rpcRequest('getEpochSchedule', []);
    const res = create(unsafeRes, GetEpochScheduleRpcResult);
    if ('error' in res) {
      throw new Error('failed to get epoch schedule: ' + res.error.message);
    }
    const epochSchedule = res.result;
    return new EpochSchedule(
      epochSchedule.slotsPerEpoch,
      epochSchedule.leaderScheduleSlotOffset,
      epochSchedule.warmup,
      epochSchedule.firstNormalEpoch,
      epochSchedule.firstNormalSlot,
    );
  }

  /**
   * Fetch the leader schedule for the current epoch
   * @return {Promise<RpcResponseAndContext<LeaderSchedule>>}
   */
  async getLeaderSchedule(): Promise<LeaderSchedule> {
    const unsafeRes = await this._rpcRequest('getLeaderSchedule', []);
    const res = create(unsafeRes, GetLeaderScheduleRpcResult);
    if ('error' in res) {
      throw new Error('failed to get leader schedule: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch the minimum balance needed to exempt an account of `dataLength`
   * size from rent
   */
  async getMinimumBalanceForRentExemption(
    dataLength: number,
    commitment?: Commitment,
  ): Promise<number> {
    const args = this._buildArgs([dataLength], commitment);
    const unsafeRes = await this._rpcRequest(
      'getMinimumBalanceForRentExemption',
      args,
    );
    const res = create(unsafeRes, GetMinimumBalanceForRentExemptionRpcResult);
    if ('error' in res) {
      console.warn('Unable to fetch minimum balance for rent exemption');
      return 0;
    }
    return res.result;
  }

  /**
   * Fetch a recent blockhash from the cluster, return with context
   * @return {Promise<RpcResponseAndContext<{blockhash: Blockhash, feeCalculator: FeeCalculator}>>}
   */
  async getRecentBlockhashAndContext(
    commitment?: Commitment,
  ): Promise<
    RpcResponseAndContext<{blockhash: Blockhash; feeCalculator: FeeCalculator}>
  > {
    const args = this._buildArgs([], commitment);
    const unsafeRes = await this._rpcRequest('getRecentBlockhash', args);
    const res = create(unsafeRes, GetRecentBlockhashAndContextRpcResult);
    if ('error' in res) {
      throw new Error('failed to get recent blockhash: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch recent performance samples
   * @return {Promise<Array<PerfSample>>}
   */
  async getRecentPerformanceSamples(
    limit?: number,
  ): Promise<Array<PerfSample>> {
    const args = this._buildArgs(limit ? [limit] : []);
    const unsafeRes = await this._rpcRequest(
      'getRecentPerformanceSamples',
      args,
    );
    const res = create(unsafeRes, GetRecentPerformanceSamplesRpcResult);
    if ('error' in res) {
      throw new Error(
        'failed to get recent performance samples: ' + res.error.message,
      );
    }

    return res.result;
  }

  /**
   * Fetch the fee calculator for a recent blockhash from the cluster, return with context
   */
  async getFeeCalculatorForBlockhash(
    blockhash: Blockhash,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<FeeCalculator | null>> {
    const args = this._buildArgs([blockhash], commitment);
    const unsafeRes = await this._rpcRequest(
      'getFeeCalculatorForBlockhash',
      args,
    );

    const res = create(unsafeRes, GetFeeCalculatorRpcResult);
    if ('error' in res) {
      throw new Error('failed to get fee calculator: ' + res.error.message);
    }
    const {context, value} = res.result;
    return {
      context,
      value: value !== null ? value.feeCalculator : null,
    };
  }

  /**
   * Fetch a recent block hash from the ledger, a fee schedule and the last valid block height the block hash is valid for
   */
  async getFees(
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<Fees>> {
    const args = this._buildArgs([], commitment, undefined);
    const unsafeRes = await this._rpcRequest('getFees', args);
    const res = create(unsafeRes, GetFeesRpcResult);
    if ('error' in res) {
      throw new Error('failed to get fees: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch a recent blockhash from the cluster
   * @return {Promise<{blockhash: Blockhash, feeCalculator: FeeCalculator}>}
   */
  async getRecentBlockhash(
    commitment?: Commitment,
  ): Promise<{blockhash: Blockhash; feeCalculator: FeeCalculator}> {
    try {
      const res = await this.getRecentBlockhashAndContext(commitment);
      return res.value;
    } catch (e) {
      throw new Error('failed to get recent blockhash: ' + e);
    }
  }

  /**
   * Fetch the node version
   */
  async getVersion(): Promise<Version> {
    const unsafeRes = await this._rpcRequest('getVersion', []);
    const res = create(unsafeRes, jsonRpcResult(VersionResult));
    if ('error' in res) {
      throw new Error('failed to get version: ' + res.error.message);
    }
    return res.result;
  }

  /**
   * Fetch a processed block from the cluster.
   */
  async getBlock(
    slot: number,
    opts?: {commitment?: Finality},
  ): Promise<BlockResponse | null> {
    const args = this._buildArgsAtLeastConfirmed(
      [slot],
      opts && opts.commitment,
    );
    const unsafeRes = await this._rpcRequest('getConfirmedBlock', args);
    const res = create(unsafeRes, GetConfirmedBlockRpcResult);

    if ('error' in res) {
      throw new Error('failed to get confirmed block: ' + res.error.message);
    }

    const result = res.result;
    if (!result) return result;

    return {
      ...result,
      transactions: result.transactions.map(({transaction, meta}) => {
        const message = new Message(transaction.message);
        return {
          meta,
          transaction: {
            ...transaction,
            message,
          },
        };
      }),
    };
  }

  /**
   * Fetch a processed transaction from the cluster.
   */
  async getTransaction(
    signature: string,
    opts?: {commitment?: Finality},
  ): Promise<TransactionResponse | null> {
    const args = this._buildArgsAtLeastConfirmed(
      [signature],
      opts && opts.commitment,
    );
    const unsafeRes = await this._rpcRequest('getConfirmedTransaction', args);
    const res = create(unsafeRes, GetConfirmedTransactionRpcResult);
    if ('error' in res) {
      throw new Error(
        'failed to get confirmed transaction: ' + res.error.message,
      );
    }

    const result = res.result;
    if (!result) return result;

    return {
      ...result,
      transaction: {
        ...result.transaction,
        message: new Message(result.transaction.message),
      },
    };
  }

  /**
   * Fetch a list of Transactions and transaction statuses from the cluster
   * for a confirmed block.
   *
   * @deprecated Deprecated since v1.13.0. Please use {@link getBlock} instead.
   */
  async getConfirmedBlock(
    slot: number,
    commitment?: Finality,
  ): Promise<ConfirmedBlock> {
    const result = await this.getBlock(slot, {commitment});
    if (!result) {
      throw new Error('Confirmed block ' + slot + ' not found');
    }

    return {
      ...result,
      transactions: result.transactions.map(({transaction, meta}) => {
        return {
          meta,
          transaction: Transaction.populate(
            transaction.message,
            transaction.signatures,
          ),
        };
      }),
    };
  }

  /**
   * Fetch a list of Signatures from the cluster for a confirmed block, excluding rewards
   */
  async getConfirmedBlockSignatures(
    slot: number,
    commitment?: Finality,
  ): Promise<ConfirmedBlockSignatures> {
    const args = this._buildArgsAtLeastConfirmed(
      [slot],
      commitment,
      undefined,
      {
        transactionDetails: 'signatures',
        rewards: false,
      },
    );
    const unsafeRes = await this._rpcRequest('getConfirmedBlock', args);
    const res = create(unsafeRes, GetConfirmedBlockSignaturesRpcResult);
    if ('error' in res) {
      throw new Error('failed to get confirmed block: ' + res.error.message);
    }
    const result = res.result;
    if (!result) {
      throw new Error('Confirmed block ' + slot + ' not found');
    }
    return result;
  }

  /**
   * Fetch a transaction details for a confirmed transaction
   */
  async getConfirmedTransaction(
    signature: TransactionSignature,
    commitment?: Finality,
  ): Promise<ConfirmedTransaction | null> {
    const result = await this.getTransaction(signature, {commitment});
    if (!result) return result;
    const {message, signatures} = result.transaction;
    return {
      ...result,
      transaction: Transaction.populate(message, signatures),
    };
  }

  /**
   * Fetch parsed transaction details for a confirmed transaction
   */
  async getParsedConfirmedTransaction(
    signature: TransactionSignature,
    commitment?: Finality,
  ): Promise<ParsedConfirmedTransaction | null> {
    const args = this._buildArgsAtLeastConfirmed(
      [signature],
      commitment,
      'jsonParsed',
    );
    const unsafeRes = await this._rpcRequest('getConfirmedTransaction', args);
    const res = create(unsafeRes, GetParsedConfirmedTransactionRpcResult);
    if ('error' in res) {
      throw new Error(
        'failed to get confirmed transaction: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch parsed transaction details for a batch of confirmed transactions
   */
  async getParsedConfirmedTransactions(
    signatures: TransactionSignature[],
    commitment?: Finality,
  ): Promise<(ParsedConfirmedTransaction | null)[]> {
    const batch = signatures.map(signature => {
      const args = this._buildArgsAtLeastConfirmed(
        [signature],
        commitment,
        'jsonParsed',
      );
      return {
        methodName: 'getConfirmedTransaction',
        args,
      };
    });

    const unsafeRes = await this._rpcBatchRequest(batch);
    const res = unsafeRes.map((unsafeRes: any) => {
      const res = create(unsafeRes, GetParsedConfirmedTransactionRpcResult);
      if ('error' in res) {
        throw new Error(
          'failed to get confirmed transactions: ' + res.error.message,
        );
      }
      return res.result;
    });

    return res;
  }

  /**
   * Fetch a list of all the confirmed signatures for transactions involving an address
   * within a specified slot range. Max range allowed is 10,000 slots.
   *
   * @deprecated Deprecated since v1.3. Please use {@link getConfirmedSignaturesForAddress2} instead.
   *
   * @param address queried address
   * @param startSlot start slot, inclusive
   * @param endSlot end slot, inclusive
   */
  async getConfirmedSignaturesForAddress(
    address: PublicKey,
    startSlot: number,
    endSlot: number,
  ): Promise<Array<TransactionSignature>> {
    let options: any = {};

    let firstAvailableBlock = await this.getFirstAvailableBlock();
    while (!('until' in options)) {
      startSlot--;
      if (startSlot <= 0 || startSlot < firstAvailableBlock) {
        break;
      }

      try {
        const block = await this.getConfirmedBlockSignatures(
          startSlot,
          'finalized',
        );
        if (block.signatures.length > 0) {
          options.until =
            block.signatures[block.signatures.length - 1].toString();
        }
      } catch (err) {
        if (err.message.includes('skipped')) {
          continue;
        } else {
          throw err;
        }
      }
    }

    let highestConfirmedRoot = await this.getSlot('finalized');
    while (!('before' in options)) {
      endSlot++;
      if (endSlot > highestConfirmedRoot) {
        break;
      }

      try {
        const block = await this.getConfirmedBlockSignatures(endSlot);
        if (block.signatures.length > 0) {
          options.before =
            block.signatures[block.signatures.length - 1].toString();
        }
      } catch (err) {
        if (err.message.includes('skipped')) {
          continue;
        } else {
          throw err;
        }
      }
    }

    const confirmedSignatureInfo = await this.getConfirmedSignaturesForAddress2(
      address,
      options,
    );
    return confirmedSignatureInfo.map(info => info.signature);
  }

  /**
   * Returns confirmed signatures for transactions involving an
   * address backwards in time from the provided signature or most recent confirmed block
   *
   *
   * @param address queried address
   * @param options
   */
  async getConfirmedSignaturesForAddress2(
    address: PublicKey,
    options?: ConfirmedSignaturesForAddress2Options,
    commitment?: Finality,
  ): Promise<Array<ConfirmedSignatureInfo>> {
    const args = this._buildArgsAtLeastConfirmed(
      [address.toBase58()],
      commitment,
      undefined,
      options,
    );
    const unsafeRes = await this._rpcRequest(
      'getConfirmedSignaturesForAddress2',
      args,
    );
    const res = create(unsafeRes, GetConfirmedSignaturesForAddress2RpcResult);
    if ('error' in res) {
      throw new Error(
        'failed to get confirmed signatures for address: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Returns confirmed signatures for transactions involving an
   * address backwards in time from the provided signature or most recent confirmed block
   *
   *
   * @param address queried address
   * @param options
   */
  async getSignaturesForAddress(
    address: PublicKey,
    options?: SignaturesForAddressOptions,
    commitment?: Finality,
  ): Promise<Array<ConfirmedSignatureInfo>> {
    const args = this._buildArgsAtLeastConfirmed(
      [address.toBase58()],
      commitment,
      undefined,
      options,
    );
    const unsafeRes = await this._rpcRequest('getSignaturesForAddress', args);
    const res = create(unsafeRes, GetSignaturesForAddressRpcResult);
    if ('error' in res) {
      throw new Error(
        'failed to get signatures for address: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * Fetch the contents of a Nonce account from the cluster, return with context
   */
  async getNonceAndContext(
    nonceAccount: PublicKey,
    commitment?: Commitment,
  ): Promise<RpcResponseAndContext<NonceAccount | null>> {
    const {context, value: accountInfo} = await this.getAccountInfoAndContext(
      nonceAccount,
      commitment,
    );

    let value = null;
    if (accountInfo !== null) {
      value = NonceAccount.fromAccountData(accountInfo.data);
    }

    return {
      context,
      value,
    };
  }

  /**
   * Fetch the contents of a Nonce account from the cluster
   */
  async getNonce(
    nonceAccount: PublicKey,
    commitment?: Commitment,
  ): Promise<NonceAccount | null> {
    return await this.getNonceAndContext(nonceAccount, commitment)
      .then(x => x.value)
      .catch(e => {
        throw new Error(
          'failed to get nonce for account ' +
            nonceAccount.toBase58() +
            ': ' +
            e,
        );
      });
  }

  /**
   * Request an allocation of lamports to the specified address
   *
   * ```typescript
   * import { Connection, PublicKey, LAMPORTS_PER_SOL } from "@solana/web3.js";
   *
   * (async () => {
   *   const connection = new Connection("https://api.testnet.solana.com", "confirmed");
   *   const myAddress = new PublicKey("2nr1bHFT86W9tGnyvmYW4vcHKsQB3sVQfnddasz4kExM");
   *   const signature = await connection.requestAirdrop(myAddress, LAMPORTS_PER_SOL);
   *   await connection.confirmTransaction(signature);
   * })();
   * ```
   */
  async requestAirdrop(
    to: PublicKey,
    lamports: number,
  ): Promise<TransactionSignature> {
    const unsafeRes = await this._rpcRequest('requestAirdrop', [
      to.toBase58(),
      lamports,
    ]);
    const res = create(unsafeRes, RequestAirdropRpcResult);
    if ('error' in res) {
      throw new Error(
        'airdrop to ' + to.toBase58() + ' failed: ' + res.error.message,
      );
    }
    return res.result;
  }

  /**
   * @internal
   */
  async _recentBlockhash(disableCache: boolean): Promise<Blockhash> {
    if (!disableCache) {
      // Wait for polling to finish
      while (this._pollingBlockhash) {
        await sleep(100);
      }
      const timeSinceFetch = Date.now() - this._blockhashInfo.lastFetch;
      const expired = timeSinceFetch >= BLOCKHASH_CACHE_TIMEOUT_MS;
      if (this._blockhashInfo.recentBlockhash !== null && !expired) {
        return this._blockhashInfo.recentBlockhash;
      }
    }

    return await this._pollNewBlockhash();
  }

  /**
   * @internal
   */
  async _pollNewBlockhash(): Promise<Blockhash> {
    this._pollingBlockhash = true;
    try {
      const startTime = Date.now();
      for (let i = 0; i < 50; i++) {
        const {blockhash} = await this.getRecentBlockhash('finalized');

        if (this._blockhashInfo.recentBlockhash != blockhash) {
          this._blockhashInfo = {
            recentBlockhash: blockhash,
            lastFetch: Date.now(),
            transactionSignatures: [],
            simulatedSignatures: [],
          };
          return blockhash;
        }

        // Sleep for approximately half a slot
        await sleep(MS_PER_SLOT / 2);
      }

      throw new Error(
        `Unable to obtain a new blockhash after ${Date.now() - startTime}ms`,
      );
    } finally {
      this._pollingBlockhash = false;
    }
  }

  /**
   * Simulate a transaction
   */
  async simulateTransaction(
    transaction: Transaction,
    signers?: Array<Signer>,
  ): Promise<RpcResponseAndContext<SimulatedTransactionResponse>> {
    if (transaction.nonceInfo && signers) {
      transaction.sign(...signers);
    } else {
      let disableCache = this._disableBlockhashCaching;
      for (;;) {
        transaction.recentBlockhash = await this._recentBlockhash(disableCache);

        if (!signers) break;

        transaction.sign(...signers);
        if (!transaction.signature) {
          throw new Error('!signature'); // should never happen
        }

        const signature = transaction.signature.toString('base64');
        if (
          !this._blockhashInfo.simulatedSignatures.includes(signature) &&
          !this._blockhashInfo.transactionSignatures.includes(signature)
        ) {
          // The signature of this transaction has not been seen before with the
          // current recentBlockhash, all done. Let's break
          this._blockhashInfo.simulatedSignatures.push(signature);
          break;
        } else {
          // This transaction would be treated as duplicate (its derived signature
          // matched to one of already recorded signatures).
          // So, we must fetch a new blockhash for a different signature by disabling
          // our cache not to wait for the cache expiration (BLOCKHASH_CACHE_TIMEOUT_MS).
          disableCache = true;
        }
      }
    }

    const signData = transaction.serializeMessage();
    const wireTransaction = transaction._serialize(signData);
    const encodedTransaction = wireTransaction.toString('base64');
    const config: any = {
      encoding: 'base64',
      commitment: this.commitment,
    };

    if (signers) {
      config.sigVerify = true;
    }

    const args = [encodedTransaction, config];
    const unsafeRes = await this._rpcRequest('simulateTransaction', args);
    const res = create(unsafeRes, SimulatedTransactionResponseStruct);
    if ('error' in res) {
      let logs;
      if ('data' in res.error) {
        logs = res.error.data.logs;
        if (logs && Array.isArray(logs)) {
          const traceIndent = '\n    ';
          const logTrace = traceIndent + logs.join(traceIndent);
          console.error(res.error.message, logTrace);
        }
      }
      throw new SendTransactionError(
        'failed to simulate transaction: ' + res.error.message,
        logs,
      );
    }
    return res.result;
  }

  /**
   * Sign and send a transaction
   */
  async sendTransaction(
    transaction: Transaction,
    signers: Array<Signer>,
    options?: SendOptions,
  ): Promise<TransactionSignature> {
    if (transaction.nonceInfo) {
      transaction.sign(...signers);
    } else {
      let disableCache = this._disableBlockhashCaching;
      for (;;) {
        transaction.recentBlockhash = await this._recentBlockhash(disableCache);
        transaction.sign(...signers);
        if (!transaction.signature) {
          throw new Error('!signature'); // should never happen
        }

        const signature = transaction.signature.toString('base64');
        if (!this._blockhashInfo.transactionSignatures.includes(signature)) {
          // The signature of this transaction has not been seen before with the
          // current recentBlockhash, all done. Let's break
          this._blockhashInfo.transactionSignatures.push(signature);
          break;
        } else {
          // This transaction would be treated as duplicate (its derived signature
          // matched to one of already recorded signatures).
          // So, we must fetch a new blockhash for a different signature by disabling
          // our cache not to wait for the cache expiration (BLOCKHASH_CACHE_TIMEOUT_MS).
          disableCache = true;
        }
      }
    }

    const wireTransaction = transaction.serialize();
    return await this.sendRawTransaction(wireTransaction, options);
  }

  /**
   * Send a transaction that has already been signed and serialized into the
   * wire format
   */
  async sendRawTransaction(
    rawTransaction: Buffer | Uint8Array | Array<number>,
    options?: SendOptions,
  ): Promise<TransactionSignature> {
    const encodedTransaction = toBuffer(rawTransaction).toString('base64');
    const result = await this.sendEncodedTransaction(
      encodedTransaction,
      options,
    );
    return result;
  }

  /**
   * Send a transaction that has already been signed, serialized into the
   * wire format, and encoded as a base64 string
   */
  async sendEncodedTransaction(
    encodedTransaction: string,
    options?: SendOptions,
  ): Promise<TransactionSignature> {
    const config: any = {encoding: 'base64'};
    const skipPreflight = options && options.skipPreflight;
    const preflightCommitment =
      (options && options.preflightCommitment) || this.commitment;

    if (skipPreflight) {
      config.skipPreflight = skipPreflight;
    }
    if (preflightCommitment) {
      config.preflightCommitment = preflightCommitment;
    }

    const args = [encodedTransaction, config];
    const unsafeRes = await this._rpcRequest('sendTransaction', args);
    const res = create(unsafeRes, SendTransactionRpcResult);
    if ('error' in res) {
      let logs;
      if ('data' in res.error) {
        logs = res.error.data.logs;
        if (logs && Array.isArray(logs)) {
          const traceIndent = '\n    ';
          const logTrace = traceIndent + logs.join(traceIndent);
          console.error(res.error.message, logTrace);
        }
      }
      throw new SendTransactionError(
        'failed to send transaction: ' + res.error.message,
        logs,
      );
    }
    return res.result;
  }

  /**
   * @internal
   */
  _wsOnOpen() {
    this._rpcWebSocketConnected = true;
    this._rpcWebSocketHeartbeat = setInterval(() => {
      // Ping server every 5s to prevent idle timeouts
      this._rpcWebSocket.notify('ping').catch(() => {});
    }, 5000);
    this._updateSubscriptions();
  }

  /**
   * @internal
   */
  _wsOnError(err: Error) {
    console.error('ws error:', err.message);
  }

  /**
   * @internal
   */
  _wsOnClose(code: number) {
    if (this._rpcWebSocketHeartbeat) {
      clearInterval(this._rpcWebSocketHeartbeat);
      this._rpcWebSocketHeartbeat = null;
    }

    if (code === 1000) {
      // explicit close, check if any subscriptions have been made since close
      this._updateSubscriptions();
      return;
    }

    // implicit close, prepare subscriptions for auto-reconnect
    this._resetSubscriptions();
  }

  /**
   * @internal
   */
  async _subscribe(
    sub: {subscriptionId: SubscriptionId | null},
    rpcMethod: string,
    rpcArgs: IWSRequestParams,
  ) {
    if (sub.subscriptionId == null) {
      sub.subscriptionId = 'subscribing';
      try {
        const id = await this._rpcWebSocket.call(rpcMethod, rpcArgs);
        if (typeof id === 'number' && sub.subscriptionId === 'subscribing') {
          // eslint-disable-next-line require-atomic-updates
          sub.subscriptionId = id;
        }
      } catch (err) {
        if (sub.subscriptionId === 'subscribing') {
          // eslint-disable-next-line require-atomic-updates
          sub.subscriptionId = null;
        }
        console.error(`${rpcMethod} error for argument`, rpcArgs, err.message);
      }
    }
  }

  /**
   * @internal
   */
  async _unsubscribe(
    sub: {subscriptionId: SubscriptionId | null},
    rpcMethod: string,
  ) {
    const subscriptionId = sub.subscriptionId;
    if (subscriptionId != null && typeof subscriptionId != 'string') {
      const unsubscribeId: number = subscriptionId;
      try {
        await this._rpcWebSocket.call(rpcMethod, [unsubscribeId]);
      } catch (err) {
        console.error(`${rpcMethod} error:`, err.message);
      }
    }
  }

  /**
   * @internal
   */
  _resetSubscriptions() {
    Object.values(this._accountChangeSubscriptions).forEach(
      s => (s.subscriptionId = null),
    );
    Object.values(this._programAccountChangeSubscriptions).forEach(
      s => (s.subscriptionId = null),
    );
    Object.values(this._rootSubscriptions).forEach(
      s => (s.subscriptionId = null),
    );
    Object.values(this._signatureSubscriptions).forEach(
      s => (s.subscriptionId = null),
    );
    Object.values(this._slotSubscriptions).forEach(
      s => (s.subscriptionId = null),
    );
    Object.values(this._slotUpdateSubscriptions).forEach(
      s => (s.subscriptionId = null),
    );
  }

  /**
   * @internal
   */
  _updateSubscriptions() {
    const accountKeys = Object.keys(this._accountChangeSubscriptions).map(
      Number,
    );
    const programKeys = Object.keys(
      this._programAccountChangeSubscriptions,
    ).map(Number);
    const slotKeys = Object.keys(this._slotSubscriptions).map(Number);
    const slotUpdateKeys = Object.keys(this._slotUpdateSubscriptions).map(
      Number,
    );
    const signatureKeys = Object.keys(this._signatureSubscriptions).map(Number);
    const rootKeys = Object.keys(this._rootSubscriptions).map(Number);
    const logsKeys = Object.keys(this._logsSubscriptions).map(Number);
    if (
      accountKeys.length === 0 &&
      programKeys.length === 0 &&
      slotKeys.length === 0 &&
      slotUpdateKeys.length === 0 &&
      signatureKeys.length === 0 &&
      rootKeys.length === 0 &&
      logsKeys.length === 0
    ) {
      if (this._rpcWebSocketConnected) {
        this._rpcWebSocketConnected = false;
        this._rpcWebSocketIdleTimeout = setTimeout(() => {
          this._rpcWebSocketIdleTimeout = null;
          this._rpcWebSocket.close();
        }, 500);
      }
      return;
    }

    if (this._rpcWebSocketIdleTimeout !== null) {
      clearTimeout(this._rpcWebSocketIdleTimeout);
      this._rpcWebSocketIdleTimeout = null;
      this._rpcWebSocketConnected = true;
    }

    if (!this._rpcWebSocketConnected) {
      this._rpcWebSocket.connect();
      return;
    }

    for (let id of accountKeys) {
      const sub = this._accountChangeSubscriptions[id];
      this._subscribe(
        sub,
        'accountSubscribe',
        this._buildArgs([sub.publicKey], sub.commitment, 'base64'),
      );
    }

    for (let id of programKeys) {
      const sub = this._programAccountChangeSubscriptions[id];
      this._subscribe(
        sub,
        'programSubscribe',
        this._buildArgs([sub.programId], sub.commitment, 'base64', {
          filters: sub.filters,
        }),
      );
    }

    for (let id of slotKeys) {
      const sub = this._slotSubscriptions[id];
      this._subscribe(sub, 'slotSubscribe', []);
    }

    for (let id of slotUpdateKeys) {
      const sub = this._slotUpdateSubscriptions[id];
      this._subscribe(sub, 'slotsUpdatesSubscribe', []);
    }

    for (let id of signatureKeys) {
      const sub = this._signatureSubscriptions[id];
      const args: any[] = [sub.signature];
      if (sub.options) args.push(sub.options);
      this._subscribe(sub, 'signatureSubscribe', args);
    }

    for (let id of rootKeys) {
      const sub = this._rootSubscriptions[id];
      this._subscribe(sub, 'rootSubscribe', []);
    }

    for (let id of logsKeys) {
      const sub = this._logsSubscriptions[id];
      let filter;
      if (typeof sub.filter === 'object') {
        filter = {mentions: [sub.filter.toString()]};
      } else {
        filter = sub.filter;
      }
      this._subscribe(
        sub,
        'logsSubscribe',
        this._buildArgs([filter], sub.commitment),
      );
    }
  }

  /**
   * @internal
   */
  _wsOnAccountNotification(notification: object) {
    const res = create(notification, AccountNotificationResult);
    for (const sub of Object.values(this._accountChangeSubscriptions)) {
      if (sub.subscriptionId === res.subscription) {
        sub.callback(res.result.value, res.result.context);
        return;
      }
    }
  }

  /**
   * Register a callback to be invoked whenever the specified account changes
   *
   * @param publicKey Public key of the account to monitor
   * @param callback Function to invoke whenever the account is changed
   * @param commitment Specify the commitment level account changes must reach before notification
   * @return subscription id
   */
  onAccountChange(
    publicKey: PublicKey,
    callback: AccountChangeCallback,
    commitment?: Commitment,
  ): number {
    const id = ++this._accountChangeSubscriptionCounter;
    this._accountChangeSubscriptions[id] = {
      publicKey: publicKey.toBase58(),
      callback,
      commitment,
      subscriptionId: null,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Deregister an account notification callback
   *
   * @param id subscription id to deregister
   */
  async removeAccountChangeListener(id: number): Promise<void> {
    if (this._accountChangeSubscriptions[id]) {
      const subInfo = this._accountChangeSubscriptions[id];
      delete this._accountChangeSubscriptions[id];
      await this._unsubscribe(subInfo, 'accountUnsubscribe');
      this._updateSubscriptions();
    } else {
      throw new Error(`Unknown account change id: ${id}`);
    }
  }

  /**
   * @internal
   */
  _wsOnProgramAccountNotification(notification: Object) {
    const res = create(notification, ProgramAccountNotificationResult);
    for (const sub of Object.values(this._programAccountChangeSubscriptions)) {
      if (sub.subscriptionId === res.subscription) {
        const {value, context} = res.result;
        sub.callback(
          {
            accountId: value.pubkey,
            accountInfo: value.account,
          },
          context,
        );
        return;
      }
    }
  }

  /**
   * Register a callback to be invoked whenever accounts owned by the
   * specified program change
   *
   * @param programId Public key of the program to monitor
   * @param callback Function to invoke whenever the account is changed
   * @param commitment Specify the commitment level account changes must reach before notification
   * @param filters The program account filters to pass into the RPC method
   * @return subscription id
   */
  onProgramAccountChange(
    programId: PublicKey,
    callback: ProgramAccountChangeCallback,
    commitment?: Commitment,
    filters?: GetProgramAccountsFilter[],
  ): number {
    const id = ++this._programAccountChangeSubscriptionCounter;
    this._programAccountChangeSubscriptions[id] = {
      programId: programId.toBase58(),
      callback,
      commitment,
      subscriptionId: null,
      filters,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Deregister an account notification callback
   *
   * @param id subscription id to deregister
   */
  async removeProgramAccountChangeListener(id: number): Promise<void> {
    if (this._programAccountChangeSubscriptions[id]) {
      const subInfo = this._programAccountChangeSubscriptions[id];
      delete this._programAccountChangeSubscriptions[id];
      await this._unsubscribe(subInfo, 'programUnsubscribe');
      this._updateSubscriptions();
    } else {
      throw new Error(`Unknown program account change id: ${id}`);
    }
  }

  /**
   * Registers a callback to be invoked whenever logs are emitted.
   */
  onLogs(
    filter: LogsFilter,
    callback: LogsCallback,
    commitment?: Commitment,
  ): number {
    const id = ++this._logsSubscriptionCounter;
    this._logsSubscriptions[id] = {
      filter,
      callback,
      commitment,
      subscriptionId: null,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Deregister a logs callback.
   *
   * @param id subscription id to deregister.
   */
  async removeOnLogsListener(id: number): Promise<void> {
    if (!this._logsSubscriptions[id]) {
      throw new Error(`Unknown logs id: ${id}`);
    }
    const subInfo = this._logsSubscriptions[id];
    delete this._logsSubscriptions[id];
    await this._unsubscribe(subInfo, 'logsUnsubscribe');
    this._updateSubscriptions();
  }

  /**
   * @internal
   */
  _wsOnLogsNotification(notification: Object) {
    const res = create(notification, LogsNotificationResult);
    const keys = Object.keys(this._logsSubscriptions).map(Number);
    for (let id of keys) {
      const sub = this._logsSubscriptions[id];
      if (sub.subscriptionId === res.subscription) {
        sub.callback(res.result.value, res.result.context);
        return;
      }
    }
  }

  /**
   * @internal
   */
  _wsOnSlotNotification(notification: Object) {
    const res = create(notification, SlotNotificationResult);
    for (const sub of Object.values(this._slotSubscriptions)) {
      if (sub.subscriptionId === res.subscription) {
        sub.callback(res.result);
        return;
      }
    }
  }

  /**
   * Register a callback to be invoked upon slot changes
   *
   * @param callback Function to invoke whenever the slot changes
   * @return subscription id
   */
  onSlotChange(callback: SlotChangeCallback): number {
    const id = ++this._slotSubscriptionCounter;
    this._slotSubscriptions[id] = {
      callback,
      subscriptionId: null,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Deregister a slot notification callback
   *
   * @param id subscription id to deregister
   */
  async removeSlotChangeListener(id: number): Promise<void> {
    if (this._slotSubscriptions[id]) {
      const subInfo = this._slotSubscriptions[id];
      delete this._slotSubscriptions[id];
      await this._unsubscribe(subInfo, 'slotUnsubscribe');
      this._updateSubscriptions();
    } else {
      throw new Error(`Unknown slot change id: ${id}`);
    }
  }

  /**
   * @internal
   */
  _wsOnSlotUpdatesNotification(notification: Object) {
    const res = create(notification, SlotUpdateNotificationResult);
    for (const sub of Object.values(this._slotUpdateSubscriptions)) {
      if (sub.subscriptionId === res.subscription) {
        sub.callback(res.result);
        return;
      }
    }
  }

  /**
   * Register a callback to be invoked upon slot updates. {@link SlotUpdate}'s
   * may be useful to track live progress of a cluster.
   *
   * @param callback Function to invoke whenever the slot updates
   * @return subscription id
   */
  onSlotUpdate(callback: SlotUpdateCallback): number {
    const id = ++this._slotUpdateSubscriptionCounter;
    this._slotUpdateSubscriptions[id] = {
      callback,
      subscriptionId: null,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Deregister a slot update notification callback
   *
   * @param id subscription id to deregister
   */
  async removeSlotUpdateListener(id: number): Promise<void> {
    if (this._slotUpdateSubscriptions[id]) {
      const subInfo = this._slotUpdateSubscriptions[id];
      delete this._slotUpdateSubscriptions[id];
      await this._unsubscribe(subInfo, 'slotsUpdatesUnsubscribe');
      this._updateSubscriptions();
    } else {
      throw new Error(`Unknown slot update id: ${id}`);
    }
  }

  _buildArgs(
    args: Array<any>,
    override?: Commitment,
    encoding?: 'jsonParsed' | 'base64',
    extra?: any,
  ): Array<any> {
    const commitment = override || this._commitment;
    if (commitment || encoding || extra) {
      let options: any = {};
      if (encoding) {
        options.encoding = encoding;
      }
      if (commitment) {
        options.commitment = commitment;
      }
      if (extra) {
        options = Object.assign(options, extra);
      }
      args.push(options);
    }
    return args;
  }

  /**
   * @internal
   */
  _buildArgsAtLeastConfirmed(
    args: Array<any>,
    override?: Finality,
    encoding?: 'jsonParsed' | 'base64',
    extra?: any,
  ): Array<any> {
    const commitment = override || this._commitment;
    if (commitment && !['confirmed', 'finalized'].includes(commitment)) {
      throw new Error(
        'Using Connection with default commitment: `' +
          this._commitment +
          '`, but method requires at least `confirmed`',
      );
    }
    return this._buildArgs(args, override, encoding, extra);
  }

  /**
   * @internal
   */
  _wsOnSignatureNotification(notification: Object) {
    const res = create(notification, SignatureNotificationResult);
    for (const [id, sub] of Object.entries(this._signatureSubscriptions)) {
      if (sub.subscriptionId === res.subscription) {
        if (res.result.value === 'receivedSignature') {
          sub.callback(
            {
              type: 'received',
            },
            res.result.context,
          );
        } else {
          // Signatures subscriptions are auto-removed by the RPC service so
          // no need to explicitly send an unsubscribe message
          delete this._signatureSubscriptions[Number(id)];
          this._updateSubscriptions();
          sub.callback(
            {
              type: 'status',
              result: res.result.value,
            },
            res.result.context,
          );
        }
        return;
      }
    }
  }

  /**
   * Register a callback to be invoked upon signature updates
   *
   * @param signature Transaction signature string in base 58
   * @param callback Function to invoke on signature notifications
   * @param commitment Specify the commitment level signature must reach before notification
   * @return subscription id
   */
  onSignature(
    signature: TransactionSignature,
    callback: SignatureResultCallback,
    commitment?: Commitment,
  ): number {
    const id = ++this._signatureSubscriptionCounter;
    this._signatureSubscriptions[id] = {
      signature,
      callback: (notification, context) => {
        if (notification.type === 'status') {
          callback(notification.result, context);
        }
      },
      options: {commitment},
      subscriptionId: null,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Register a callback to be invoked when a transaction is
   * received and/or processed.
   *
   * @param signature Transaction signature string in base 58
   * @param callback Function to invoke on signature notifications
   * @param options Enable received notifications and set the commitment
   *   level that signature must reach before notification
   * @return subscription id
   */
  onSignatureWithOptions(
    signature: TransactionSignature,
    callback: SignatureSubscriptionCallback,
    options?: SignatureSubscriptionOptions,
  ): number {
    const id = ++this._signatureSubscriptionCounter;
    this._signatureSubscriptions[id] = {
      signature,
      callback,
      options,
      subscriptionId: null,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Deregister a signature notification callback
   *
   * @param id subscription id to deregister
   */
  async removeSignatureListener(id: number): Promise<void> {
    if (this._signatureSubscriptions[id]) {
      const subInfo = this._signatureSubscriptions[id];
      delete this._signatureSubscriptions[id];
      await this._unsubscribe(subInfo, 'signatureUnsubscribe');
      this._updateSubscriptions();
    } else {
      throw new Error(`Unknown signature result id: ${id}`);
    }
  }

  /**
   * @internal
   */
  _wsOnRootNotification(notification: Object) {
    const res = create(notification, RootNotificationResult);
    for (const sub of Object.values(this._rootSubscriptions)) {
      if (sub.subscriptionId === res.subscription) {
        sub.callback(res.result);
        return;
      }
    }
  }

  /**
   * Register a callback to be invoked upon root changes
   *
   * @param callback Function to invoke whenever the root changes
   * @return subscription id
   */
  onRootChange(callback: RootChangeCallback): number {
    const id = ++this._rootSubscriptionCounter;
    this._rootSubscriptions[id] = {
      callback,
      subscriptionId: null,
    };
    this._updateSubscriptions();
    return id;
  }

  /**
   * Deregister a root notification callback
   *
   * @param id subscription id to deregister
   */
  async removeRootChangeListener(id: number): Promise<void> {
    if (this._rootSubscriptions[id]) {
      const subInfo = this._rootSubscriptions[id];
      delete this._rootSubscriptions[id];
      await this._unsubscribe(subInfo, 'rootUnsubscribe');
      this._updateSubscriptions();
    } else {
      throw new Error(`Unknown root change id: ${id}`);
    }
  }
}
