import bs58 from 'bs58';
import {Buffer} from 'buffer';
import {Token, u64} from '@solana/spl-token';
import {expect, use} from 'chai';
import chaiAsPromised from 'chai-as-promised';

import {
  Account,
  Authorized,
  Connection,
  EpochSchedule,
  SystemProgram,
  Transaction,
  LAMPORTS_PER_SOL,
  Lockup,
  PublicKey,
  StakeProgram,
  sendAndConfirmTransaction,
  Keypair,
} from '../src';
import invariant from '../src/util/assert';
import {DEFAULT_TICKS_PER_SLOT, NUM_TICKS_PER_SECOND} from '../src/timing';
import {MOCK_PORT, url} from './url';
import {
  BLOCKHASH_CACHE_TIMEOUT_MS,
  Commitment,
  EpochInfo,
  InflationGovernor,
  SlotInfo,
} from '../src/connection';
import {sleep} from '../src/util/sleep';
import {
  helpers,
  mockErrorMessage,
  mockErrorResponse,
  mockRpcBatchResponse,
  mockRpcResponse,
  mockServer,
} from './mocks/rpc-http';
import {stubRpcWebSocket, restoreRpcWebSocket} from './mocks/rpc-websockets';
import type {TransactionSignature} from '../src/transaction';
import type {
  SignatureStatus,
  TransactionError,
  KeyedAccountInfo,
} from '../src/connection';

use(chaiAsPromised);

const verifySignatureStatus = (
  status: SignatureStatus | null,
  err?: TransactionError,
): SignatureStatus => {
  if (status === null) {
    expect(status).not.to.be.null;
    throw new Error(); // unreachable
  }

  const expectedErr = err || null;
  expect(status.err).to.eql(expectedErr);
  expect(status.slot).to.be.at.least(0);
  if (expectedErr !== null) return status;

  const confirmations = status.confirmations;
  if (typeof confirmations === 'number') {
    expect(confirmations).to.be.at.least(0);
  } else {
    expect(confirmations).to.be.null;
  }
  return status;
};

describe('Connection', () => {
  let connection: Connection;
  beforeEach(() => {
    connection = new Connection(url);
  });

  if (mockServer) {
    const server = mockServer;
    beforeEach(() => {
      server.start(MOCK_PORT);
      stubRpcWebSocket(connection);
    });

    afterEach(() => {
      server.stop();
      restoreRpcWebSocket(connection);
    });
  }

  if (mockServer) {
    it('should pass HTTP headers to RPC', async () => {
      const headers = {
        Authorization: 'Bearer 123',
      };

      let connection = new Connection(url, {
        httpHeaders: headers,
      });

      await mockRpcResponse({
        method: 'getVersion',
        params: [],
        value: {'solana-core': '0.20.4'},
        withHeaders: headers,
      });

      expect(await connection.getVersion()).to.be.not.null;
    });

    it('should allow middleware to augment request', async () => {
      let connection = new Connection(url, {
        fetchMiddleware: (url, options, fetch) => {
          options.headers = Object.assign(options.headers, {
            Authorization: 'Bearer 123',
          });
          fetch(url, options);
        },
      });

      await mockRpcResponse({
        method: 'getVersion',
        params: [],
        value: {'solana-core': '0.20.4'},
        withHeaders: {
          Authorization: 'Bearer 123',
        },
      });

      expect(await connection.getVersion()).to.be.not.null;
    });
  }

  it('get account info - not found', async () => {
    const account = Keypair.generate();

    await mockRpcResponse({
      method: 'getAccountInfo',
      params: [account.publicKey.toBase58(), {encoding: 'base64'}],
      value: null,
      withContext: true,
    });

    expect(await connection.getAccountInfo(account.publicKey)).to.be.null;

    await mockRpcResponse({
      method: 'getAccountInfo',
      params: [account.publicKey.toBase58(), {encoding: 'jsonParsed'}],
      value: null,
      withContext: true,
    });

    expect((await connection.getParsedAccountInfo(account.publicKey)).value).to
      .be.null;
  });

  it('get multiple accounts info', async () => {
    const account1 = Keypair.generate();
    const account2 = Keypair.generate();

    {
      await helpers.airdrop({
        connection,
        address: account1.publicKey,
        amount: LAMPORTS_PER_SOL,
      });

      await helpers.airdrop({
        connection,
        address: account2.publicKey,
        amount: LAMPORTS_PER_SOL,
      });
    }

    const value = [
      {
        owner: '11111111111111111111111111111111',
        lamports: LAMPORTS_PER_SOL,
        data: ['', 'base64'],
        executable: false,
        rentEpoch: 0,
      },
      {
        owner: '11111111111111111111111111111111',
        lamports: LAMPORTS_PER_SOL,
        data: ['', 'base64'],
        executable: false,
        rentEpoch: 0,
      },
    ];

    await mockRpcResponse({
      method: 'getMultipleAccounts',
      params: [
        [account1.publicKey.toBase58(), account2.publicKey.toBase58()],
        {encoding: 'base64'},
      ],
      value: value,
      withContext: true,
    });

    const res = await connection.getMultipleAccountsInfo(
      [account1.publicKey, account2.publicKey],
      'confirmed',
    );

    const expectedValue = [
      {
        owner: new PublicKey('11111111111111111111111111111111'),
        lamports: LAMPORTS_PER_SOL,
        data: Buffer.from([]),
        executable: false,
        rentEpoch: 0,
      },
      {
        owner: new PublicKey('11111111111111111111111111111111'),
        lamports: LAMPORTS_PER_SOL,
        data: Buffer.from([]),
        executable: false,
        rentEpoch: 0,
      },
    ];

    expect(res).to.eql(expectedValue);
  });

  it('get program accounts', async () => {
    const account0 = Keypair.generate();
    const account1 = Keypair.generate();
    const programId = Keypair.generate();

    {
      await helpers.airdrop({
        connection,
        address: account0.publicKey,
        amount: LAMPORTS_PER_SOL,
      });

      const transaction = new Transaction().add(
        SystemProgram.assign({
          accountPubkey: account0.publicKey,
          programId: programId.publicKey,
        }),
      );

      await helpers.processTransaction({
        connection,
        transaction,
        signers: [account0],
        commitment: 'confirmed',
      });
    }

    {
      await helpers.airdrop({
        connection,
        address: account1.publicKey,
        amount: 0.5 * LAMPORTS_PER_SOL,
      });

      const transaction = new Transaction().add(
        SystemProgram.assign({
          accountPubkey: account1.publicKey,
          programId: programId.publicKey,
        }),
      );

      await helpers.processTransaction({
        connection,
        transaction,
        signers: [account1],
        commitment: 'confirmed',
      });
    }

    const feeCalculator = (await helpers.recentBlockhash({connection}))
      .feeCalculator;

    {
      await mockRpcResponse({
        method: 'getProgramAccounts',
        params: [
          programId.publicKey.toBase58(),
          {commitment: 'confirmed', encoding: 'base64'},
        ],
        value: [
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports: LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account0.publicKey.toBase58(),
          },
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports:
                0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account1.publicKey.toBase58(),
          },
        ],
      });

      const programAccounts = await connection.getProgramAccounts(
        programId.publicKey,
        {
          commitment: 'confirmed',
        },
      );
      expect(programAccounts).to.have.length(2);
      programAccounts.forEach(function (keyedAccount) {
        if (keyedAccount.pubkey.equals(account0.publicKey)) {
          expect(keyedAccount.account.lamports).to.eq(
            LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
          );
        } else {
          expect(keyedAccount.pubkey).to.eql(account1.publicKey);
          expect(keyedAccount.account.lamports).to.eq(
            0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
          );
        }
      });
    }

    {
      await mockRpcResponse({
        method: 'getProgramAccounts',
        params: [
          programId.publicKey.toBase58(),
          {commitment: 'confirmed', encoding: 'base64'},
        ],
        value: [
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports: LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account0.publicKey.toBase58(),
          },
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports:
                0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account1.publicKey.toBase58(),
          },
        ],
      });

      const programAccounts = await connection.getProgramAccounts(
        programId.publicKey,
        'confirmed',
      );
      expect(programAccounts).to.have.length(2);
      programAccounts.forEach(function (keyedAccount) {
        if (keyedAccount.pubkey.equals(account0.publicKey)) {
          expect(keyedAccount.account.lamports).to.eq(
            LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
          );
        } else {
          expect(keyedAccount.pubkey).to.eql(account1.publicKey);
          expect(keyedAccount.account.lamports).to.eq(
            0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
          );
        }
      });
    }

    {
      await mockRpcResponse({
        method: 'getProgramAccounts',
        params: [
          programId.publicKey.toBase58(),
          {
            commitment: 'confirmed',
            encoding: 'base64',
            filters: [
              {
                dataSize: 0,
              },
            ],
          },
        ],
        value: [
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports: LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account0.publicKey.toBase58(),
          },
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports:
                0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account1.publicKey.toBase58(),
          },
        ],
      });

      const programAccountsDoMatchFilter = await connection.getProgramAccounts(
        programId.publicKey,
        {
          commitment: 'confirmed',
          encoding: 'base64',
          filters: [{dataSize: 0}],
        },
      );
      expect(programAccountsDoMatchFilter).to.have.length(2);
    }

    {
      await mockRpcResponse({
        method: 'getProgramAccounts',
        params: [
          programId.publicKey.toBase58(),
          {
            commitment: 'confirmed',
            encoding: 'base64',
            filters: [
              {
                memcmp: {
                  offset: 0,
                  bytes: 'XzdZ3w',
                },
              },
            ],
          },
        ],
        value: [],
      });

      const programAccountsDontMatchFilter =
        await connection.getProgramAccounts(programId.publicKey, {
          commitment: 'confirmed',
          filters: [
            {
              memcmp: {
                offset: 0,
                bytes: 'XzdZ3w',
              },
            },
          ],
        });
      expect(programAccountsDontMatchFilter).to.have.length(0);
    }

    {
      await mockRpcResponse({
        method: 'getProgramAccounts',
        params: [
          programId.publicKey.toBase58(),
          {commitment: 'confirmed', encoding: 'jsonParsed'},
        ],
        value: [
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports: LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account0.publicKey.toBase58(),
          },
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports:
                0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account1.publicKey.toBase58(),
          },
        ],
      });

      const programAccounts = await connection.getParsedProgramAccounts(
        programId.publicKey,
        {
          commitment: 'confirmed',
        },
      );
      expect(programAccounts).to.have.length(2);

      programAccounts.forEach(function (element) {
        if (element.pubkey.equals(account0.publicKey)) {
          expect(element.account.lamports).to.eq(
            LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
          );
        } else {
          expect(element.pubkey).to.eql(account1.publicKey);
          expect(element.account.lamports).to.eq(
            0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
          );
        }
      });
    }

    {
      await mockRpcResponse({
        method: 'getProgramAccounts',
        params: [
          programId.publicKey.toBase58(),
          {
            commitment: 'confirmed',
            encoding: 'jsonParsed',
            filters: [
              {
                dataSize: 2,
              },
            ],
          },
        ],
        value: [],
      });

      const programAccountsDontMatchFilter =
        await connection.getParsedProgramAccounts(programId.publicKey, {
          commitment: 'confirmed',
          filters: [{dataSize: 2}],
        });
      expect(programAccountsDontMatchFilter).to.have.length(0);
    }

    {
      await mockRpcResponse({
        method: 'getProgramAccounts',
        params: [
          programId.publicKey.toBase58(),
          {
            commitment: 'confirmed',
            encoding: 'jsonParsed',
            filters: [
              {
                memcmp: {
                  offset: 0,
                  bytes: '',
                },
              },
            ],
          },
        ],
        value: [
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports: LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account0.publicKey.toBase58(),
          },
          {
            account: {
              data: ['', 'base64'],
              executable: false,
              lamports:
                0.5 * LAMPORTS_PER_SOL - feeCalculator.lamportsPerSignature,
              owner: programId.publicKey.toBase58(),
              rentEpoch: 20,
            },
            pubkey: account1.publicKey.toBase58(),
          },
        ],
      });

      const programAccountsDoMatchFilter =
        await connection.getParsedProgramAccounts(programId.publicKey, {
          commitment: 'confirmed',
          filters: [
            {
              memcmp: {
                offset: 0,
                bytes: '',
              },
            },
          ],
        });
      expect(programAccountsDoMatchFilter).to.have.length(2);
    }
  });

  it('get balance', async () => {
    const account = Keypair.generate();

    await mockRpcResponse({
      method: 'getBalance',
      params: [account.publicKey.toBase58()],
      value: {
        context: {
          slot: 11,
        },
        value: 0,
      },
    });

    const balance = await connection.getBalance(account.publicKey);
    expect(balance).to.be.at.least(0);
  });

  it('get inflation', async () => {
    await mockRpcResponse({
      method: 'getInflationGovernor',
      params: [],
      value: {
        foundation: 0.05,
        foundationTerm: 7.0,
        initial: 0.15,
        taper: 0.15,
        terminal: 0.015,
      },
    });

    const inflation = await connection.getInflationGovernor();
    const inflationKeys: (keyof InflationGovernor)[] = [
      'initial',
      'terminal',
      'taper',
      'foundation',
      'foundationTerm',
    ];

    for (const key of inflationKeys) {
      expect(inflation).to.have.property(key);
      expect(inflation[key]).to.be.greaterThan(0);
    }
  });

  it('get inflation reward', async () => {
    if (mockServer) {
      await mockRpcResponse({
        method: 'getInflationReward',
        params: [
          [
            '7GHnTRB8Rz14qZQhDXf8ox1Kfu7mPcPLpKaBJJirmYj2',
            'CrinLuHjVGDDcQfrEoCmM4k31Ni9sMoTCEEvNSUSh7Jg',
          ],
          {
            epoch: 0,
          },
        ],
        value: [
          {
            amount: 3646143,
            effectiveSlot: 432000,
            epoch: 0,
            postBalance: 30504783,
          },
          null,
        ],
      });

      const inflationReward = await connection.getInflationReward(
        [
          new PublicKey('7GHnTRB8Rz14qZQhDXf8ox1Kfu7mPcPLpKaBJJirmYj2'),
          new PublicKey('CrinLuHjVGDDcQfrEoCmM4k31Ni9sMoTCEEvNSUSh7Jg'),
        ],
        0,
      );

      expect(inflationReward).to.have.lengthOf(2);
    }
  });

  it('get epoch info', async () => {
    await mockRpcResponse({
      method: 'getEpochInfo',
      params: [{commitment: 'confirmed'}],
      value: {
        epoch: 0,
        slotIndex: 1,
        slotsInEpoch: 8192,
        absoluteSlot: 1,
        blockHeight: 1,
      },
    });

    const epochInfo = await connection.getEpochInfo('confirmed');
    const epochInfoKeys: (keyof EpochInfo)[] = [
      'epoch',
      'slotIndex',
      'slotsInEpoch',
      'absoluteSlot',
      'blockHeight',
    ];

    for (const key of epochInfoKeys) {
      expect(epochInfo).to.have.property(key);
      expect(epochInfo[key]).to.be.at.least(0);
    }
  });

  it('get epoch schedule', async () => {
    await mockRpcResponse({
      method: 'getEpochSchedule',
      params: [],
      value: {
        firstNormalEpoch: 8,
        firstNormalSlot: 8160,
        leaderScheduleSlotOffset: 8192,
        slotsPerEpoch: 8192,
        warmup: true,
      },
    });

    const epochSchedule = await connection.getEpochSchedule();
    const epochScheduleKeys: (keyof EpochSchedule)[] = [
      'firstNormalEpoch',
      'firstNormalSlot',
      'leaderScheduleSlotOffset',
      'slotsPerEpoch',
    ];

    for (const key of epochScheduleKeys) {
      expect(epochSchedule).to.have.property('warmup');
      expect(epochSchedule).to.have.property(key);
      if (epochSchedule.warmup) {
        expect(epochSchedule[key]).to.be.greaterThan(0);
      }
    }
  });

  it('get leader schedule', async () => {
    await mockRpcResponse({
      method: 'getLeaderSchedule',
      params: [],
      value: {
        '123vij84ecQEKUvQ7gYMKxKwKF6PbYSzCzzURYA4xULY': [0, 1, 2, 3],
        '8PTjAikKoAybKXcEPnDSoy8wSNNikUBJ1iKawJKQwXnB': [4, 5, 6, 7],
      },
    });

    const leaderSchedule = await connection.getLeaderSchedule();
    expect(Object.keys(leaderSchedule).length).to.be.at.least(1);
    for (const key in leaderSchedule) {
      const slots = leaderSchedule[key];
      expect(Array.isArray(slots)).to.be.true;
      expect(slots.length).to.be.at.least(4);
    }
  });

  it('get slot', async () => {
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 123,
    });

    const slot = await connection.getSlot();
    if (mockServer) {
      expect(slot).to.eq(123);
    } else {
      // No idea what the correct slot value should be on a live cluster, so
      // just check the type
      expect(typeof slot).to.eq('number');
    }
  });

  it('get slot leader', async () => {
    await mockRpcResponse({
      method: 'getSlotLeader',
      params: [],
      value: '11111111111111111111111111111111',
    });

    const slotLeader = await connection.getSlotLeader();
    if (mockServer) {
      expect(slotLeader).to.eq('11111111111111111111111111111111');
    } else {
      // No idea what the correct slotLeader value should be on a live cluster, so
      // just check the type
      expect(typeof slotLeader).to.eq('string');
    }
  });

  it('get slot leaders', async () => {
    await mockRpcResponse({
      method: 'getSlotLeaders',
      params: [0, 1],
      value: ['11111111111111111111111111111111'],
    });

    const slotLeaders = await connection.getSlotLeaders(0, 1);
    expect(slotLeaders).to.have.length(1);
    expect(slotLeaders[0]).to.be.instanceOf(PublicKey);
  });

  it('get cluster nodes', async () => {
    await mockRpcResponse({
      method: 'getClusterNodes',
      params: [],
      value: [
        {
          pubkey: '11111111111111111111111111111111',
          gossip: '127.0.0.0:1234',
          tpu: '127.0.0.0:1235',
          rpc: null,
          version: '1.1.10',
        },
      ],
    });

    const clusterNodes = await connection.getClusterNodes();
    if (mockServer) {
      expect(clusterNodes).to.have.length(1);
      expect(clusterNodes[0].pubkey).to.eq('11111111111111111111111111111111');
      expect(typeof clusterNodes[0].gossip).to.eq('string');
      expect(typeof clusterNodes[0].tpu).to.eq('string');
      expect(clusterNodes[0].rpc).to.be.null;
    } else {
      // There should be at least one node (the node that we're talking to)
      expect(clusterNodes.length).to.be.greaterThan(0);
    }
  });

  if (process.env.TEST_LIVE) {
    it('get vote accounts', async () => {
      const voteAccounts = await connection.getVoteAccounts();
      expect(
        voteAccounts.current.concat(voteAccounts.delinquent).length,
      ).to.be.greaterThan(0);
    });
  }

  it('confirm transaction - error', async () => {
    const badTransactionSignature = 'bad transaction signature';

    await expect(
      connection.confirmTransaction(badTransactionSignature),
    ).to.be.rejectedWith('signature must be base58 encoded');

    await mockRpcResponse({
      method: 'getSignatureStatuses',
      params: [[badTransactionSignature]],
      error: mockErrorResponse,
    });

    await expect(
      connection.getSignatureStatus(badTransactionSignature),
    ).to.be.rejectedWith(mockErrorMessage);
  });

  it('get transaction count', async () => {
    await mockRpcResponse({
      method: 'getTransactionCount',
      params: [],
      value: 1000000,
    });

    const count = await connection.getTransactionCount();
    expect(count).to.be.at.least(0);
  });

  it('get total supply', async () => {
    await mockRpcResponse({
      method: 'getSupply',
      params: [],
      value: {
        total: 1000000,
        circulating: 100000,
        nonCirculating: 900000,
        nonCirculatingAccounts: [Keypair.generate().publicKey.toBase58()],
      },
      withContext: true,
    });

    const count = await connection.getTotalSupply();
    expect(count).to.be.at.least(0);
  });

  it('get minimum balance for rent exemption', async () => {
    await mockRpcResponse({
      method: 'getMinimumBalanceForRentExemption',
      params: [512],
      value: 1000000,
    });

    const count = await connection.getMinimumBalanceForRentExemption(512);
    expect(count).to.be.at.least(0);
  });

  it('get confirmed signatures for address', async () => {
    const connection = new Connection(url);

    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [1],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [
          {
            meta: {
              fee: 10000,
              postBalances: [499260347380, 15298080, 1, 1, 1],
              preBalances: [499260357380, 15298080, 1, 1, 1],
              status: {Ok: null},
              err: null,
            },
            transaction: {
              message: {
                accountKeys: [
                  'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
                  'SysvarS1otHashes111111111111111111111111111',
                  'SysvarC1ock11111111111111111111111111111111',
                  'Vote111111111111111111111111111111111111111',
                ],
                header: {
                  numReadonlySignedAccounts: 0,
                  numReadonlyUnsignedAccounts: 3,
                  numRequiredSignatures: 2,
                },
                instructions: [
                  {
                    accounts: [1, 2, 3],
                    data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                    programIdIndex: 4,
                  },
                ],
                recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
              },
              signatures: [
                'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
                '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
              ],
            },
          },
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let slot = 0;
    let address: PublicKey | undefined;
    let expectedSignature: string | undefined;
    while (!address || !expectedSignature) {
      slot++;
      const block = await connection.getConfirmedBlock(slot);
      if (block.transactions.length > 0) {
        const {signature, publicKey} =
          block.transactions[0].transaction.signatures[0];
        if (signature) {
          address = publicKey;
          expectedSignature = bs58.encode(signature);
        }
      }
    }

    // getConfirmedSignaturesForAddress tests...
    await mockRpcResponse({
      method: 'getFirstAvailableBlock',
      params: [],
      value: 0,
    });
    const mockSignature =
      '5SHZ9NwpnS9zYnauN7pnuborKf39zGMr11XpMC59VvRSeDJNcnYLecmdxXCVuBFPNQLdCBBjyZiNCL4KoHKr3tvz';
    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [slot, {transactionDetails: 'signatures', rewards: false}],
      value: {
        blockTime: 1614281964,
        blockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 1,
        signatures: [mockSignature],
      },
    });
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 123,
    });
    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [slot + 2, {transactionDetails: 'signatures', rewards: false}],
      value: {
        blockTime: 1614281964,
        blockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 1,
        signatures: [mockSignature],
      },
    });
    await mockRpcResponse({
      method: 'getConfirmedSignaturesForAddress2',
      params: [address.toBase58(), {before: mockSignature}],
      value: [
        {
          signature: expectedSignature,
          slot,
          err: null,
          memo: null,
        },
      ],
    });

    const confirmedSignatures =
      await connection.getConfirmedSignaturesForAddress(
        address,
        slot,
        slot + 1,
      );
    expect(confirmedSignatures.includes(expectedSignature)).to.be.true;

    const badSlot = Number.MAX_SAFE_INTEGER - 1;
    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [badSlot - 1, {transactionDetails: 'signatures', rewards: false}],
      error: {message: 'Block not available for slot ' + badSlot},
    });
    expect(
      connection.getConfirmedSignaturesForAddress(
        address,
        badSlot,
        badSlot + 1,
      ),
    ).to.be.rejected;

    // getConfirmedSignaturesForAddress2 tests...
    await mockRpcResponse({
      method: 'getConfirmedSignaturesForAddress2',
      params: [address.toBase58(), {limit: 1}],
      value: [
        {
          signature: expectedSignature,
          slot,
          err: null,
          memo: null,
        },
      ],
    });

    const confirmedSignatures2 =
      await connection.getConfirmedSignaturesForAddress2(address, {limit: 1});
    expect(confirmedSignatures2).to.have.length(1);
    if (mockServer) {
      expect(confirmedSignatures2[0].signature).to.eq(expectedSignature);
      expect(confirmedSignatures2[0].slot).to.eq(slot);
      expect(confirmedSignatures2[0].err).to.be.null;
      expect(confirmedSignatures2[0].memo).to.be.null;
    }
  });

  it('get signatures for address', async () => {
    const connection = new Connection(url);

    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [1],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [
          {
            meta: {
              fee: 10000,
              postBalances: [499260347380, 15298080, 1, 1, 1],
              preBalances: [499260357380, 15298080, 1, 1, 1],
              status: {Ok: null},
              err: null,
            },
            transaction: {
              message: {
                accountKeys: [
                  'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
                  'SysvarS1otHashes111111111111111111111111111',
                  'SysvarC1ock11111111111111111111111111111111',
                  'Vote111111111111111111111111111111111111111',
                ],
                header: {
                  numReadonlySignedAccounts: 0,
                  numReadonlyUnsignedAccounts: 3,
                  numRequiredSignatures: 2,
                },
                instructions: [
                  {
                    accounts: [1, 2, 3],
                    data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                    programIdIndex: 4,
                  },
                ],
                recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
              },
              signatures: [
                'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
                '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
              ],
            },
          },
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let slot = 0;
    let address: PublicKey | undefined;
    let expectedSignature: string | undefined;
    while (!address || !expectedSignature) {
      slot++;
      const block = await connection.getConfirmedBlock(slot);
      if (block.transactions.length > 0) {
        const {signature, publicKey} =
          block.transactions[0].transaction.signatures[0];
        if (signature) {
          address = publicKey;
          expectedSignature = bs58.encode(signature);
        }
      }
    }

    // getSignaturesForAddress tests...
    await mockRpcResponse({
      method: 'getSignaturesForAddress',
      params: [address.toBase58(), {limit: 1}],
      value: [
        {
          signature: expectedSignature,
          slot,
          err: null,
          memo: null,
        },
      ],
    });

    const signatures = await connection.getSignaturesForAddress(address, {
      limit: 1,
    });
    expect(signatures).to.have.length(1);
    if (mockServer) {
      expect(signatures[0].signature).to.eq(expectedSignature);
      expect(signatures[0].slot).to.eq(slot);
      expect(signatures[0].err).to.be.null;
      expect(signatures[0].memo).to.be.null;
    }
  });

  it('get parsed confirmed transactions', async () => {
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [1],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [
          {
            meta: {
              fee: 10000,
              postBalances: [499260347380, 15298080, 1, 1, 1],
              preBalances: [499260357380, 15298080, 1, 1, 1],
              status: {Ok: null},
              err: null,
            },
            transaction: {
              message: {
                accountKeys: [
                  'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
                  'SysvarS1otHashes111111111111111111111111111',
                  'SysvarC1ock11111111111111111111111111111111',
                  'Vote111111111111111111111111111111111111111',
                ],
                header: {
                  numReadonlySignedAccounts: 0,
                  numReadonlyUnsignedAccounts: 3,
                  numRequiredSignatures: 2,
                },
                instructions: [
                  {
                    accounts: [1, 2, 3],
                    data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                    programIdIndex: 4,
                  },
                ],
                recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
              },
              signatures: [
                'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
                '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
              ],
            },
          },
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let slot = 0;
    let confirmedTransaction: string | undefined;
    while (!confirmedTransaction) {
      slot++;
      const block = await connection.getConfirmedBlock(slot);
      for (const tx of block.transactions) {
        if (tx.transaction.signature) {
          confirmedTransaction = bs58.encode(tx.transaction.signature);
        }
      }
    }

    await mockRpcBatchResponse({
      batch: [
        {
          methodName: 'getConfirmedTransaction',
          args: [],
        },
      ],
      result: [
        {
          blockTime: 1616102519,
          meta: {
            err: null,
            fee: 5000,
            innerInstructions: [],
            logMessages: [
              'Program Vote111111111111111111111111111111111111111 invoke [1]',
              'Program Vote111111111111111111111111111111111111111 success',
            ],
            postBalances: [499999995000, 26858640, 1, 1, 1],
            postTokenBalances: [],
            preBalances: [500000000000, 26858640, 1, 1, 1],
            preTokenBalances: [],
            status: {
              Ok: null,
            },
          },
          slot: 2,
          transaction: {
            message: {
              accountKeys: [
                {
                  pubkey: 'jcU4R7JccGEvDpe1i6bahvHpe47XahMXacG73EzE198',
                  signer: true,
                  writable: true,
                },
                {
                  pubkey: 'GfBcnCAU7kWfAYqKRCNyWEHjdEJZmzRZvEcX5bbzEQqt',
                  signer: false,
                  writable: true,
                },
                {
                  pubkey: 'SysvarS1otHashes111111111111111111111111111',
                  signer: false,
                  writable: false,
                },
                {
                  pubkey: 'SysvarC1ock11111111111111111111111111111111',
                  signer: false,
                  writable: false,
                },
                {
                  pubkey: 'Vote111111111111111111111111111111111111111',
                  signer: false,
                  writable: false,
                },
              ],
              instructions: [
                {
                  parsed: {
                    info: {
                      clockSysvar:
                        'SysvarC1ock11111111111111111111111111111111',
                      slotHashesSysvar:
                        'SysvarS1otHashes111111111111111111111111111',
                      vote: {
                        hash: 'GuCya3AAGxn1qhoqxqy3WEdZdZUkXKpa9pthQ3tqvbpx',
                        slots: [1],
                        timestamp: 1616102669,
                      },
                      voteAccount:
                        'GfBcnCAU7kWfAYqKRCNyWEHjdEJZmzRZvEcX5bbzEQqt',
                      voteAuthority:
                        'jcU4R7JccGEvDpe1i6bahvHpe47XahMXacG73EzE198',
                    },
                    type: 'vote',
                  },
                  program: 'vote',
                  programId: 'Vote111111111111111111111111111111111111111',
                },
              ],
              recentBlockhash: 'G9ywjV5CVgMtLXruXtrE7af4QgFKYNXgDTw4jp7SWcSo',
            },
            signatures: [
              '4G4rTqnUdzrmBHsdKJSiMtonpQLWSw1avJ8YxWQ95jE6iFFHFsEkBnoYycxnkBS9xHWRc6EarDsrFG9USFBbjfjx',
            ],
          },
        },
        {
          blockTime: 1616102519,
          meta: {
            err: null,
            fee: 5000,
            innerInstructions: [],
            logMessages: [
              'Program Vote111111111111111111111111111111111111111 invoke [1]',
              'Program Vote111111111111111111111111111111111111111 success',
            ],
            postBalances: [499999995000, 26858640, 1, 1, 1],
            postTokenBalances: [],
            preBalances: [500000000000, 26858640, 1, 1, 1],
            preTokenBalances: [],
            status: {
              Ok: null,
            },
          },
          slot: 2,
          transaction: {
            message: {
              accountKeys: [
                {
                  pubkey: 'jcU4R7JccGEvDpe1i6bahvHpe47XahMXacG73EzE198',
                  signer: true,
                  writable: true,
                },
                {
                  pubkey: 'GfBcnCAU7kWfAYqKRCNyWEHjdEJZmzRZvEcX5bbzEQqt',
                  signer: false,
                  writable: true,
                },
                {
                  pubkey: 'SysvarS1otHashes111111111111111111111111111',
                  signer: false,
                  writable: false,
                },
                {
                  pubkey: 'SysvarC1ock11111111111111111111111111111111',
                  signer: false,
                  writable: false,
                },
                {
                  pubkey: 'Vote111111111111111111111111111111111111111',
                  signer: false,
                  writable: false,
                },
              ],
              instructions: [
                {
                  parsed: {
                    info: {
                      clockSysvar:
                        'SysvarC1ock11111111111111111111111111111111',
                      slotHashesSysvar:
                        'SysvarS1otHashes111111111111111111111111111',
                      vote: {
                        hash: 'GuCya3AAGxn1qhoqxqy3WEdZdZUkXKpa9pthQ3tqvbpx',
                        slots: [1],
                        timestamp: 1616102669,
                      },
                      voteAccount:
                        'GfBcnCAU7kWfAYqKRCNyWEHjdEJZmzRZvEcX5bbzEQqt',
                      voteAuthority:
                        'jcU4R7JccGEvDpe1i6bahvHpe47XahMXacG73EzE198',
                    },
                    type: 'vote',
                  },
                  program: 'vote',
                  programId: 'Vote111111111111111111111111111111111111111',
                },
              ],
              recentBlockhash: 'G9ywjV5CVgMtLXruXtrE7af4QgFKYNXgDTw4jp7SWcSo',
            },
            signatures: [
              '4G4rTqnUdzrmBHsdKJSiMtonpQLWSw1avJ8YxWQ95jE6iFFHFsEkBnoYycxnkBS9xHWRc6EarDsrFG9USFBbjfjx',
            ],
          },
        },
      ],
    });

    let result = await connection.getParsedConfirmedTransactions([
      confirmedTransaction,
      confirmedTransaction,
    ]);

    if (!result) {
      expect(result).to.be.ok;
      return;
    }

    expect(result).to.be.length(2);
    expect(result[0]).to.not.be.null;
    expect(result[1]).to.not.be.null;
    if (result[0] !== null) {
      expect(result[0].transaction.signatures).not.to.be.null;
    }
    if (result[1] !== null) {
      expect(result[1].transaction.signatures).not.to.be.null;
    }

    result = await connection.getParsedConfirmedTransactions([]);
    if (!result) {
      expect(result).to.be.ok;
      return;
    }

    expect(result).to.be.empty;
  });

  it('get transaction', async () => {
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [1],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [
          {
            meta: {
              fee: 10000,
              postBalances: [499260347380, 15298080, 1, 1, 1],
              preBalances: [499260357380, 15298080, 1, 1, 1],
              status: {Ok: null},
              err: null,
            },
            transaction: {
              message: {
                accountKeys: [
                  'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
                  'SysvarS1otHashes111111111111111111111111111',
                  'SysvarC1ock11111111111111111111111111111111',
                  'Vote111111111111111111111111111111111111111',
                ],
                header: {
                  numReadonlySignedAccounts: 0,
                  numReadonlyUnsignedAccounts: 3,
                  numRequiredSignatures: 2,
                },
                instructions: [
                  {
                    accounts: [1, 2, 3],
                    data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                    programIdIndex: 4,
                  },
                ],
                recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
              },
              signatures: [
                'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
                '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
              ],
            },
          },
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let slot = 0;
    let confirmedTransaction: string | undefined;
    while (!confirmedTransaction) {
      slot++;
      const block = await connection.getBlock(slot);
      if (block && block.transactions.length > 0) {
        confirmedTransaction = block.transactions[0].transaction.signatures[0];
      }
    }

    await mockRpcResponse({
      method: 'getConfirmedTransaction',
      params: [confirmedTransaction],
      value: {
        slot,
        transaction: {
          message: {
            accountKeys: [
              'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
              '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
              'SysvarS1otHashes111111111111111111111111111',
              'SysvarC1ock11111111111111111111111111111111',
              'Vote111111111111111111111111111111111111111',
            ],
            header: {
              numReadonlySignedAccounts: 0,
              numReadonlyUnsignedAccounts: 3,
              numRequiredSignatures: 2,
            },
            instructions: [
              {
                accounts: [1, 2, 3],
                data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                programIdIndex: 4,
              },
            ],
            recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
          },
          signatures: [
            'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
            '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
          ],
        },
        meta: {
          fee: 10000,
          postBalances: [499260347380, 15298080, 1, 1, 1],
          preBalances: [499260357380, 15298080, 1, 1, 1],
          status: {Ok: null},
          err: null,
        },
      },
    });

    const result = await connection.getTransaction(confirmedTransaction);

    if (!result) {
      expect(result).to.be.ok;
      return;
    }

    const resultSignature = result.transaction.signatures[0];
    expect(resultSignature).to.eq(confirmedTransaction);

    const newAddress = Keypair.generate().publicKey;
    const recentSignature = await helpers.airdrop({
      connection,
      address: newAddress,
      amount: 1,
    });

    await mockRpcResponse({
      method: 'getConfirmedTransaction',
      params: [recentSignature],
      value: null,
    });

    // Signature hasn't been finalized yet
    const nullResponse = await connection.getTransaction(recentSignature);
    expect(nullResponse).to.be.null;
  });

  it('get confirmed transaction', async () => {
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [1],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [
          {
            meta: {
              fee: 10000,
              postBalances: [499260347380, 15298080, 1, 1, 1],
              preBalances: [499260357380, 15298080, 1, 1, 1],
              status: {Ok: null},
              err: null,
            },
            transaction: {
              message: {
                accountKeys: [
                  'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
                  'SysvarS1otHashes111111111111111111111111111',
                  'SysvarC1ock11111111111111111111111111111111',
                  'Vote111111111111111111111111111111111111111',
                ],
                header: {
                  numReadonlySignedAccounts: 0,
                  numReadonlyUnsignedAccounts: 3,
                  numRequiredSignatures: 2,
                },
                instructions: [
                  {
                    accounts: [1, 2, 3],
                    data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                    programIdIndex: 4,
                  },
                ],
                recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
              },
              signatures: [
                'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
                '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
              ],
            },
          },
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let slot = 0;
    let confirmedTransaction: string | undefined;
    while (!confirmedTransaction) {
      slot++;
      const block = await connection.getConfirmedBlock(slot);
      for (const tx of block.transactions) {
        if (tx.transaction.signature) {
          confirmedTransaction = bs58.encode(tx.transaction.signature);
        }
      }
    }

    await mockRpcResponse({
      method: 'getConfirmedTransaction',
      params: [confirmedTransaction],
      value: {
        slot,
        transaction: {
          message: {
            accountKeys: [
              'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
              '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
              'SysvarS1otHashes111111111111111111111111111',
              'SysvarC1ock11111111111111111111111111111111',
              'Vote111111111111111111111111111111111111111',
            ],
            header: {
              numReadonlySignedAccounts: 0,
              numReadonlyUnsignedAccounts: 3,
              numRequiredSignatures: 2,
            },
            instructions: [
              {
                accounts: [1, 2, 3],
                data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                programIdIndex: 4,
              },
            ],
            recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
          },
          signatures: [
            'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
            '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
          ],
        },
        meta: {
          fee: 10000,
          postBalances: [499260347380, 15298080, 1, 1, 1],
          preBalances: [499260357380, 15298080, 1, 1, 1],
          status: {Ok: null},
          err: null,
        },
      },
    });

    const result = await connection.getConfirmedTransaction(
      confirmedTransaction,
    );

    if (!result) {
      expect(result).to.be.ok;
      return;
    }

    if (result.transaction.signature === null) {
      expect(result.transaction.signature).not.to.be.null;
      return;
    }

    const resultSignature = bs58.encode(result.transaction.signature);
    expect(resultSignature).to.eq(confirmedTransaction);

    const newAddress = Keypair.generate().publicKey;
    const recentSignature = await helpers.airdrop({
      connection,
      address: newAddress,
      amount: 1,
    });

    await mockRpcResponse({
      method: 'getConfirmedTransaction',
      params: [recentSignature],
      value: null,
    });

    // Signature hasn't been finalized yet
    const nullResponse = await connection.getConfirmedTransaction(
      recentSignature,
    );
    expect(nullResponse).to.be.null;
  });

  if (mockServer) {
    it('get parsed confirmed transaction coerces public keys of inner instructions', async () => {
      const confirmedTransaction: TransactionSignature =
        '4ADvAUQYxkh4qWKYE9QLW8gCLomGG94QchDLG4quvpBz1WqARYvzWQDDitKduAKspuy1DjcbnaDAnCAfnKpJYs48';

      function getMockData(inner: any) {
        return {
          slot: 353050305,
          transaction: {
            message: {
              accountKeys: [
                {
                  pubkey: 'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  signer: true,
                  writable: true,
                },
              ],
              instructions: [
                {
                  accounts: ['va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf'],
                  data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                  programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                },
              ],
              recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
            },
            signatures: [
              'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
              '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
            ],
          },
          meta: {
            fee: 10000,
            postBalances: [499260347380, 15298080, 1, 1, 1],
            preBalances: [499260357380, 15298080, 1, 1, 1],
            innerInstructions: [
              {
                index: 0,
                instructions: [inner],
              },
            ],
            status: {Ok: null},
            err: null,
          },
        };
      }

      await mockRpcResponse({
        method: 'getConfirmedTransaction',
        params: [confirmedTransaction, {encoding: 'jsonParsed'}],
        value: getMockData({
          parsed: {},
          program: 'spl-token',
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        }),
      });

      const result = await connection.getParsedConfirmedTransaction(
        confirmedTransaction,
      );

      if (result && result.meta && result.meta.innerInstructions) {
        const innerInstructions = result.meta.innerInstructions;
        const firstIx = innerInstructions[0].instructions[0];
        expect(firstIx.programId).to.be.instanceOf(PublicKey);
      }

      await mockRpcResponse({
        method: 'getConfirmedTransaction',
        params: [confirmedTransaction, {encoding: 'jsonParsed'}],
        value: getMockData({
          accounts: [
            'EeJqWk5pczNjsqqY3jia9xfFNG1dD68te4s8gsdCuEk7',
            '6tVrjJhFm5SAvvdh6tysjotQurCSELpxuW3JaAAYeC1m',
          ],
          data: 'ai3535',
          programId: 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
        }),
      });

      const result2 = await connection.getParsedConfirmedTransaction(
        confirmedTransaction,
      );

      if (result2 && result2.meta && result2.meta.innerInstructions) {
        const innerInstructions = result2.meta.innerInstructions;
        const instruction = innerInstructions[0].instructions[0];
        expect(instruction.programId).to.be.instanceOf(PublicKey);
        if ('accounts' in instruction) {
          expect(instruction.accounts[0]).to.be.instanceOf(PublicKey);
          expect(instruction.accounts[1]).to.be.instanceOf(PublicKey);
        } else {
          expect('accounts' in instruction).to.be.true;
        }
      }
    });
  }

  it('get block', async () => {
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [0],
      value: {
        blockTime: 1614281964,
        blockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [],
      },
    });

    // Block 0 never has any transactions in test validator
    const block0 = await connection.getBlock(0);
    if (!block0) {
      expect(block0).not.to.be.null;
      return;
    }

    const blockhash0 = block0.blockhash;
    expect(block0.transactions).to.have.length(0);
    expect(blockhash0).not.to.be.null;
    expect(block0.previousBlockhash).not.to.be.null;
    expect(block0.parentSlot).to.eq(0);

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [1],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [
          {
            meta: {
              fee: 10000,
              postBalances: [499260347380, 15298080, 1, 1, 1],
              preBalances: [499260357380, 15298080, 1, 1, 1],
              status: {Ok: null},
              err: null,
            },
            transaction: {
              message: {
                accountKeys: [
                  'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
                  'SysvarS1otHashes111111111111111111111111111',
                  'SysvarC1ock11111111111111111111111111111111',
                  'Vote111111111111111111111111111111111111111',
                ],
                header: {
                  numReadonlySignedAccounts: 0,
                  numReadonlyUnsignedAccounts: 3,
                  numRequiredSignatures: 2,
                },
                instructions: [
                  {
                    accounts: [1, 2, 3],
                    data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                    programIdIndex: 4,
                  },
                ],
                recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
              },
              signatures: [
                'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
                '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
              ],
            },
          },
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let x = 1;
    while (x < 10) {
      const block1 = await connection.getBlock(x);
      if (block1 && block1.transactions.length >= 1) {
        expect(block1.previousBlockhash).to.eq(blockhash0);
        expect(block1.blockhash).not.to.be.null;
        expect(block1.parentSlot).to.eq(0);
        expect(block1.transactions[0].transaction).not.to.be.null;
        break;
      }
      x++;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [Number.MAX_SAFE_INTEGER],
      error: {
        message: `Block not available for slot ${Number.MAX_SAFE_INTEGER}`,
      },
    });
    await expect(
      connection.getBlock(Number.MAX_SAFE_INTEGER),
    ).to.be.rejectedWith(
      `Block not available for slot ${Number.MAX_SAFE_INTEGER}`,
    );
  });

  it('get confirmed block', async () => {
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [0],
      value: {
        blockTime: 1614281964,
        blockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [],
      },
    });

    // Block 0 never has any transactions in test validator
    const block0 = await connection.getConfirmedBlock(0);
    const blockhash0 = block0.blockhash;
    expect(block0.transactions).to.have.length(0);
    expect(blockhash0).not.to.be.null;
    expect(block0.previousBlockhash).not.to.be.null;
    expect(block0.parentSlot).to.eq(0);

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [1],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        transactions: [
          {
            meta: {
              fee: 10000,
              postBalances: [499260347380, 15298080, 1, 1, 1],
              preBalances: [499260357380, 15298080, 1, 1, 1],
              status: {Ok: null},
              err: null,
            },
            transaction: {
              message: {
                accountKeys: [
                  'va12u4o9DipLEB2z4fuoHszroq1U9NcAB9aooFDPJSf',
                  '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
                  'SysvarS1otHashes111111111111111111111111111',
                  'SysvarC1ock11111111111111111111111111111111',
                  'Vote111111111111111111111111111111111111111',
                ],
                header: {
                  numReadonlySignedAccounts: 0,
                  numReadonlyUnsignedAccounts: 3,
                  numRequiredSignatures: 2,
                },
                instructions: [
                  {
                    accounts: [1, 2, 3],
                    data: '37u9WtQpcm6ULa3VtWDFAWoQc1hUvybPrA3dtx99tgHvvcE7pKRZjuGmn7VX2tC3JmYDYGG7',
                    programIdIndex: 4,
                  },
                ],
                recentBlockhash: 'GeyAFFRY3WGpmam2hbgrKw4rbU2RKzfVLm5QLSeZwTZE',
              },
              signatures: [
                'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
                '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
              ],
            },
          },
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let x = 1;
    while (x < 10) {
      const block1 = await connection.getConfirmedBlock(x);
      if (block1.transactions.length >= 1) {
        expect(block1.previousBlockhash).to.eq(blockhash0);
        expect(block1.blockhash).not.to.be.null;
        expect(block1.parentSlot).to.eq(0);
        expect(block1.transactions[0].transaction).not.to.be.null;
        break;
      }
      x++;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [Number.MAX_SAFE_INTEGER],
      error: {
        message: `Block not available for slot ${Number.MAX_SAFE_INTEGER}`,
      },
    });
    await expect(
      connection.getConfirmedBlock(Number.MAX_SAFE_INTEGER),
    ).to.be.rejectedWith(
      `Block not available for slot ${Number.MAX_SAFE_INTEGER}`,
    );
  });

  it('get confirmed block signatures', async () => {
    await mockRpcResponse({
      method: 'getSlot',
      params: [],
      value: 1,
    });

    while ((await connection.getSlot()) <= 0) {
      continue;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [
        0,
        {
          transactionDetails: 'signatures',
          rewards: false,
        },
      ],
      value: {
        blockTime: 1614281964,
        blockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        signatures: [],
      },
    });

    // Block 0 never has any transactions in test validator
    const block0 = await connection.getConfirmedBlockSignatures(0);
    const blockhash0 = block0.blockhash;
    expect(block0.signatures).to.have.length(0);
    expect(blockhash0).not.to.be.null;
    expect(block0.previousBlockhash).not.to.be.null;
    expect(block0.parentSlot).to.eq(0);
    expect(block0).to.not.have.property('rewards');

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [
        1,
        {
          transactionDetails: 'signatures',
          rewards: false,
        },
      ],
      value: {
        blockTime: 1614281964,
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        previousBlockhash: 'H5nJ91eGag3B5ZSRHZ7zG5ZwXJ6ywCt2hyR8xCsV7xMo',
        parentSlot: 0,
        signatures: [
          'w2Zeq8YkpyB463DttvfzARD7k9ZxGEwbsEw4boEK7jDp3pfoxZbTdLFSsEPhzXhpCcjGi2kHtHFobgX49MMhbWt',
          '4oCEqwGrMdBeMxpzuWiukCYqSfV4DsSKXSiVVCh1iJ6pS772X7y219JZP3mgqBz5PhsvprpKyhzChjYc3VSBQXzG',
        ],
      },
    });

    // Find a block that has a transaction, usually Block 1
    let x = 1;
    while (x < 10) {
      const block1 = await connection.getConfirmedBlockSignatures(x);
      if (block1.signatures.length >= 1) {
        expect(block1.previousBlockhash).to.eq(blockhash0);
        expect(block1.blockhash).not.to.be.null;
        expect(block1.parentSlot).to.eq(0);
        expect(block1.signatures[0]).not.to.be.null;
        expect(block1).to.not.have.property('rewards');
        break;
      }
      x++;
    }

    await mockRpcResponse({
      method: 'getConfirmedBlock',
      params: [Number.MAX_SAFE_INTEGER],
      error: {
        message: `Block not available for slot ${Number.MAX_SAFE_INTEGER}`,
      },
    });
    await expect(
      connection.getConfirmedBlockSignatures(Number.MAX_SAFE_INTEGER),
    ).to.be.rejectedWith(
      `Block not available for slot ${Number.MAX_SAFE_INTEGER}`,
    );
  });

  it('get recent blockhash', async () => {
    const commitments: Commitment[] = ['processed', 'confirmed', 'finalized'];
    for (const commitment of commitments) {
      const {blockhash, feeCalculator} = await helpers.recentBlockhash({
        connection,
        commitment,
      });
      expect(bs58.decode(blockhash)).to.have.length(32);
      expect(feeCalculator.lamportsPerSignature).to.be.at.least(0);
    }
  });

  it('get fee calculator', async () => {
    const {blockhash} = await helpers.recentBlockhash({connection});
    await mockRpcResponse({
      method: 'getFeeCalculatorForBlockhash',
      params: [blockhash, {commitment: 'confirmed'}],
      value: {
        feeCalculator: {
          lamportsPerSignature: 5000,
        },
      },
      withContext: true,
    });

    const feeCalculator = (
      await connection.getFeeCalculatorForBlockhash(blockhash, 'confirmed')
    ).value;
    if (feeCalculator === null) {
      expect(feeCalculator).not.to.be.null;
      return;
    }
    expect(feeCalculator.lamportsPerSignature).to.eq(5000);
  });

  it('get fees', async () => {
    await mockRpcResponse({
      method: 'getFees',
      params: [{commitment: 'confirmed'}],
      value: {
        blockhash: '57zQNBZBEiHsCZFqsaY6h176ioXy5MsSLmcvHkEyaLGy',
        feeCalculator: {
          lamportsPerSignature: 5000,
        },
        lastValidSlot: 5678,
        lastValidBlockHeight: 1234,
      },
      withContext: true,
    });

    const fees = (await connection.getFees('confirmed')).value;
    expect(fees.feeCalculator).not.to.be.null;
    expect(fees.lastValidSlot).to.be.greaterThan(0);
    expect(fees.lastValidBlockHeight).to.be.greaterThan(0);
  })

  it('get block time', async () => {
    await mockRpcResponse({
      method: 'getBlockTime',
      params: [1],
      value: 10000,
    });

    const blockTime = await connection.getBlockTime(1);
    if (blockTime === null) {
      expect(blockTime).not.to.be.null;
    } else {
      expect(blockTime).to.be.greaterThan(0);
    }
  });

  it('get minimum ledger slot', async () => {
    await mockRpcResponse({
      method: 'minimumLedgerSlot',
      params: [],
      value: 0,
    });

    const minimumLedgerSlot = await connection.getMinimumLedgerSlot();
    expect(minimumLedgerSlot).to.be.at.least(0);
  });

  it('get first available block', async () => {
    await mockRpcResponse({
      method: 'getFirstAvailableBlock',
      params: [],
      value: 0,
    });

    const firstAvailableBlock = await connection.getFirstAvailableBlock();
    expect(firstAvailableBlock).to.be.at.least(0);
  });

  it('get supply', async () => {
    await mockRpcResponse({
      method: 'getSupply',
      params: [],
      value: {
        total: 1000,
        circulating: 100,
        nonCirculating: 900,
        nonCirculatingAccounts: [Keypair.generate().publicKey.toBase58()],
      },
      withContext: true,
    });

    const supply = (await connection.getSupply()).value;
    expect(supply.total).to.be.greaterThan(0);
    expect(supply.circulating).to.be.greaterThan(0);
    expect(supply.nonCirculating).to.be.at.least(0);
    expect(supply.nonCirculatingAccounts.length).to.be.at.least(0);
  });

  it('get performance samples', async () => {
    await mockRpcResponse({
      method: 'getRecentPerformanceSamples',
      params: [],
      value: [
        {
          slot: 1234,
          numTransactions: 1000,
          numSlots: 60,
          samplePeriodSecs: 60,
        },
      ],
    });

    const perfSamples = await connection.getRecentPerformanceSamples();
    expect(Array.isArray(perfSamples)).to.be.true;

    if (perfSamples.length > 0) {
      expect(perfSamples[0].slot).to.be.greaterThan(0);
      expect(perfSamples[0].numTransactions).to.be.greaterThan(0);
      expect(perfSamples[0].numSlots).to.be.greaterThan(0);
      expect(perfSamples[0].samplePeriodSecs).to.be.greaterThan(0);
    }
  });

  it('get performance samples limit too high', async () => {
    await mockRpcResponse({
      method: 'getRecentPerformanceSamples',
      params: [100000],
      error: mockErrorResponse,
    });

    await expect(connection.getRecentPerformanceSamples(100000)).to.be.rejected;
  });

  const TOKEN_PROGRAM_ID = new PublicKey(
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
  );

  if (process.env.TEST_LIVE) {
    describe('token methods', () => {
      const connection = new Connection(url, 'confirmed');
      const newAccount = Keypair.generate().publicKey;

      let testToken: Token;
      let testTokenPubkey: PublicKey;
      let testTokenAccount: PublicKey;
      let testSignature: TransactionSignature;
      let testOwner: Account;

      // Setup token mints and accounts for token tests
      before(async function () {
        this.timeout(30 * 1000);

        const payerAccount = new Account();
        await connection.confirmTransaction(
          await connection.requestAirdrop(payerAccount.publicKey, 100000000000),
        );

        const mintOwner = new Account();
        const accountOwner = new Account();
        const token = await Token.createMint(
          connection as any,
          payerAccount,
          mintOwner.publicKey,
          null,
          2,
          TOKEN_PROGRAM_ID,
        );

        const tokenAccount = await token.createAccount(accountOwner.publicKey);
        await token.mintTo(tokenAccount, mintOwner, [], 11111);

        const token2 = await Token.createMint(
          connection as any,
          payerAccount,
          mintOwner.publicKey,
          null,
          2,
          TOKEN_PROGRAM_ID,
        );

        const token2Account = await token2.createAccount(
          accountOwner.publicKey,
        );
        await token2.mintTo(token2Account, mintOwner, [], 100);

        const tokenAccountDest = await token.createAccount(
          accountOwner.publicKey,
        );
        testSignature = await token.transfer(
          tokenAccount,
          tokenAccountDest,
          accountOwner,
          [],
          new u64(1),
        );

        await connection.confirmTransaction(testSignature, 'finalized');

        testOwner = accountOwner;
        testToken = token;
        testTokenAccount = tokenAccount as PublicKey;
        testTokenPubkey = testToken.publicKey as PublicKey;
      });

      it('get token supply', async () => {
        const supply = (await connection.getTokenSupply(testTokenPubkey)).value;
        expect(supply.uiAmount).to.eq(111.11);
        expect(supply.decimals).to.eq(2);
        expect(supply.amount).to.eq('11111');

        await expect(connection.getTokenSupply(newAccount)).to.be.rejected;
      });

      it('get token largest accounts', async () => {
        const largestAccounts = (
          await connection.getTokenLargestAccounts(testTokenPubkey)
        ).value;

        expect(largestAccounts).to.have.length(2);
        const largestAccount = largestAccounts[0];
        expect(largestAccount.address).to.eql(testTokenAccount);
        expect(largestAccount.amount).to.eq('11110');
        expect(largestAccount.decimals).to.eq(2);
        expect(largestAccount.uiAmount).to.eq(111.1);

        await expect(connection.getTokenLargestAccounts(newAccount)).to.be
          .rejected;
      });

      it('get confirmed token transaction', async () => {
        const parsedTx = await connection.getParsedConfirmedTransaction(
          testSignature,
        );
        if (parsedTx === null) {
          expect(parsedTx).not.to.be.null;
          return;
        }
        const {signatures, message} = parsedTx.transaction;
        expect(signatures[0]).to.eq(testSignature);
        const ix = message.instructions[0];
        if ('parsed' in ix) {
          expect(ix.program).to.eq('spl-token');
          expect(ix.programId).to.eql(TOKEN_PROGRAM_ID);
        } else {
          expect('parsed' in ix).to.be.true;
        }

        const missingSignature =
          '45pGoC4Rr3fJ1TKrsiRkhHRbdUeX7633XAGVec6XzVdpRbzQgHhe6ZC6Uq164MPWtiqMg7wCkC6Wy3jy2BqsDEKf';
        const nullResponse = await connection.getParsedConfirmedTransaction(
          missingSignature,
        );

        expect(nullResponse).to.be.null;
      });

      it('get token account balance', async () => {
        const balance = (
          await connection.getTokenAccountBalance(testTokenAccount)
        ).value;
        expect(balance.amount).to.eq('11110');
        expect(balance.decimals).to.eq(2);
        expect(balance.uiAmount).to.eq(111.1);

        await expect(connection.getTokenAccountBalance(newAccount)).to.be
          .rejected;
      });

      it('get parsed token account info', async () => {
        const accountInfo = (
          await connection.getParsedAccountInfo(testTokenAccount)
        ).value;
        if (accountInfo) {
          const data = accountInfo.data;
          if (Buffer.isBuffer(data)) {
            expect(Buffer.isBuffer(data)).to.eq(false);
          } else {
            expect(data.program).to.eq('spl-token');
            expect(data.parsed).to.be.ok;
          }
        }
      });

      it('get parsed token program accounts', async () => {
        const tokenAccounts = await connection.getParsedProgramAccounts(
          TOKEN_PROGRAM_ID,
        );
        tokenAccounts.forEach(({account}) => {
          expect(account.owner).to.eql(TOKEN_PROGRAM_ID);
          const data = account.data;
          if (Buffer.isBuffer(data)) {
            expect(Buffer.isBuffer(data)).to.eq(false);
          } else {
            expect(data.parsed).to.be.ok;
            expect(data.program).to.eq('spl-token');
          }
        });
      });

      it('get parsed token accounts by owner', async () => {
        const tokenAccounts = (
          await connection.getParsedTokenAccountsByOwner(testOwner.publicKey, {
            mint: testTokenPubkey,
          })
        ).value;
        tokenAccounts.forEach(({account}) => {
          expect(account.owner).to.eql(TOKEN_PROGRAM_ID);
          const data = account.data;
          if (Buffer.isBuffer(data)) {
            expect(Buffer.isBuffer(data)).to.eq(false);
          } else {
            expect(data.parsed).to.be.ok;
            expect(data.program).to.eq('spl-token');
          }
        });
      });

      it('get token accounts by owner', async () => {
        const accountsWithMintFilter = (
          await connection.getTokenAccountsByOwner(testOwner.publicKey, {
            mint: testTokenPubkey,
          })
        ).value;
        expect(accountsWithMintFilter).to.have.length(2);

        const accountsWithProgramFilter = (
          await connection.getTokenAccountsByOwner(testOwner.publicKey, {
            programId: TOKEN_PROGRAM_ID,
          })
        ).value;
        expect(accountsWithProgramFilter).to.have.length(3);

        const noAccounts = (
          await connection.getTokenAccountsByOwner(newAccount, {
            mint: testTokenPubkey,
          })
        ).value;
        expect(noAccounts).to.have.length(0);

        await expect(
          connection.getTokenAccountsByOwner(testOwner.publicKey, {
            mint: newAccount,
          }),
        ).to.be.rejected;

        await expect(
          connection.getTokenAccountsByOwner(testOwner.publicKey, {
            programId: newAccount,
          }),
        ).to.be.rejected;
      });
    });

    it('consistent preflightCommitment', async () => {
      const connection = new Connection(url, 'singleGossip');
      const sender = Keypair.generate();
      const recipient = Keypair.generate();
      let signature = await connection.requestAirdrop(
        sender.publicKey,
        2 * LAMPORTS_PER_SOL,
      );
      await connection.confirmTransaction(signature, 'singleGossip');
      const transaction = new Transaction().add(
        SystemProgram.transfer({
          fromPubkey: sender.publicKey,
          toPubkey: recipient.publicKey,
          lamports: LAMPORTS_PER_SOL,
        }),
      );
      await sendAndConfirmTransaction(connection, transaction, [sender]);
    });
  }

  it('get largest accounts', async () => {
    await mockRpcResponse({
      method: 'getLargestAccounts',
      params: [],
      value: new Array(20).fill(0).map(() => ({
        address: Keypair.generate().publicKey.toBase58(),
        lamports: 1000,
      })),
      withContext: true,
    });

    const largestAccounts = (await connection.getLargestAccounts()).value;
    expect(largestAccounts).to.have.length(20);
  });

  it('stake activation should throw when called for not delegated account', async () => {
    const publicKey = Keypair.generate().publicKey;
    await mockRpcResponse({
      method: 'getStakeActivation',
      params: [publicKey.toBase58(), {}],
      error: {message: 'account not delegated'},
    });

    await expect(connection.getStakeActivation(publicKey)).to.be.rejected;
  });

  if (process.env.TEST_LIVE) {
    it('stake activation should return activating for new accounts', async () => {
      const voteAccounts = await connection.getVoteAccounts();
      const voteAccount = voteAccounts.current.concat(
        voteAccounts.delinquent,
      )[0];
      const votePubkey = new PublicKey(voteAccount.votePubkey);

      const authorized = Keypair.generate();
      let signature = await connection.requestAirdrop(
        authorized.publicKey,
        2 * LAMPORTS_PER_SOL,
      );
      await connection.confirmTransaction(signature, 'confirmed');

      const minimumAmount = await connection.getMinimumBalanceForRentExemption(
        StakeProgram.space,
      );

      const newStakeAccount = Keypair.generate();
      let createAndInitialize = StakeProgram.createAccount({
        fromPubkey: authorized.publicKey,
        stakePubkey: newStakeAccount.publicKey,
        authorized: new Authorized(authorized.publicKey, authorized.publicKey),
        lockup: new Lockup(0, 0, new PublicKey(0)),
        lamports: minimumAmount + 42,
      });

      await sendAndConfirmTransaction(
        connection,
        createAndInitialize,
        [authorized, newStakeAccount],
        {
          preflightCommitment: 'confirmed',
          commitment: 'confirmed',
        },
      );
      let delegation = StakeProgram.delegate({
        stakePubkey: newStakeAccount.publicKey,
        authorizedPubkey: authorized.publicKey,
        votePubkey,
      });
      await sendAndConfirmTransaction(connection, delegation, [authorized], {
        preflightCommitment: 'confirmed',
        commitment: 'confirmed',
      });

      const LARGE_EPOCH = 4000;
      await expect(
        connection.getStakeActivation(
          newStakeAccount.publicKey,
          'confirmed',
          LARGE_EPOCH,
        ),
      ).to.be.rejectedWith(
        `failed to get Stake Activation ${newStakeAccount.publicKey.toBase58()}: Invalid param: epoch ${LARGE_EPOCH} has not yet started`,
      );

      const activationState = await connection.getStakeActivation(
        newStakeAccount.publicKey,
        'confirmed',
      );
      expect(activationState.state).to.eq('activating');
      expect(activationState.inactive).to.eq(42);
      expect(activationState.active).to.eq(0);
    });
  }

  if (mockServer) {
    it('stake activation should only accept state with valid string literals', async () => {
      const publicKey = Keypair.generate().publicKey;

      const addStakeActivationMock = async (state: any) => {
        await mockRpcResponse({
          method: 'getStakeActivation',
          params: [publicKey.toBase58(), {}],
          value: {
            state: state,
            active: 0,
            inactive: 80,
          },
        });
      };

      await addStakeActivationMock('active');
      let activation = await connection.getStakeActivation(
        publicKey,
        'confirmed',
      );
      expect(activation.state).to.eq('active');
      expect(activation.active).to.eq(0);
      expect(activation.inactive).to.eq(80);

      await addStakeActivationMock('invalid');
      await expect(connection.getStakeActivation(publicKey, 'confirmed')).to.be
        .rejected;
    });
  }

  it('getVersion', async () => {
    await mockRpcResponse({
      method: 'getVersion',
      params: [],
      value: {'solana-core': '0.20.4'},
    });

    const version = await connection.getVersion();
    expect(version['solana-core']).to.be.ok;
  });

  it('request airdrop', async () => {
    const account = Keypair.generate();

    await helpers.airdrop({
      connection,
      address: account.publicKey,
      amount: LAMPORTS_PER_SOL,
    });

    await mockRpcResponse({
      method: 'getBalance',
      params: [account.publicKey.toBase58(), {commitment: 'confirmed'}],
      value: LAMPORTS_PER_SOL,
      withContext: true,
    });

    const balance = await connection.getBalance(account.publicKey, 'confirmed');
    expect(balance).to.eq(LAMPORTS_PER_SOL);

    await mockRpcResponse({
      method: 'getAccountInfo',
      params: [
        account.publicKey.toBase58(),
        {commitment: 'confirmed', encoding: 'base64'},
      ],
      value: {
        owner: '11111111111111111111111111111111',
        lamports: LAMPORTS_PER_SOL,
        data: ['', 'base64'],
        executable: false,
        rentEpoch: 20,
      },
      withContext: true,
    });

    const accountInfo = await connection.getAccountInfo(
      account.publicKey,
      'confirmed',
    );
    if (accountInfo === null) {
      expect(accountInfo).not.to.be.null;
      return;
    }
    expect(accountInfo.lamports).to.eq(LAMPORTS_PER_SOL);
    expect(accountInfo.data).to.have.length(0);
    expect(accountInfo.owner).to.eql(SystemProgram.programId);

    await mockRpcResponse({
      method: 'getAccountInfo',
      params: [
        account.publicKey.toBase58(),
        {commitment: 'confirmed', encoding: 'jsonParsed'},
      ],
      value: {
        owner: '11111111111111111111111111111111',
        lamports: LAMPORTS_PER_SOL,
        data: ['', 'base64'],
        executable: false,
        rentEpoch: 20,
      },
      withContext: true,
    });

    const parsedAccountInfo = (
      await connection.getParsedAccountInfo(account.publicKey, 'confirmed')
    ).value;
    if (parsedAccountInfo === null) {
      expect(parsedAccountInfo).not.to.be.null;
      return;
    } else if ('parsed' in parsedAccountInfo.data) {
      expect(parsedAccountInfo.data.parsed).not.to.be.ok;
      return;
    }
    expect(parsedAccountInfo.lamports).to.eq(LAMPORTS_PER_SOL);
    expect(parsedAccountInfo.data).to.have.length(0);
    expect(parsedAccountInfo.owner).to.eql(SystemProgram.programId);
  });

  it('transaction failure', async () => {
    const payer = Keypair.generate();

    await helpers.airdrop({
      connection,
      address: payer.publicKey,
      amount: LAMPORTS_PER_SOL,
    });

    const newAccount = Keypair.generate();
    let transaction = new Transaction().add(
      SystemProgram.createAccount({
        fromPubkey: payer.publicKey,
        newAccountPubkey: newAccount.publicKey,
        lamports: LAMPORTS_PER_SOL / 2,
        space: 0,
        programId: SystemProgram.programId,
      }),
    );

    await helpers.processTransaction({
      connection,
      transaction,
      signers: [payer, newAccount],
      commitment: 'confirmed',
    });

    // This should fail because the account is already created
    const expectedErr = {InstructionError: [0, {Custom: 0}]};
    const confirmResult = (
      await helpers.processTransaction({
        connection,
        transaction,
        signers: [payer, newAccount],
        commitment: 'confirmed',
        err: expectedErr,
      })
    ).value;
    expect(confirmResult.err).to.eql(expectedErr);

    invariant(transaction.signature);
    const signature = bs58.encode(transaction.signature);
    await mockRpcResponse({
      method: 'getSignatureStatuses',
      params: [[signature]],
      value: [
        {
          slot: 0,
          confirmations: 11,
          status: {Err: expectedErr},
          err: expectedErr,
        },
      ],
      withContext: true,
    });

    const response = (await connection.getSignatureStatus(signature)).value;
    verifySignatureStatus(response, expectedErr);
  });

  if (process.env.TEST_LIVE) {
    it('transaction', async () => {
      connection._commitment = 'confirmed';

      const accountFrom = Keypair.generate();
      const accountTo = Keypair.generate();
      const minimumAmount = await connection.getMinimumBalanceForRentExemption(
        0,
      );

      await helpers.airdrop({
        connection,
        address: accountFrom.publicKey,
        amount: minimumAmount + 100010,
      });
      await helpers.airdrop({
        connection,
        address: accountTo.publicKey,
        amount: minimumAmount,
      });

      const transaction = new Transaction().add(
        SystemProgram.transfer({
          fromPubkey: accountFrom.publicKey,
          toPubkey: accountTo.publicKey,
          lamports: 10,
        }),
      );

      const signature = await sendAndConfirmTransaction(
        connection,
        transaction,
        [accountFrom],
        {preflightCommitment: 'confirmed'},
      );

      // Send again and ensure that new blockhash is used
      const lastFetch = Date.now();
      const transaction2 = new Transaction().add(
        SystemProgram.transfer({
          fromPubkey: accountFrom.publicKey,
          toPubkey: accountTo.publicKey,
          lamports: 10,
        }),
      );

      const signature2 = await sendAndConfirmTransaction(
        connection,
        transaction2,
        [accountFrom],
        {preflightCommitment: 'confirmed'},
      );

      expect(signature).not.to.eq(signature2);
      expect(transaction.recentBlockhash).not.to.eq(
        transaction2.recentBlockhash,
      );

      // Send new transaction and ensure that same blockhash is used
      const transaction3 = new Transaction().add(
        SystemProgram.transfer({
          fromPubkey: accountFrom.publicKey,
          toPubkey: accountTo.publicKey,
          lamports: 9,
        }),
      );
      await sendAndConfirmTransaction(connection, transaction3, [accountFrom], {
        preflightCommitment: 'confirmed',
      });
      expect(transaction2.recentBlockhash).to.eq(transaction3.recentBlockhash);

      // Sleep until blockhash cache times out
      await sleep(
        Math.max(
          0,
          1000 + BLOCKHASH_CACHE_TIMEOUT_MS - (Date.now() - lastFetch),
        ),
      );

      const transaction4 = new Transaction().add(
        SystemProgram.transfer({
          fromPubkey: accountFrom.publicKey,
          toPubkey: accountTo.publicKey,
          lamports: 13,
        }),
      );

      await sendAndConfirmTransaction(connection, transaction4, [accountFrom], {
        preflightCommitment: 'confirmed',
      });

      expect(transaction4.recentBlockhash).not.to.eq(
        transaction3.recentBlockhash,
      );

      // accountFrom may have less than 100000 due to transaction fees
      const balance = await connection.getBalance(accountFrom.publicKey);
      expect(balance).to.be.greaterThan(0);
      expect(balance).to.be.at.most(minimumAmount + 100000);
      expect(await connection.getBalance(accountTo.publicKey)).to.eq(
        minimumAmount + 42,
      );
    }).timeout(45 * 1000); // waits 30s for cache timeout

    it('multi-instruction transaction', async () => {
      connection._commitment = 'confirmed';

      const accountFrom = Keypair.generate();
      const accountTo = Keypair.generate();

      let signature = await connection.requestAirdrop(
        accountFrom.publicKey,
        LAMPORTS_PER_SOL,
      );
      await connection.confirmTransaction(signature);
      expect(await connection.getBalance(accountFrom.publicKey)).to.eq(
        LAMPORTS_PER_SOL,
      );

      const minimumAmount = await connection.getMinimumBalanceForRentExemption(
        0,
      );

      signature = await connection.requestAirdrop(
        accountTo.publicKey,
        minimumAmount + 21,
      );
      await connection.confirmTransaction(signature);
      expect(await connection.getBalance(accountTo.publicKey)).to.eq(
        minimumAmount + 21,
      );

      // 1. Move(accountFrom, accountTo)
      // 2. Move(accountTo, accountFrom)
      const transaction = new Transaction()
        .add(
          SystemProgram.transfer({
            fromPubkey: accountFrom.publicKey,
            toPubkey: accountTo.publicKey,
            lamports: 100,
          }),
        )
        .add(
          SystemProgram.transfer({
            fromPubkey: accountTo.publicKey,
            toPubkey: accountFrom.publicKey,
            lamports: 100,
          }),
        );
      signature = await connection.sendTransaction(
        transaction,
        [accountFrom, accountTo],
        {skipPreflight: true},
      );

      await connection.confirmTransaction(signature);

      const response = (await connection.getSignatureStatus(signature)).value;
      if (response !== null) {
        expect(typeof response.slot).to.eq('number');
        expect(response.err).to.be.null;
      } else {
        expect(response).not.to.be.null;
      }

      // accountFrom may have less than LAMPORTS_PER_SOL due to transaction fees
      expect(
        await connection.getBalance(accountFrom.publicKey),
      ).to.be.greaterThan(0);
      expect(await connection.getBalance(accountFrom.publicKey)).to.be.at.most(
        LAMPORTS_PER_SOL,
      );

      expect(await connection.getBalance(accountTo.publicKey)).to.eq(
        minimumAmount + 21,
      );
    });

    // it('account change notification', async () => {
    //   if (mockServer) {
    //     console.log('non-live test skipped');
    //     return;
    //   }

    //   const connection = new Connection(url, 'confirmed');
    //   const owner = Keypair.generate();
    //   const programAccount = Keypair.generate();

    //   const mockCallback = jest.fn();

    //   const subscriptionId = connection.onAccountChange(
    //     programAccount.publicKey,
    //     mockCallback,
    //     'confirmed',
    //   );

    //   const balanceNeeded = Math.max(
    //     await connection.getMinimumBalanceForRentExemption(0),
    //     1,
    //   );

    //   let signature = await connection.requestAirdrop(
    //     owner.publicKey,
    //     LAMPORTS_PER_SOL,
    //   );
    //   await connection.confirmTransaction(signature);
    //   try {
    //     const transaction = new Transaction().add(
    //       SystemProgram.transfer({
    //         fromPubkey: owner.publicKey,
    //         toPubkey: programAccount.publicKey,
    //         lamports: balanceNeeded,
    //       }),
    //     );
    //     await sendAndConfirmTransaction(connection, transaction, [owner], {
    //       commitment: 'confirmed',
    //     });
    //   } catch (err) {
    //     await connection.removeAccountChangeListener(subscriptionId);
    //     throw err;
    //   }

    //   // Wait for mockCallback to receive a call
    //   let i = 0;
    //   for (;;) {
    //     if (mockCallback.mock.calls.length > 0) {
    //       break;
    //     }

    //     if (++i === 30) {
    //       throw new Error('Account change notification not observed');
    //     }
    //     // Sleep for a 1/4 of a slot, notifications only occur after a block is
    //     // processed
    //     await sleep((250 * DEFAULT_TICKS_PER_SLOT) / NUM_TICKS_PER_SECOND);
    //   }

    //   await connection.removeAccountChangeListener(subscriptionId);

    //   expect(mockCallback.mock.calls[0][0].lamports).to.eq(balanceNeeded);
    //   expect(mockCallback.mock.calls[0][0].owner).to.eq(SystemProgram.programId);
    // });

    it('program account change notification', async () => {
      connection._commitment = 'confirmed';

      const owner = Keypair.generate();
      const programAccount = Keypair.generate();
      const balanceNeeded = await connection.getMinimumBalanceForRentExemption(
        0,
      );

      let notified = false;
      const subscriptionId = connection.onProgramAccountChange(
        SystemProgram.programId,
        (keyedAccountInfo: KeyedAccountInfo) => {
          if (keyedAccountInfo.accountId.equals(programAccount.publicKey)) {
            expect(keyedAccountInfo.accountInfo.lamports).to.eq(balanceNeeded);
            expect(
              keyedAccountInfo.accountInfo.owner.equals(
                SystemProgram.programId,
              ),
            ).to.be.true;
            notified = true;
          }
        },
      );

      await helpers.airdrop({
        connection,
        address: owner.publicKey,
        amount: LAMPORTS_PER_SOL,
      });

      try {
        const transaction = new Transaction().add(
          SystemProgram.transfer({
            fromPubkey: owner.publicKey,
            toPubkey: programAccount.publicKey,
            lamports: balanceNeeded,
          }),
        );
        await sendAndConfirmTransaction(connection, transaction, [owner], {
          commitment: 'confirmed',
        });
      } catch (err) {
        await connection.removeProgramAccountChangeListener(subscriptionId);
        throw err;
      }

      let i = 0;
      while (!notified) {
        if (++i === 30) {
          throw new Error('Program change notification not observed');
        }
        // Sleep for a 1/4 of a slot, notifications only occur after a block is
        // processed
        await sleep((250 * DEFAULT_TICKS_PER_SLOT) / NUM_TICKS_PER_SECOND);
      }

      await connection.removeProgramAccountChangeListener(subscriptionId);
    });

    it('slot notification', async () => {
      let notifiedSlotInfo: SlotInfo | undefined;
      const subscriptionId = connection.onSlotChange(slotInfo => {
        notifiedSlotInfo = slotInfo;
      });

      // Wait for notification
      let i = 0;
      while (!notifiedSlotInfo) {
        if (++i === 30) {
          throw new Error('Slot change notification not observed');
        }
        // Sleep for a 1/4 of a slot, notifications only occur after a block is
        // processed
        await sleep((250 * DEFAULT_TICKS_PER_SLOT) / NUM_TICKS_PER_SECOND);
      }

      expect(notifiedSlotInfo.parent).to.be.at.least(0);
      expect(notifiedSlotInfo.root).to.be.at.least(0);
      expect(notifiedSlotInfo.slot).to.be.at.least(1);

      await connection.removeSlotChangeListener(subscriptionId);
    });

    it('root notification', async () => {
      let roots: number[] = [];
      const subscriptionId = connection.onRootChange(root => {
        roots.push(root);
      });

      // Wait for mockCallback to receive a call
      let i = 0;
      while (roots.length < 2) {
        if (++i === 30) {
          throw new Error('Root change notification not observed');
        }
        // Sleep for a 1/4 of a slot, notifications only occur after a block is
        // processed
        await sleep((250 * DEFAULT_TICKS_PER_SLOT) / NUM_TICKS_PER_SECOND);
      }

      expect(roots[1]).to.be.greaterThan(roots[0]);
      await connection.removeRootChangeListener(subscriptionId);
    });

    it('logs notification', async () => {
      let listener: number | undefined;
      const owner = Keypair.generate();
      const [logsRes, ctx] = await new Promise(resolve => {
        let received = false;
        listener = connection.onLogs(
          owner.publicKey,
          (logs, ctx) => {
            if (!logs.err) {
              received = true;
              resolve([logs, ctx]);
            }
          },
          'processed',
        );

        // Send transactions until the log subscription receives an event
        (async () => {
          while (!received) {
            // Execute a transaction so that we can pickup its logs.
            await connection.requestAirdrop(owner.publicKey, 1);
          }
        })();
      });
      expect(ctx.slot).to.be.greaterThan(0);
      expect(logsRes.logs.length).to.eq(2);
      expect(logsRes.logs[0]).to.eq(
        'Program 11111111111111111111111111111111 invoke [1]',
      );
      expect(logsRes.logs[1]).to.eq(
        'Program 11111111111111111111111111111111 success',
      );
      await connection.removeOnLogsListener(listener!);
    });

    it('https request', async () => {
      const connection = new Connection('https://api.mainnet-beta.solana.com');
      const version = await connection.getVersion();
      expect(version['solana-core']).to.be.ok;
    });
  }
});
