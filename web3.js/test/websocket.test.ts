import bs58 from 'bs58';
import {Buffer} from 'buffer';
import {expect, use} from 'chai';
import chaiAsPromised from 'chai-as-promised';

import {Connection} from '../src';
import {url, wsUrl} from './url';
import {sleep} from '../src/util/sleep';

use(chaiAsPromised);

if (process.env.TEST_LIVE) {
  describe('websocket', () => {
    const connection = new Connection(url);

    it('connect and disconnect', async () => {
      const testSignature = bs58.encode(Buffer.alloc(64));
      const id = connection.onSignature(testSignature, () => {});

      // wait for websocket to connect
      await sleep(100);
      expect(connection._rpcWebSocketConnected).to.be.true;
      expect(connection._rpcWebSocketHeartbeat).not.to.eq(null);

      // test if socket is open
      await connection._rpcWebSocket.notify('ping');

      await connection.removeSignatureListener(id);
      expect(connection._rpcWebSocketConnected).to.eq(false);
      expect(connection._rpcWebSocketHeartbeat).not.to.eq(null);
      expect(connection._rpcWebSocketIdleTimeout).not.to.eq(null);

      // wait for websocket to disconnect
      await sleep(1100);
      expect(connection._rpcWebSocketConnected).to.eq(false);
      expect(connection._rpcWebSocketHeartbeat).to.eq(null);
      expect(connection._rpcWebSocketIdleTimeout).to.eq(null);

      // test if socket is closed
      await expect(connection._rpcWebSocket.notify('ping')).to.be.rejectedWith(
        'socket not ready',
      );
    });

    it('idle timeout', async () => {
      const testSignature = bs58.encode(Buffer.alloc(64));
      const id = connection.onSignature(testSignature, () => {});

      // wait for websocket to connect
      await sleep(100);
      expect(connection._rpcWebSocketIdleTimeout).to.eq(null);

      await connection.removeSignatureListener(id);
      expect(connection._rpcWebSocketIdleTimeout).not.to.eq(null);

      const nextId = connection.onSignature(testSignature, () => {});
      expect(connection._rpcWebSocketIdleTimeout).to.eq(null);

      await connection.removeSignatureListener(nextId);
      expect(connection._rpcWebSocketIdleTimeout).not.to.eq(null);

      // wait for websocket to disconnect
      await sleep(1100);
      expect(connection._rpcWebSocketIdleTimeout).to.eq(null);
    });

    it('connect by websocket endpoint from options', async () => {
      let connection = new Connection('http://localhost', {
        wsEndpoint: wsUrl,
      });

      const testSignature = bs58.encode(Buffer.alloc(64));
      const id = connection.onSignature(testSignature, () => {});

      // wait for websocket to connect
      await sleep(100);
      expect(connection._rpcWebSocketConnected).to.be.true;
      expect(connection._rpcWebSocketHeartbeat).not.to.eq(null);

      await connection.removeSignatureListener(id);
    });

    it('reconnects once subscription are created', async () => {
      const newConnection = new Connection(url);

      // wait for websocket to disconnect
      await sleep(1100);
      expect(newConnection._rpcWebSocketConnected).to.be.false;
      expect(newConnection._rpcWebSocketIdleTimeout).to.eq(null);

      const testSignature = bs58.encode(Buffer.alloc(64));
      const id = newConnection.onSignature(testSignature, () => {});

      // wait for websocket to connect
      await sleep(100);
      expect(newConnection._rpcWebSocketConnected).to.be.true;
      expect(newConnection._rpcWebSocketIdleTimeout).to.eq(null);

      await newConnection.removeSignatureListener(id);
    });
  });
}
