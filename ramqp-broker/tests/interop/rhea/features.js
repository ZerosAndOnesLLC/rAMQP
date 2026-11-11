// rhea (Node.js) against ramqp-broker's 0.10 feature set: PLAIN logins,
// exchange addresses, amqps. Reads MATRIX_* env (../matrix/run.sh); prints
// PASS/FAIL per scenario, exits non-zero on any failure.
'use strict';
const fs = require('fs');
const rhea = require('rhea');

const E = process.env;
const base = { host: E.MATRIX_HOST, port: Number(E.MATRIX_PLAIN_PORT), reconnect: false };
const login = { username: E.MATRIX_USER, password: E.MATRIX_PASS };
let failures = 0;

function withTimeout(p, ms, what) {
  return Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error(`${what}: timed out`)), ms))]);
}

// Open a connection; resolves on 'connection_open', rejects on refusal.
function connect(opts) {
  return withTimeout(new Promise((resolve, reject) => {
    const c = rhea.create_container().connect(opts);
    c.once('connection_open', () => resolve(c));
    c.once('connection_error', (ctx) => reject(new Error(`connection error: ${JSON.stringify(ctx.connection.error)}`)));
    c.once('disconnected', (ctx) => reject(new Error(`disconnected: ${ctx.error || 'auth refused'}`)));
  }), 10000, 'connect');
}

function send(conn, address, message) {
  return withTimeout(new Promise((resolve, reject) => {
    const s = conn.open_sender(address);
    s.once('sendable', () => s.send(message));
    s.once('accepted', () => { s.close(); resolve(); });
    s.once('rejected', (ctx) => reject(new Error(`rejected: ${JSON.stringify(ctx.delivery.remote_state)}`)));
    s.once('sender_error', (ctx) => reject(new Error(`sender error: ${JSON.stringify(ctx.sender.error)}`)));
  }), 10000, `send to ${address}`);
}

function receive(conn, address, n) {
  return withTimeout(new Promise((resolve, reject) => {
    const got = [];
    const r = conn.open_receiver({ source: address, credit_window: n, autoaccept: true });
    r.on('message', (ctx) => {
      got.push(ctx.message.body);
      if (got.length === n) { r.close(); resolve(got); }
    });
    r.once('receiver_error', (ctx) => reject(new Error(`receiver error: ${JSON.stringify(ctx.receiver.error)}`)));
  }), 10000, `receive from ${address}`);
}

async function check(name, fn) {
  try {
    await fn();
    console.log(`PASS  ${name}`);
  } catch (e) {
    failures += 1;
    console.log(`FAIL  ${name}: ${e.message}`);
  }
}

async function main() {
  await check('PLAIN login + round trip', async () => {
    const c = await connect({ ...base, ...login });
    await send(c, '/queues/rhea-q', { body: 'ping' });
    const [m] = await receive(c, '/queues/rhea-q', 1);
    if (m !== 'ping') throw new Error(`got ${m}`);
    c.close();
  });
  await check('wrong password refused', async () => {
    let c;
    try {
      c = await connect({ ...base, username: E.MATRIX_USER, password: 'nope' });
    } catch (_) {
      return; // refused, as expected
    }
    c.close();
    throw new Error('connected with a wrong password');
  });
  await check('exchanges: key in address + key from subject', async () => {
    const c = await connect({ ...base, ...login });
    await send(c, '/exchanges/events/rhea.order.created', { body: 'keyed' });
    await send(c, '/exchanges/events', { body: 'by-subject', subject: 'rhea.order.paid' });
    const got = (await receive(c, '/queues/rhea-orders', 2)).sort();
    if (JSON.stringify(got) !== JSON.stringify(['by-subject', 'keyed'])) throw new Error(`routed ${got}`);
    c.close();
  });
  await check('unknown exchange refused', async () => {
    const c = await connect({ ...base, ...login });
    let refused = false;
    try {
      await send(c, '/exchanges/missing/k', { body: 'x' });
    } catch (_) {
      refused = true;
    }
    c.close();
    if (!refused) throw new Error('sent to an unknown exchange');
  });
  await check('amqps (private CA, hostname verified)', async () => {
    const c = await connect({
      ...login, host: 'localhost', port: Number(E.MATRIX_TLS_PORT), reconnect: false,
      transport: 'tls', ca: [fs.readFileSync(E.MATRIX_CA)], servername: 'localhost',
      rejectUnauthorized: true,
    });
    await send(c, '/queues/rhea-tls', { body: 'ping' });
    const [m] = await receive(c, '/queues/rhea-tls', 1);
    if (m !== 'ping') throw new Error(`got ${m}`);
    c.close();
  });
  console.log(failures === 0 ? 'ALL_OK' : `${failures} FAILED`);
  process.exit(failures === 0 ? 0 : 1);
}

main();
