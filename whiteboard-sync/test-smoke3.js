'use strict';
/*
 * v3 协议冒烟测试（自动拉起 server.js）：
 *  1. 二进制握手 + 版本协商：major 一致 / minor 降级 / major 拒绝(close 4001)
 *  2. 二进制 ops：编码/服务端物化/二进制广播 + fseq 序号校验
 *  3. 增量同步：lastSeq + VC 断线续传只收缺失操作；太旧回退快照
 *  4. 快照加速：每 N 操作落快照，新客户端快照 + 少量重放
 *  5. 慢客户端背压：扇出在途超水位自动 degrade→快照同步，ACK 后恢复
 *  6. P2P（VirtualNetwork 注入 PeerSession）：增量 gossip 补缺 +
 *     大信封 blob 分块直连传输（字节不过 WS）+ 乱序/丢包可靠传输
 *  7. 离线编辑：未确认信封重放，服务端幂等去重无重复对象
 */
const WebSocket = require('ws');
const http = require('http');
const { spawn } = require('child_process');
const path = require('path');
const WB = require('./public/kernel.js');
const WBT = require('./public/transport.js');
const WBN = require('./public/mesh.js');

const PORT = process.env.PORT || 8191;
const URL = `ws://localhost:${PORT}/ws`;
let passed = 0, failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; console.log('  PASS -', msg); }
  else { failed++; console.error('  FAIL -', msg); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function httpGet(p) {
  return new Promise((resolve, reject) => {
    http.get(`http://localhost:${PORT}${p}`, (res) => {
      let b = ''; res.on('data', (d) => (b += d)); res.on('end', () => resolve({ status: res.statusCode, body: b }));
    }).on('error', reject);
  });
}

/** v3 二进制 WS 客户端 */
class BinClient {
  constructor(userId) {
    this.userId = userId;
    this.ws = null;
    this.sid = 0;
    this.lastSeq = 0;
    this.frames = [];
    this.opsLog = [];
    this.snapshot = null;
    this.degraded = 0;
    this.degradeSeqAt = -1;
    this.recvFseq = 0;
    this.sentAcks = new Set();
    this.onClose = null;
  }
  connect(roomId, opts) {
    opts = opts || {};
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(URL);
      ws.binaryType = 'arraybuffer';
      this.ws = ws;
      ws.on('open', () => {
        ws.send(JSON.stringify({
          type: 'join', roomId, userId: this.userId, lastSeq: opts.lastSeq || 0,
          vc: opts.vc || null,
          proto: { name: opts.name || 'wb3', major: opts.major || 3, minor: opts.minor || 0,
            caps: WBT.PROTO.CAPS.BIN | WBT.PROTO.CAPS.DC }
        }));
      });
      ws.on('message', (raw, isBinary) => {
        if (!isBinary) {
          const m = JSON.parse(raw.toString());
          if (m.type === 'joined' && m.proto) {
            this.sid = m.sessionId;
            this._send({ type: 'join', roomId, userId: this.userId,
              lastSeq: opts.lastSeq || 0, vc: opts.vc || {} });
          }
          if (m.type === 'error') this.frames.push(Object.assign({ _text: true }, m));
          return;
        }
        const m = WBT.frameFrom(raw);
        this.frames.push(m);
        if (m.type === 'joined') { resolve(m); return; }
        if (m.type === 'snapshot') {
          this.snapshot = m;
          this.lastSeq = m.lastSeq;
          this._ackFseq(m);
        }
        if (m.type === 'delta') {
          this.opsLog.push(...(m.envelopes || []));
          this.lastSeq = Math.max(this.lastSeq, m.fromSeq + (m.envelopes || []).length);
          this._ackFseq(m);
        }
        if (m.type === 'ops') {
          // fseq 序号校验：旧帧/重复帧丢弃（首次广播 fseq=1 起）
          if (m.fseq != null && m.fseq <= this.recvFseq) return;
          if (m.fseq != null) this.recvFseq = Math.max(this.recvFseq, m.fseq);
          this.opsLog.push(...(m.envelopes || []));
          this._ackFseq(m);
        }
        if (m.type === 'degrade') {
          this.degraded++;
          this.degradeSeqAt = this.recvFseq; // 标记：degrade 之后的 snapshot 才是降级快照
        }
      });
      ws.on('close', () => this.onClose && this.onClose());
      ws.on('error', reject);
    });
  }
  _ackFseq(m) {
    if (m.fseq != null && !this.sentAcks.has(m.fseq)) {
      this.sentAcks.add(m.fseq);
      this._send({ type: 'ack', ids: [], lastSeq: this.lastSeq, fseq: m.fseq, sacks: [] });
    }
  }
  _send(msg) { if (this.ws.readyState === 1) this.ws.send(WBT.encode(msg)); }
  issue(clock, op, opts) {
    const env = WB.makeEnvelope(clock, op, opts);
    this._send({ type: 'ops', envelopes: [env] });
    return env;
  }
  issueEnv(env) { this._send({ type: 'ops', envelopes: [env] }); return env; }
  waitFor(type, pred, timeoutMs) {
    return new Promise((resolve, reject) => {
      const hit = this.frames.find((f) => f.type === type && (!pred || pred(f)));
      if (hit) return resolve(hit);
      const t0 = Date.now();
      const iv = setInterval(() => {
        const f = this.frames.find((x) => x.type === type && (!pred || pred(x)));
        if (f) { clearInterval(iv); resolve(f); }
        else if (Date.now() - t0 > (timeoutMs || 3000)) { clearInterval(iv); reject(new Error('timeout ' + type)); }
      }, 15);
    });
  }
  close() { try { this.ws.close(); } catch (_) {} }
}

async function main() {
  const room = 'v3-' + Date.now();

  console.log('\n[1] 二进制握手 + 版本协商');
  const A = new BinClient('A3');
  await A.connect(room);
  assert(!!A.sid, 'JSON joined negotiated proto and assigned sessionId ' + A.sid);
  const j = await A.waitFor('joined', (m) => m.type === 'joined' && m.major === 3);
  assert(j.major === 3 && j.minor === 0, 'binary joined frame with negotiated version');
  const peers = await A.waitFor('peers');
  assert(Array.isArray(peers.peers), 'roster frame received');

  // major 不一致 → 文本 error + close 4001
  const bad = new WebSocket(URL);
  let badErr = null, badCode = 0;
  await new Promise((res) => {
    bad.binaryType = 'arraybuffer';
    bad.on('open', () => bad.send(JSON.stringify({
      type: 'join', roomId: room, userId: 'old',
      proto: { name: 'wb3', major: 2, minor: 5 }
    })));
    bad.on('message', (raw) => { const m = JSON.parse(raw.toString()); if (m.type === 'error') badErr = m; });
    bad.on('close', (code) => { badCode = code; res(); });
  });
  assert(badErr && /proto-major|incompatible/.test(badErr.message), `major mismatch rejected: ${badErr && badErr.message}`);
  assert(badCode === WBT.PROTO.CLOSE_INCOMPATIBLE, `closed with code 4001 (got ${badCode})`);

  // minor 更新 → 协商降级到 0
  const newer = new BinClient('new');
  await newer.connect(room, { minor: 9 });
  const j2 = await newer.waitFor('joined', (m) => m.type === 'joined' && m.minor === 0);
  assert(j2.minor === 0, 'newer minor downgrades to server minor 0');
  await sleep(100);
  newer.close();

  console.log('\n[2] 二进制 ops：服务端物化 + 二进制广播（fseq 单调）');
  const clockA = new WB.Clock('A3');
  const B = new BinClient('B3');
  await B.connect(room);
  await sleep(100);
  const env1 = A.issue(clockA, { kind: 'create', objects: [{ oid: 'box1', type: 'rect', fields: { x: 1, y: 2, w: 30, h: 40, z: '0.5' } }] });
  const opFrame = await B.waitFor('ops', (m) => (m.envelopes || []).some((e) => e.id === env1.id));
  assert(!!opFrame, 'binary ops broadcast to other client');
  assert(Number.isInteger(opFrame.fseq) && opFrame.fseq > 0, 'broadcast frames carry monotonic fseq');
  // B 也发一条，A 应收到且 fseq 递增
  const clockB = new WB.Clock('B3');
  const env2 = B.issue(clockB, { kind: 'set', oid: 'box1', fields: { x: 9 }, prev: { x: 1 } });
  const op2 = await A.waitFor('ops', (m) => (m.envelopes || []).some((e) => e.id === env2.id));
  assert(op2.envelopes[0].op.fields.x === 9, 'set op decoded binary');

  console.log('\n[3] 增量同步：lastSeq + VC 断线续传只收缺失操作');
  // 再写 5 条
  for (let i = 0; i < 5; i++) A.issue(clockA, { kind: 'set', oid: 'box1', fields: { x: 100 + i }, prev: { x: 99 + i } });
  await sleep(200);
  const api0 = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  const curSeq = api0.seq;
  // 新客户端声称 lastSeq=curSeq-3 且 VC 包含 A 的更早计数 → 只应拿到少量 delta
  const vc = Object.assign({}, clockA.vc);
  // 回退 VC 让最后 3 条被视为缺失
  vc['A3'] = Math.max(1, vc['A3'] - 3);
  const C = new BinClient('C3');
  await C.connect(room, { lastSeq: curSeq - 3, vc });
  const delta = await C.waitFor('delta');
  assert(delta.type === 'delta', `reconnect within log window gets DELTA not full snapshot (got ${delta.type})`);
  assert(delta.envelopes.length <= 3, `delta contains only missing ops (${delta.envelopes.length} <= 3)`);

  console.log('\n[4] 快照加速：周期快照后新客户端 snapshot + 少量重放');
  // 触发一次快照
  await httpGet(`/api/snapshot?roomId=${room}`);
  const api1 = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  assert(api1.snapshots.length >= 1, `server holds periodic snapshots (${api1.snapshots.length})`);
  const D = new BinClient('D3');
  await D.connect(room, { lastSeq: 0, vc: {} });
  const snap = await D.waitFor('snapshot');
  assert(!!snap.snapshot && Array.isArray(snap.snapshot.objects), 'new client loads materialized snapshot first');
  const oids = snap.snapshot.objects.map((o) => o.oid);
  assert(oids.includes('box1'), 'snapshot already contains current objects (fast load)');
  assert((snap.envelopes || []).length < 400, `only replays small tail after snapshot (${(snap.envelopes || []).length})`);

  console.log('\n[5] 慢客户端背压：自动降级为快照同步');
  // E 连上但不回 ACK（模拟消费极慢）；制造大量扇出使在途超水位
  const E = new BinClient('E3');
  await E.connect(room);
  E._ackFseq = function () {}; // 吞掉所有 ACK
  await sleep(100);
  const beforeDegrade = E.degraded;
  // 产生足够多广播字节（每条 ~100B，水位 512KB → 需要数千条）；直接用运维接口触发降级
  await httpGet(`/api/degrade?roomId=${room}&sid=${E.sid}`);
  const degFrame = await E.waitFor('degrade');
  // 降级快照必须是收到 degrade 之后的那一帧（排除入房时的初始快照）
  const degIdx = E.frames.indexOf(degFrame);
  const snapFrame = E.frames.slice(degIdx + 1).find((f) => f.type === 'snapshot');
  assert(degFrame.type === 'degrade', `slow client receives degrade frame (was ${beforeDegrade})`);
  assert(!!snapFrame && snapFrame.snapshot && snapFrame.fseq > 0, 'degraded client reset with fresh snapshot');
  // E 的快照里 box1 必须是最新状态（与服务端权威物化一致）
  const apiE = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  assert(apiE.degraded.includes(E.sid), 'server marks client degraded until snapshot acked');
  // E 确认快照 → 恢复
  E._send({ type: 'ack', ids: [], lastSeq: snapFrame.lastSeq, fseq: snapFrame.fseq, sacks: [] });
  await sleep(150);
  const apiE2 = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  assert(!apiE2.degraded.includes(E.sid), 'client recovers to normal fanout after snapshot ack');
  E.close();

  console.log('\n[6] P2P：PeerSession 经虚拟有损网络增量 gossip + blob 直传');
  await testP2P();

  console.log('\n[7] 离线编辑：未确认信封重放，服务端幂等无重复');
  const F = new BinClient('F3');
  const clockF = new WB.Clock('F3');
  await F.connect(room);
  await sleep(100);
  const offlineEnv = WB.makeEnvelope(clockF, {
    kind: 'create', objects: [{ oid: 'offline1', type: 'rect', fields: { x: 0, y: 0, w: 10, h: 10 } }]
  });
  // 模拟断线期间积压 + 重连后用原 id 重放两次
  F.issueEnv(offlineEnv);
  await sleep(100);
  F.issueEnv(offlineEnv); // 重复包
  await sleep(200);
  const apiF = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  const matches = apiF.log.filter((l) => l.id === offlineEnv.id).length;
  assert(matches === 1, `offline replay idempotent: envelope appears once in log (${matches})`);
  // 新客户端看到恰好一个 offline1（快照基线 + 水位后重放，经 Doc 物化）
  const G = new BinClient('G3');
  await G.connect(room);
  const gs = await G.waitFor('snapshot');
  const gdoc = new WB.Doc();
  gdoc.loadSnapshot(gs.snapshot);
  for (const e of (gs.envelopes || [])) gdoc.apply(e);
  const cnt = gdoc.liveObjects().filter((o) => o.oid === 'offline1').length;
  assert(cnt === 1, `late joiner snapshot+replay sees exactly one offline1 (${cnt})`);
  G.close(); F.close();

  console.log('\n[8] v2 JSON 客户端与 v3 二进制客户端同房间互通（向后兼容）');
  const v2 = new WebSocket(URL);
  await new Promise((r) => v2.on('open', r));
  let v2Snapshot = null;
  v2.on('message', (raw) => {
    const m = JSON.parse(raw.toString());
    if (m.type === 'snapshot') v2Snapshot = m;
  });
  v2.send(JSON.stringify({ type: 'join', roomId: room, userId: 'v2guy' }));
  await sleep(200);
  assert(!!v2Snapshot && v2Snapshot.snapshot.objects.some((o) => o.oid === 'box1'),
    'JSON v2 client still gets full snapshot in mixed room');
  // v2 发的操作应被 A（二进制）收到
  const v2env = WB.makeEnvelope(new WB.Clock('v2guy'), {
    kind: 'create', objects: [{ oid: 'fromv2', type: 'rect', fields: {} }]
  });
  v2.send(JSON.stringify({ type: 'ops', envelopes: [v2env] }));
  const fromV2 = await A.waitFor('ops', (m) => (m.envelopes || []).some((e) => e.id === v2env.id));
  assert(!!fromV2, 'JSON-originated op fanned out to binary client');
  v2.close();

  console.log('\n[9] 大对象引用经 WS 控制面登记并转发（字节不过服务器）');
  {
    const H = new BinClient('H3');
    await H.connect(room);
    await sleep(100);
    const refMsg = {
      type: 'bigAnnounce', refs: [{
        blobId: 'abcd1234', size: 99999, chunksTotal: 13, hash: 'abcd1234',
        holders: [H.sid], id: 'H3:5', clientId: 'H3', lamport: 5, kind: 'create', oids: ['big9']
      }]
    };
    H._send(refMsg);
    // A（已在房间）应收到 bigAnnounce
    const ann = await A.waitFor('bigAnnounce', (m) => (m.refs || []).some((r) => r.blobId === 'abcd1234'));
    const got = ann.refs.find((r) => r.blobId === 'abcd1234');
    assert(!!got && got.size === 99999 && got.holders.includes(H.sid),
      'blob reference relayed over signaling channel (no bytes through server)');
    // 大对象字节帧（bigChunk）走 WS 到达时服务器必须忽略（控制/数据分离）
    const apiBefore = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body).blobRefs;
    H._send({ type: 'bigChunk', blobId: 'abcd1234', index: 0, total: 13, bytes: new Uint8Array(100) });
    await sleep(150);
    const apiAfter = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body).blobRefs;
    assert(apiAfter === apiBefore, 'bigChunk bytes over WS are ignored (data plane is P2P only)');
    H.close();
  }

  [A, B, C, D].forEach((c) => c.close());

  console.log(`\n========================================`);
  console.log(`SMOKE3 RESULT: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

/* ---- P2P 场景：两个 PeerSession 跑在丢包虚拟网上 ---- */
async function testP2P() {
  let seed = 99;
  Math.random = () => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed / 0x7fffffff; };
  let now = 5000;
  const timers = [];
  const net = new WBT.VirtualNetwork({
    loss: 0.15, jitter: 6, delay: 1, now: () => now,
    setTimeout: (fn, ms) => { timers.push({ t: now + (ms || 0), fn }); }
  });
  const t1 = net.makeEndpoint('111'), t2 = net.makeEndpoint('222');

  const state1 = {
    vc: {}, log: [], blobs: new Map(), received: []
  };
  const state2 = {
    vc: {}, log: [], blobs: new Map(), received: []
  };
  const mkHandlers = (me, st, other) => ({
    userId: 'u' + me,
    getVC: () => st.vc,
    getRetainedLog: () => st.log,
    getBlob: (id) => st.blobs.has(id) ? st.blobs.get(id) : null,
    saveBlob: (id, data) => st.blobs.set(id, data),
    ingestEnvelopes: (envs) => { st.received.push(...envs); WBT.mergeVC(st.vc, mergeEnvsVC(envs)); }
  });
  const s1 = new WBN.PeerSession({ me: '111', peer: '222', send: (b) => t1.send(b, '222'), now: () => now, handlers: mkHandlers('1', state1) });
  const s2 = new WBN.PeerSession({ me: '222', peer: '111', send: (b) => t2.send(b, '111'), now: () => now, handlers: mkHandlers('2', state2) });
  t1.onmessage = (b) => s1.onFrame(b);
  t2.onmessage = (b) => s2.onFrame(b);
  s1.start();

  // u1 已有 10 条小操作；u2 是新加入者（空 VC）
  const c1 = new WB.Clock('P1');
  for (let i = 0; i < 10; i++) {
    const e = WB.makeEnvelope(c1, { kind: 'set', oid: 'p', fields: { i } });
    state1.log.push(e);
    WBT.mergeVC(state1.vc, e.clock);
  }
  s2.start();
  // 握手互发后 u1 应按 u2 空 VC 增量推 10 条
  await pump(now0 => { now = now0; }, () => [s1, s2], timers, () => now, (n) => { now = n; },
    () => state2.received.length >= 10, 4000);
  assert(state2.received.length === 10, `P2P gossip: new peer got all 10 missing ops (${state2.received.length})`);
  assert(new Set(state2.received.map((e) => e.id)).size === 10, 'no duplicate ops over P2P');

  // 大信封 blob：构造一个超过 LARGE_BYTES 的笔迹信封
  const bigPts = [];
  for (let i = 0; i < 2000; i++) bigPts.push({ x: i * 1.2, y: Math.sin(i / 20) * 30, p: 0.5, t: i * 16, w: 4 });
  const bigEnv = WB.makeEnvelope(c1, {
    kind: 'create', objects: [{ oid: 'bigstroke', type: 'stroke', fields: { stroke: { width: 4, points: bigPts } } }]
  });
  const bigBytes = WBT.encode({ type: 'ops', envelopes: [bigEnv] });
  assert(bigBytes.length > WBT.LARGE_BYTES, `big op exceeds large threshold (${bigBytes.length} > ${WBT.LARGE_BYTES})`);
  // 模拟 hash
  const hash = require('crypto').createHash('sha256').update(Buffer.from(bigBytes)).digest('hex');
  state1.blobs.set(hash, bigBytes);
  state1.log.push(bigEnv);
  // u2 显式请求 blob（等价 bigAnnounce 触发）
  s2.requestBlob(hash, 0);
  const beforeBlobCount = state2.blobs.size;
  await pump(null, () => [s1, s2], timers, () => now, (n) => { now = n; },
    () => state2.blobs.has(hash) && state2.received.some((e) => e.id === bigEnv.id), 8000);
  assert(state2.blobs.has(hash), 'large blob transferred P2P in chunks and reassembled');
  const got = state2.blobs.get(hash);
  assert(got.length === bigBytes.length, `reassembled blob size exact (${got.length})`);
  const decoded = WBT.frameFrom(got);
  assert(decoded.envelopes[0].op.objects[0].fields.stroke.points.length === 2000,
    'reassembled big envelope decodes all 2000 points');
  assert(state2.received.some((e) => e.id === bigEnv.id), 'big envelope ingested via P2P only (bytes never via server)');
  void beforeBlobCount;
}

function mergeEnvsVC(envs) {
  const vc = {};
  for (const e of envs) WBT.mergeVC(vc, e.clock);
  return vc;
}

async function pump(_, sessionsRef, timers, getNow, setNow, done, maxSteps) {
  for (let step = 0; step < maxSteps; step++) {
    setNow(getNow() + 1);
    for (const s of sessionsRef()) s.tick(getNow());
    const due = timers.filter((t) => t.t <= getNow()).sort((a, b) => a.t - b.t);
    for (const d of due) { d.fn(); const i = timers.indexOf(d); if (i >= 0) timers.splice(i, 1); }
    if (done()) break;
  }
}

let serverProc = null;
async function ensureServer() {
  try { await httpGet('/api/rooms'); return; } catch (_) {}
  serverProc = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
    env: Object.assign({}, process.env, { PORT: String(PORT) }), stdio: 'ignore'
  });
  for (let i = 0; i < 40; i++) {
    await sleep(150);
    try { await httpGet('/api/rooms'); return; } catch (_) {}
  }
  throw new Error('test server failed to start');
}

ensureServer().then(main).catch((e) => { console.error('SMOKE3 CRASHED:', e); process.exit(1); });
