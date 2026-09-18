'use strict';
/*
 * v2 协议冒烟测试（需先启动服务端，脚本会自动拉起 PORT 上的 node server.js）。
 * 覆盖验收场景：
 *  1. 三客户端并发编辑同一对象/区域 → 最终一致、无重复
 *  2. A 撤销自己旧操作，B 已修改 → A 的撤销不破坏 B
 *  3. 多对象移动事务：其他端要么全看到，要么看不到
 *  4. 压感笔迹（点含 p/t/w）两端一致
 *  5. 橡皮擦分块两端一致
 *  另：快照/晚加入、幂等 ack、房间隔离、非法信封、压缩日志、心跳。
 */
const WebSocket = require('ws');
const http = require('http');
const { spawn } = require('child_process');
const path = require('path');
const WB = require('./public/kernel.js');

const PORT = process.env.PORT || 8080;
const URL = `ws://localhost:${PORT}/ws`;
let passed = 0, failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; console.log('  PASS -', msg); }
  else { failed++; console.error('  FAIL -', msg); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function connect() { return new WebSocket(URL); }
function send(ws, o) { ws.send(JSON.stringify(o)); }
function waitMsg(ws, type, pred, timeout = 3000) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('timeout ' + type)), timeout);
    ws.on('message', function on(raw) {
      let m; try { m = JSON.parse(raw.toString()); } catch (_) { return; }
      if (m.type === type && (!pred || pred(m))) { clearTimeout(t); ws.off('message', on); resolve(m); }
    });
  });
}

async function joinAs(room, userId) {
  const ws = connect();
  await new Promise((r) => ws.on('open', r));
  const pj = waitMsg(ws, 'joined');
  const ps = waitMsg(ws, 'snapshot');
  send(ws, { type: 'join', roomId: room, userId });
  const joined = await pj, snap = await ps;
  return { ws, joined, snap };
}

/** 一个带独立 CRDT 状态的客户端 */
function makeClient(userId, room) {
  const clock = new WB.Clock(userId);
  const buf = new WB.CausalBuffer();
  const doc = new WB.Doc();
  return {
    userId, clock, buf, doc,
    ws: null,
    async connect() {
      this.ws = connect();
      await new Promise((r) => this.ws.on('open', r));
      const ps = waitMsg(this.ws, 'snapshot');
      send(this.ws, { type: 'join', roomId: room, userId });
      const snapMsg = await ps;
      this.doc.loadSnapshot(snapMsg.snapshot);
      for (const k of Object.keys(snapMsg.snapshot.known || {})) this.buf.known[k] = snapMsg.snapshot.known[k];
      for (const env of snapMsg.envelopes || []) this._ingest(env);
      this.ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m.type === 'ops') for (const env of m.envelopes) this._ingest(env);
      });
      await sleep(50);
    },
    _ingest(env) {
      const ready = this.buf.push(env);
      for (const e of ready) {
        this.clock.observeLamport(e.lamport);
        this.clock.mergeVC(e.clock);
        this.doc.apply(e);
      }
    },
    /** 本地乐观提交 + 发送 */
    issue(op, opts) {
      const env = WB.makeEnvelope(this.clock, op, opts);
      this._ingest(env);
      send(this.ws, { type: 'ops', envelopes: [env] });
      return env;
    },
    issueBatch(envs) {
      if (envs.length > 1) WB.atomic(envs);
      for (const e of envs) this._ingest(e);
      send(this.ws, { type: 'ops', envelopes: envs });
      return envs;
    },
    close() { try { this.ws.close(); } catch (_) {} }
  };
}

function httpGet(p) {
  return new Promise((resolve, reject) => {
    http.get(`http://localhost:${PORT}${p}`, (res) => {
      let b = ''; res.on('data', (d) => (b += d)); res.on('end', () => resolve({ status: res.statusCode, body: b }));
    }).on('error', reject);
  });
}

async function main() {
  const room = 'v2-' + Date.now();

  console.log('\n[1] 三客户端并发：创建/移动同一对象，乱序广播后收敛，无重复对象');
  const A = makeClient('A', room), B = makeClient('B', room), C = makeClient('C', room);
  await A.connect(); await B.connect(); await C.connect();

  const create = A.issue({ kind: 'create', objects: [{ oid: 'shared1', type: 'rect', fields: { x: 0, y: 0, w: 50, h: 50 } }] });
  await sleep(150); // 广播到 B、C
  // B、C 基于同一状态并发 set x（真并发：两条信封时钟互不含对方）
  const moveB = B.issue({ kind: 'set', oid: 'shared1', fields: { x: 11 }, prev: { x: 0 } });
  const moveC = C.issue({ kind: 'set', oid: 'shared1', fields: { x: 22 }, prev: { x: 0 } });
  await sleep(200);
  // 三端最终 x 必须一致（LWW：lamport 相同（都为2）时 clientId 大的胜 → C 的 22）
  const xa = A.doc.get('shared1').x, xb = B.doc.get('shared1').x, xc = C.doc.get('shared1').x;
  assert(xa === xb && xb === xc, `concurrent edits converge x=${xa} (A=${xa},B=${xb},C=${xc})`);
  // 三端各自画一笔（不同 oid），无重复
  A.issue({ kind: 'create', objects: [{ oid: 'sA', type: 'stroke', fields: { stroke: { width: 3, points: [{ x: 1, y: 1 }, { x: 2, y: 2 }] } } }] });
  B.issue({ kind: 'create', objects: [{ oid: 'sB', type: 'stroke', fields: { stroke: { width: 3, points: [{ x: 3, y: 3 }, { x: 4, y: 4 }] } } }] });
  C.issue({ kind: 'create', objects: [{ oid: 'sC', type: 'stroke', fields: { stroke: { width: 3, points: [{ x: 5, y: 5 }, { x: 6, y: 6 }] } } }] });
  await sleep(250);
  for (const [name, cl] of [['A', A], ['B', B], ['C', C]]) {
    const ids = cl.doc.liveObjects().map((o) => o.oid).sort().join(',');
    assert(ids === 'sA,sB,sC,shared1', `${name} sees exactly one copy of every object (${ids})`);
  }

  console.log('\n[2] 选择性撤销：A 撤销自己旧的移动，B 已在其上修改 → B 的结果保留');
  const D = makeClient('D', room), E = makeClient('E', room);
  await D.connect(); await E.connect();
  const create2 = D.issue({ kind: 'create', objects: [{ oid: 'undo1', type: 'rect', fields: { x: 0, y: 0, w: 10, h: 10 } }] });
  await sleep(150);
  const moveD = D.issue({ kind: 'set', oid: 'undo1', fields: { x: 40 }, prev: { x: 0 } });
  await sleep(150);
  const moveE = E.issue({ kind: 'set', oid: 'undo1', fields: { x: 77 }, prev: { x: 40 } });
  await sleep(150);
  // D 端撤销 moveD（用 UndoManager 生成逆操作）
  const um = new WB.UndoManager(D.clock);
  um.record([moveD], 'move');
  // undo 返回「事务组的数组」[[env,...]]，真实客户端 dispatchInverse 会 flat()
  const [inv] = um.undo(new Map([[moveD.id, moveD]])).flat();
  D._ingest(inv); send(D.ws, { type: 'ops', envelopes: [inv] });
  await sleep(200);
  assert(D.doc.get('undo1').x === 77, `D's undo void: E result x=77 kept locally (got ${D.doc.get('undo1').x})`);
  assert(E.doc.get('undo1').x === 77, `E keeps x=77 after D's undo broadcast (got ${E.doc.get('undo1').x})`);

  console.log('\n[3] 原子事务：一次移动多个对象，其它端整组可见');
  const objs = [];
  for (let i = 0; i < 5; i++) {
    objs.push({ oid: 'tx' + i, type: 'rect', fields: { x: i * 20, y: 0, w: 10, h: 10 } });
  }
  A.issue({ kind: 'create', objects: objs });
  await sleep(200);
  const moves = objs.map((o, i) => WB.makeEnvelope(A.clock,
    { kind: 'set', oid: o.oid, fields: { x: 200 + i * 20 }, prev: { x: o.fields.x } }));
  WB.atomic(moves);
  // A 自己必然先本地物化整组事务（真实客户端行为）
  for (const e of moves) A._ingest(e);
  // 但向服务端乱序发送（先后半），验证其他端 B 在事务不完整时不会看到“移动一半”
  send(A.ws, { type: 'ops', envelopes: moves.slice(3) });
  await sleep(120);
  const partial = B.doc.liveObjects().filter((o) => o.oid.startsWith('tx')).map((o) => Math.round(o.x));
  const movedCount = partial.filter((x) => x >= 200).length;
  assert(movedCount === 0, `B shows 0/5 moved while txn incomplete (got ${movedCount}/5)`);
  send(A.ws, { type: 'ops', envelopes: moves.slice(0, 3) });
  await sleep(250);
  const all = B.doc.liveObjects().filter((o) => o.oid.startsWith('tx')).map((o) => Math.round(o.x)).sort((p, q) => p - q);
  assert(all.length === 5 && all[0] === 200 && all[4] === 280, `B atomically sees all 5 moved [200..280] (${all.join(',')})`);

  console.log('\n[4] 压感笔迹：点集携带 p/t/w，晚加入端渲染数据逐点一致');
  const pts = [];
  for (let i = 0; i < 12; i++) pts.push({ x: i * 4, y: 100 + Math.sin(i) * 6, p: 0.2 + (i % 5) * 0.15, t: i * 16 });
  const widths = WB.computeWidths(pts, 6);
  const richPts = pts.map((p, i) => ({ x: p.x, y: round1(p.y), p: round2(p.p), t: p.t, w: round2(widths[i]) }));
  A.issue({ kind: 'create', objects: [{ oid: 'pressure1', type: 'stroke', fields: { stroke: { brush: 'pen', width: 6, smooth: 'catmull', cellSize: 6, points: richPts } } }] });
  await sleep(200);
  const F = makeClient('F', room); await F.connect();
  const remoteStroke = F.doc.get('pressure1').stroke;
  assert(JSON.stringify(remoteStroke.points) === JSON.stringify(richPts), 'pressure/timestamp/width points identical on late joiner');
  const wA = A.doc.get('pressure1').stroke.points.map((p) => p.w);
  const wF = remoteStroke.points.map((p) => p.w);
  assert(JSON.stringify(wA) === JSON.stringify(wF), `width profile identical both ends (${wA.slice(0, 3).join(',')}…)`);

  console.log('\n[5] 橡皮擦分块：像素擦除只同步受影响块，两端擦除单元一致');
  const strokePts = [];
  for (let i = 0; i <= 40; i++) strokePts.push({ x: i * 6, y: 300 });
  A.issue({ kind: 'create', objects: [{ oid: 'eraseMe', type: 'stroke', fields: { stroke: { width: 8, cellSize: 8, points: strokePts } } }] });
  await sleep(200);
  const erasePath = [{ x: 30, y: 300 }, { x: 90, y: 300 }, { x: 150, y: 300 }];
  const { chunks } = WB.rasterizeErase(erasePath, 8);
  assert(chunks.length >= 1, `erase path rasterized into tiles (${chunks.length})`);
  A.issue({ kind: 'erase', chunks: chunks.map((c) => Object.assign({ oid: 'eraseMe' }, c)) });
  await sleep(250);
  const cellsA = [...A.doc.erasedCells('eraseMe')].sort();
  const cellsB = [...B.doc.erasedCells('eraseMe')].sort();
  const cellsF = [...F.doc.erasedCells('eraseMe')].sort();
  assert(cellsA.length > 0, `some cells erased (${cellsA.length})`);
  assert(JSON.stringify(cellsA) === JSON.stringify(cellsB) && JSON.stringify(cellsB) === JSON.stringify(cellsF),
    'erased cell set identical on A/B/F');

  console.log('\n[5b] 两人同块像素擦 + 选择性撤销：只恢复 A 的格子，B 的保留（含晚加入快照）');
  A.issue({ kind: 'create', objects: [{ oid: 'eraseShared', type: 'stroke',
    fields: { stroke: { width: 8, cellSize: 8, points: [{ x: 0, y: 0 }, { x: 100, y: 100 }] } } }] });
  await sleep(200);
  // A 与 B 先后擦同一 tile(0,0) 内不同单元
  const eraseA2 = A.issue({ kind: 'erase', chunks: [{ oid: 'eraseShared', tx: 0, ty: 0,
    cells: [[0, 0], [1, 1]] }] });
  await sleep(150);
  const eraseB2 = B.issue({ kind: 'erase', chunks: [{ oid: 'eraseShared', tx: 0, ty: 0,
    cells: [[2, 2], [3, 3]] }] });
  await sleep(150);
  // A 撤销自己的擦除（经服务端广播给 B 及其他成员）
  const um2 = new WB.UndoManager(A.clock);
  um2.record([eraseA2], 'pixel erase');
  // undo 返回「事务组的数组」[[env,...]]，真实客户端 dispatchInverse 会 flat()
  const undoErase = um2.undo(new Map([[eraseA2.id, eraseA2]])).flat();
  for (const e of undoErase) A._ingest(e);
  send(A.ws, { type: 'ops', envelopes: undoErase });
  await sleep(250);
  const wantErased = ['0:0,2,2', '0:0,3,3'];
  const eA = [...A.doc.erasedCells('eraseShared')].sort();
  const eB = [...B.doc.erasedCells('eraseShared')].sort();
  assert(JSON.stringify(eA) === JSON.stringify(wantErased),
    `A undo restores only A cells, keeps B cells (A=${eA.join(',')})`);
  assert(JSON.stringify(eB) === JSON.stringify(wantErased),
    `broadcast undo applies selectively on B too (B=${eB.join(',')})`);
  // 撤销之后晚加入者从服务端快照得到完全一致的单元状态
  const H = makeClient('H', room); await H.connect();
  const eH = [...H.doc.erasedCells('eraseShared')].sort();
  assert(JSON.stringify(eH) === JSON.stringify(wantErased),
    `late joiner snapshot agrees after selective erase-undo (H=${eH.join(',')})`);
  H.close();

  console.log('\n[6] 晚加入快照 + 幂等 + 房间隔离 + 非法消息 + 压缩');
  const api = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  assert(api.liveCount >= 5, `server materializes live objects (${api.liveCount})`);
  assert(api.log.every((l) => Number.isInteger(l.lamport) && l.clientId), 'log entries carry clientId + lamport; seq is ordering-only');

  // 重发相同信封：ack 返回相同 id，不新增日志
  const beforeSeq = api.seq;
  send(A.ws, { type: 'ops', envelopes: [create] });
  await sleep(150);
  const api2 = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  assert(api2.seq === beforeSeq, `duplicate envelope idempotent, seq unchanged ${beforeSeq}->${api2.seq}`);

  // 房间隔离
  const other = connect();
  await new Promise((r) => other.on('open', r));
  let leaked = 0;
  other.on('message', (raw) => { const m = JSON.parse(raw.toString()); if (m.type === 'ops') leaked++; });
  send(other, { type: 'join', roomId: room + '-x', userId: 'X' });
  await sleep(100);
  A.issue({ kind: 'create', objects: [{ oid: 'isolated', type: 'rect', fields: {} }] });
  await sleep(200);
  assert(leaked === 0, 'ops not broadcast across rooms');
  other.close();

  // 非法信封
  const bad = connect();
  await new Promise((r) => bad.on('open', r));
  send(bad, { type: 'join', roomId: room, userId: 'badguy' });
  await sleep(100);
  const errP = waitMsg(bad, 'error');
  send(bad, { type: 'ops', envelopes: [{ id: 'bad:1', clientId: 'badguy', lamport: 1, clock: { badguy: 1 }, op: { kind: 'nope' } }] });
  const err = await errP;
  assert(/bad op kind/.test(err.message), 'invalid op kind rejected');
  // 伪造他人 clientId 被拒
  const errP2 = waitMsg(bad, 'error');
  const fake = WB.makeEnvelope(new WB.Clock('someoneelse'),
    { kind: 'create', objects: [{ oid: 'x', type: 'rect', fields: {} }] });
  send(bad, { type: 'ops', envelopes: [fake] });
  const err2 = await errP2;
  assert(/clientId mismatch/.test(err2.message), 'cannot submit ops under another clientId');
  bad.close();

  // 压缩：30 条同 squashKey 的移动，经 /api/compact 折叠后日志里只剩 1 条最终状态
  for (let i = 0; i < 30; i++) {
    A.issue({ kind: 'set', oid: 'shared1', fields: { x: 100 + i }, prev: { x: 99 + i } }, { squashKey: 'move:shared1' });
  }
  await sleep(250);
  const compacted = JSON.parse((await httpGet(`/api/compact?roomId=${room}`)).body);
  assert(compacted.before - compacted.after >= 29, `server squash collapsed log ${compacted.before} -> ${compacted.after}`);
  const api3 = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body);
  const squashLogs = api3.log.filter((l) => l.squashKey === 'move:shared1');
  assert(squashLogs.length === 1, `squashed move keeps one log entry (got ${squashLogs.length})`);
  // 压缩后晚加入者仍拿到最终 x=129
  const G = makeClient('G', room); await G.connect();
  assert(G.doc.get('shared1').x === 129, `late joiner after compaction sees final x=129 (got ${G.doc.get('shared1').x})`);
  G.close();

  // 心跳
  const pong = waitMsg(A.ws, 'pong');
  send(A.ws, { type: 'ping' });
  assert(!!(await pong), 'ping/pong');

  [A, B, C, D, E, F].forEach((c) => c.close());

  console.log(`\n========================================`);
  console.log(`SMOKE RESULT: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}
const round1 = (v) => Math.round(v * 10) / 10;
const round2 = (v) => Math.round(v * 100) / 100;

let serverProc = null;
async function ensureServer() {
  try {
    await httpGet('/api/rooms');
    console.log('using already-running server on', PORT);
    return;
  } catch (_) { /* start */ }
  serverProc = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
    env: Object.assign({}, process.env, { PORT: String(PORT) }),
    stdio: 'ignore'
  });
  for (let i = 0; i < 40; i++) {
    await sleep(150);
    try { await httpGet('/api/rooms'); console.log('spawned test server on', PORT); return; } catch (_) {}
  }
  throw new Error('test server failed to start');
}

ensureServer().then(main).catch((err) => { console.error('SMOKE CRASHED:', err); process.exit(1); });
