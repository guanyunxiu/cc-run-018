'use strict';
/*
 * v3 协议冒烟测试（需先启动服务端，脚本自动拉起 PORT 上的 node server.js）。
 * 覆盖：
 *  1. HELLO 版本协商：正常、major 不一致拒绝、minor 偏高降级
 *  2. 二进制 JOIN（lastSeq+VC）→ 快照；二进制 OPS/ACK；重复信封幂等
 *  3. 增量同步：客户端带 lastSeq+VC 重连，只收到缺失信封（纯 delta，无快照）
 *  4. 快照链：每 100 操作出快照，晚加入者“快照 + 少量重放”
 *  5. 慢客户端背压：阻塞消费 → MODE=1 降级；REQ_SYNC → 快照补齐 → MODE=0
 *  6. v2(JSON) 与 v3(二进制) 客户端同房间共存，双向广播可见
 *  7. 媒体中继 PUT + MEDIA_REQ 按 offset 断点续传
 *  8. RTC SDP/ICE 信令经 WS 在 v3 成员间转发
 *  9. OfflineStore：未确认信封持久化 + 重连合并（内存适配器）
 */
const WebSocket = require('ws');
const http = require('http');
const { spawn } = require('child_process');
const path = require('path');
const WB = require('./public/kernel.js');
const N = require('./public/net.js');
const MT = N.MT;

const PORT = process.env.PORT || 8093;
// 测试默认让系统分配空闲端口（PORT=0），从子进程日志解析实际端口，
// 避免沙箱/CI 中残留进程占用固定端口导致 EADDRINUSE。
const AUTO_PORT = process.env.PORT ? false : true;
let actualPort = PORT;
const WS_URL = () => `ws://localhost:${actualPort}/ws`;
let passed = 0, failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; console.log('  PASS -', msg); }
  else { failed++; console.error('  FAIL -', msg); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ----------------------- 二进制 v3 客户端 ----------------------- */

async function openSocket() {
  const ws = new WebSocket(WS_URL());
  ws.binaryType = 'nodebuffer';
  await new Promise((r, e) => { ws.on('open', r); ws.on('error', e); });
  return ws;
}

/**
 * @param {object} opts {major,minor,join:{roomId,userId,lastSeq,vc}, slow}
 *   slow: 暂停 Link 对 OPS 的交付（模拟慢客户端，服务端 Outbox 堆积）
 */
async function v3Client(opts) {
  opts = opts || {};
  const ws = await openSocket();
  // slowLink：抑制本端所有上行帧（ACK/NACK/PONG），真实模拟慢客户端——
  // 服务端发送窗口被在途帧占满，触发 Outbox 快照降级。
  let uplinkOn = !opts.slowLink;
  const t = new N.StreamTransport({
    send(u8) {
      if (!uplinkOn) return true; // 吞掉，服务端收不到 ACK
      ws.send(Buffer.from(u8.buffer, u8.byteOffset, u8.byteLength));
      return true;
    }
  });
  ws.on('message', (raw, isBinary) => {
    if (isBinary || (raw.length && raw[0] >= 0x80)) t.feed(new Uint8Array(raw.buffer, raw.byteOffset, raw.length));
  });
  const link = new N.Link(t, { pingInterval: 0, maxRetries: 50 });
  const client = {
    ws, link,
    userId: opts.userId, roomId: opts.roomId,
    clock: new WB.Clock(opts.userId),
    buf: new WB.CausalBuffer(),
    doc: new WB.Doc(),
    messages: [],
    envelopes: [],
    lastSeq: (opts.join && opts.join.lastSeq) || 0,
    modeEvents: [],
    closed: false,
    pauseOps: !!opts.slow,
    resumeUplink() { uplinkOn = true; },
    _uplinkOff() { uplinkOn = false; },
    send(type, msg) { link.send(type, msg); },
    close() { this.closed = true; try { link.close(); } catch (_) {} try { ws.close(); } catch (_) {} },

    async hello(major, minor) {
      return new Promise((resolve) => {
        link.on('message', function on(type, msg) {
          if (type === MT.WELCOME) { link.off('message', on); resolve(msg); }
        });
        link.send(MT.HELLO, { name: N.PROTO.name, major: major | 0, minor: minor | 0, session: 1234 });
      });
    },

    async join(joinMsg) {
      const welcome = await this.hello(opts.major || N.PROTO.major, opts.minor || N.PROTO.minor);
      const got = { welcome, snapshot: null, joined: null };
      await new Promise((resolve) => {
        const on = (type, msg) => {
          if (type === MT.JOINED) { got.joined = msg; this.lastSeq = msg.lastSeq; }
          if (type === MT.SNAPSHOT) {
            got.snapshot = msg;
            this._consumeSnapshot(msg);
            link.off('message', on);
            resolve(got);
          }
        };
        link.on('message', on);
        link.send(MT.JOIN, Object.assign({
          roomId: this.roomId, userId: this.userId,
          lastSeq: this.lastSeq, vc: {}, relay: 1
        }, joinMsg || {}));
      });
      this._attachOps();
      return got;
    },

    _consumeSnapshot(msg) {
      if (msg.hasSnapshot && msg.snapshot) {
        this.doc.loadSnapshot(msg.snapshot);
        for (const k of Object.keys(msg.snapshot.known || {})) this.buf.known[k] = msg.snapshot.known[k];
      }
      this._ingest(msg.envelopes || []);
      this.lastSeq = msg.lastSeq;
    },

    _ingest(envs) {
      this.buf.enqueue(envs);
      const ready = this.buf.drain();
      for (const e of ready) {
        this.clock.observeLamport(e.lamport);
        this.clock.mergeVC(e.clock);
        this.doc.apply(e);
      }
      if (ready.length) this.envelopes.push(...ready);
      return ready;
    },

    _attachOps() {
      link.on('message', (type, msg) => {
        this.messages.push([type, msg]);
        if (type === MT.OPS && !this.pauseOps) this._ingest(msg.envelopes || []);
        if (type === MT.ACK) this.lastSeq = Math.max(this.lastSeq, msg.lastSeq | 0);
        if (type === MT.MODE) this.modeEvents.push(msg.mode);
        if (type === MT.SNAPSHOT) this._consumeSnapshot(msg);
      });
    },

    /** 本地签发+发送（乐观物化） */
    issue(op, o) {
      const env = WB.makeEnvelope(this.clock, op, o);
      this._ingest([env]);
      link.send(MT.OPS, { envelopes: [env] });
      return env;
    },

    waitFor(type, pred, timeout) {
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('timeout waiting ' + type)), timeout || 3000);
        const on = (t, m) => {
          if (t === type && (!pred || pred(m))) { clearTimeout(timer); link.off('message', on); resolve(m); }
        };
        link.on('message', on);
      });
    },
    wait(timeout) { return sleep(timeout || 200); }
  };
  return client;
}

/* ----------------------- v2 JSON 客户端 ----------------------- */

function v2Client(roomId, userId) {
  const ws = new WebSocket(WS_URL());
  const client = {
    ws, clock: new WB.Clock(userId), buf: new WB.CausalBuffer(), doc: new WB.Doc(),
    async start() {
      await new Promise((r) => ws.on('open', r));
      ws.send(JSON.stringify({ type: 'join', roomId, userId }));
      await new Promise((resolve) => {
        ws.on('message', (raw) => {
          const m = JSON.parse(raw.toString());
          if (m.type === 'snapshot') {
            this.doc.loadSnapshot(m.snapshot);
            this._ingest(m.envelopes || []);
            resolve();
          }
        });
      });
      ws.on('message', (raw) => {
        const m = JSON.parse(raw.toString());
        if (m.type === 'ops') this._ingest(m.envelopes || []);
      });
    },
    _ingest(envs) {
      this.buf.enqueue(envs);
      for (const e of this.buf.drain()) { this.clock.mergeVC(e.clock); this.doc.apply(e); }
    },
    issue(op) {
      const env = WB.makeEnvelope(this.clock, op);
      this._ingest([env]);
      ws.send(JSON.stringify({ type: 'ops', envelopes: [env] }));
      return env;
    },
    close() { try { ws.close(); } catch (_) {} }
  };
  return client;
}

function httpReq(method, p, body) {
  return new Promise((resolve, reject) => {
    const u = new URL('http://localhost:' + actualPort + p);
    const req = http.request({ hostname: '127.0.0.1', port: actualPort, path: u.pathname + u.search,
      method, headers: body ? { 'Content-Type': 'application/octet-stream', 'Content-Length': body.length } : {} },
    (res) => {
      const chunks = [];
      res.on('data', (d) => chunks.push(d));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}
const httpGet = (p) => httpReq('GET', p);

async function main() {
  const room = 'v3-' + Date.now();

  console.log('\n[1] 协议版本协商：接受 / major 拒绝 / minor 降级');
  {
    const c = await v3Client({});
    const w = await c.hello(N.PROTO.major, N.PROTO.minor);
    assert(w.ok === true && w.action === 0, `same version accepted (action=${w.action})`);
    c.close();
  }
  {
    const c = await v3Client({});
    const w = await c.hello(N.PROTO.major, N.PROTO.minor + 2);
    assert(w.ok === true && w.action === 1, `higher minor -> degrade (action=${w.action})`);
    c.close();
  }
  {
    const c = await v3Client({});
    const w = await c.hello(N.PROTO.major + 1, 0);
    assert(w.ok === false && w.action === 2, `major mismatch rejected (ok=${w.ok}, reason=${w.reason})`);
    c.close();
    await sleep(100);
  }

  console.log('\n[2] 二进制 JOIN + OPS + ACK + 幂等去重');
  const A = await v3Client({ roomId: room, userId: 'v3A' });
  await A.join();
  const B = await v3Client({ roomId: room, userId: 'v3B' });
  await B.join();
  const env = A.issue({ kind: 'create', objects: [{ oid: 'bin1', type: 'rect',
    fields: { x: 1, y: 2, w: 10, h: 10, color: '#123', z: '0.5' } }] });
  const ack = await A.waitFor(MT.ACK, (m) => m.ids.includes(env.id));
  assert(Array.isArray(ack.ids) && ack.lastSeq >= 1, `binary ACK with ids+lastSeq (seq=${ack.lastSeq})`);
  await sleep(150);
  assert(B.doc.get('bin1') && B.doc.get('bin1').x === 1, 'B materializes binary-broadcast object');

  // 重发同一信封（断线续传/重复包）：不新增
  const api1 = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body.toString());
  A.link.send(MT.OPS, { envelopes: [env] });
  await sleep(200);
  const api2 = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body.toString());
  assert(api2.seq === api1.seq, `duplicate envelope idempotent seq ${api1.seq}->${api2.seq}`);
  assert(B.doc.liveObjects().filter((o) => o.oid === 'bin1').length === 1, 'B keeps exactly one object copy');

  console.log('\n[3] 增量同步：带 lastSeq+VC 的 REQ_SYNC 只返回缺失 delta（无快照）');
  // 再制造 2 个操作；B 已有 seq=N-2 状态，请求增量
  await sleep(50);
  A.issue({ kind: 'create', objects: [{ oid: 'delta1', type: 'rect', fields: { x: 0, y: 0, w: 1, h: 1 } }] });
  await sleep(120);
  A.issue({ kind: 'create', objects: [{ oid: 'delta2', type: 'rect', fields: { x: 0, y: 0, w: 1, h: 1 } }] });
  await sleep(120);
  const seqAfter = JSON.parse((await httpGet(`/api/room?roomId=${room}`)).body.toString()).seq;
  // 新客户端 C 先全量加入，再模拟“落后”：用 REQ_SYNC 拉 lastSeq = seqAfter-2
  const C = await v3Client({ roomId: room, userId: 'v3C' });
  await C.join();
  assert(C.doc.get('delta1') && C.doc.get('delta2'), 'C full join sees both deltas');
  // 直接验证 planSync 纯增量路径：新建 D 只 join 一次（lastSeq=0），再用已知 seq 请求
  const D = await v3Client({ roomId: room, userId: 'v3D' });
  const dJoin = await D.join();
  const baseSeq = dJoin.joined.lastSeq;
  A.issue({ kind: 'create', objects: [{ oid: 'delta3', type: 'rect', fields: { x: 0, y: 0, w: 1, h: 1 } }] });
  await sleep(200);
  const syncP = D.waitFor(MT.SNAPSHOT, null, 2000);
  D.link.send(MT.REQ_SYNC, { lastSeq: baseSeq, vc: D.clock.vc });
  const sync = await syncP;
  assert(sync.hasSnapshot === false, 'REQ_SYNC returns delta only (hasSnapshot=0)');
  assert(sync.envelopes.some((e) => e.op.objects && e.op.objects[0].oid === 'delta3'),
    'delta payload contains the missing envelope');
  assert(D.doc.get('delta3'), 'D materializes missing op after delta sync');

  console.log('\n[4] 快照链：每 100 操作拍快照；晚加入者“快照+少量重放”');
  const snapRoom = 'v3snap-' + Date.now();
  const S = await v3Client({ roomId: snapRoom, userId: 'snapA' });
  await S.join();
  for (let i = 0; i < 105; i++) {
    S.issue({ kind: 'create', objects: [{ oid: 'so' + i, type: 'rect',
      fields: { x: i, y: 0, w: 1, h: 1, z: '0.' + String(1000 + i) } }] });
  }
  await sleep(800);
  const snapApi = JSON.parse((await httpGet(`/api/room?roomId=${snapRoom}`)).body.toString());
  assert(snapApi.snapshots.length >= 1, `snapshot chain created (${JSON.stringify(snapApi.snapshots)})`);
  const L = await v3Client({ roomId: snapRoom, userId: 'snapLate' });
  const late = await L.join();
  assert(late.snapshot.hasSnapshot === true, 'late joiner receives snapshot baseline');
  const count = L.doc.liveObjects().length;
  assert(count === 105, `late joiner snapshot+replay sees all 105 objects (got ${count})`);
  // 重放信封数量应远小于 100（快照基线之后只有 5 条左右）
  assert(late.snapshot.envelopes.length <= 10,
    `snapshot accelerates join: only ${late.snapshot.envelopes.length} envelopes replayed`);
  S.close(); L.close();

  console.log('\n[5] 慢客户端背压：自动降级快照同步，再恢复流式');
  const slowRoom = 'v3slow-' + Date.now();
  const SA = await v3Client({ roomId: slowRoom, userId: 'slowA' });
  await SA.join();
  // 正常加入（JOIN/SNAPSHOT 期间上行通畅），随后切断上行 ACK 模拟慢客户端
  const Slow = await v3Client({ roomId: slowRoom, userId: 'slowGuy' });
  await Slow.join();
  let degraded = false;
  Slow.link.on('message', function watch(t, m) {
    if (t === MT.MODE && m.mode === 1) degraded = true;
  });
  // 切断上行：服务端窗口逐渐被在途帧占满
  Slow._uplinkOff && Slow._uplinkOff();
  // 大量小操作制造持续广播流：慢端零 ACK，约一个发送窗口后服务端对其降级；
  // 降级后慢端被跳过，SA 的编辑继续正常物化（验证背压隔离，不拖垮其他成员）。
  const TOTAL_OPS = 250;
  let issued = 0;
  for (let i = 0; i < TOTAL_OPS; i++) {
    SA.issue({ kind: 'create', objects: [{ oid: 'slow' + i, type: 'rect',
      fields: { x: i, y: 0, w: 2, h: 2 } }] });
    issued++; await sleep(4);
  }
  for (let i = 0; i < 50 && !degraded; i++) await sleep(20);
  assert(degraded, 'slow client auto-degraded to snapshot mode (MODE=1)');
  for (let i = issued; i < issued + 60; i++) {
    SA.issue({ kind: 'create', objects: [{ oid: 'post' + i, type: 'rect',
      fields: { x: i, y: 0, w: 2, h: 2 } }] });
    await sleep(4);
  }

  // 恢复上行并 REQ_SYNC：积压 ACK 冲刷，服务端补快照并 MODE=0 恢复流式。
  // 本段监听同时 ingest 积压期间到达的 OPS（与快照 delta 幂等，id 去重）。
  Slow.pauseOps = false;
  Slow.link.removeAllListeners && Slow.link.removeAllListeners('message');
  // 监听必须在恢复上行之前挂好，避免漏掉服务端随时下发的 MODE=0 控制帧。
  let caughtSnap = null;
  const resumeSnap = new Promise((res) => {
    const on = (t, m) => {
      if (t === MT.OPS) Slow._ingest(m.envelopes || []);
      if (t === MT.MODE) Slow.modeEvents.push(m.mode);
      if (t === MT.SNAPSHOT) {
        Slow._consumeSnapshot(m);
        const n = m.hasSnapshot && m.snapshot ? m.snapshot.objects.length : 0;
        // 不在这里 off：MODE=0 可靠帧排在快照之后，需保留监听继续接收
        if (n >= 100 && !caughtSnap) { caughtSnap = m; res(); }
      }
    };
    Slow.link.on('message', on);
    Slow._resumeListener = on;
  });
  Slow.resumeUplink();
  // 给服务端 RTO 重传 → 本端 ACK 一点时间，让窗口排空
  await sleep(1200);
  // REQ_SYNC 的快照对象数需明显超过“积压 OPS 量级”（>=100），作为补齐完成判据。
  Slow.link.send(MT.REQ_SYNC, { lastSeq: 0, vc: {} });
  await resumeSnap;
  void caughtSnap;
  // 等待可靠的 MODE=0（在快照之后同通道到达）
  for (let i = 0; i < 30 && !Slow.modeEvents.includes(0); i++) await sleep(50);
  Slow.link.off('message', Slow._resumeListener);
  // 再等一拍，让快照 delta 之后可能迟到的 OPS 完成因果投递
  await sleep(200);
  const liveN = Slow.doc.liveObjects().length;
  assert(liveN >= 50, `snapshot recovery catches slow client up (${liveN} objects)`);
  assert(Slow.modeEvents.includes(1) && Slow.modeEvents.includes(0),
    `mode transitioned stream->snapshot->stream (${JSON.stringify(Slow.modeEvents)})`);
  SA.close(); Slow.close();

  console.log('\n[6] v2 JSON 与 v3 二进制同房间共存，双向可见');
  const mixRoom = 'v3mix-' + Date.now();
  const v2 = v2Client(mixRoom, 'jsonGuy');
  await v2.start();
  const v3 = await v3Client({ roomId: mixRoom, userId: 'binGuy' });
  await v3.join();
  v2.issue({ kind: 'create', objects: [{ oid: 'fromV2', type: 'rect', fields: { x: 1, y: 1, w: 9, h: 9 } }] });
  await sleep(200);
  assert(v3.doc.get('fromV2'), 'v3 client receives v2 JSON-originated op');
  v3.issue({ kind: 'create', objects: [{ oid: 'fromV3', type: 'rect', fields: { x: 2, y: 2, w: 8, h: 8 } }] });
  await sleep(250);
  assert(v2.doc.get('fromV3'), 'v2 client receives v3 binary-originated op (server translates)');
  v2.close(); v3.close();

  console.log('\n[7] 媒体中继 + 按 offset 断点续传');
  const mediaRoom = 'v3media-' + Date.now();
  const MA = await v3Client({ roomId: mediaRoom, userId: 'mediaA' });
  await MA.join();
  const bytes = Buffer.alloc(40000);
  for (let i = 0; i < bytes.length; i++) bytes[i] = (i * 7) & 255;
  const put = await httpReq('PUT', `/api/media?roomId=${mediaRoom}&oid=img1`, bytes);
  assert(put.status === 200, 'PUT /api/media accepted');
  const meta = JSON.parse(put.body.toString());
  assert(meta.totalBytes === 40000, `media meta totalBytes=40000 (got ${meta.totalBytes})`);

  // 用 MEDIA_REQ 从 offset=20000 续传（模拟断线后本地已有前 20KB）。
  // WS 中继只发一个 16KB 引导块（last=false 表示后续走 P2P/HTTP），校验 offset/内容。
  const first = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('media timeout')), 4000);
    MA.link.on('message', function on(t, m) {
      if (t !== MT.MEDIA_DATA || m.mediaId !== meta.mediaId) return;
      clearTimeout(timer); MA.link.off('message', on); resolve(m);
    });
    MA.link.send(MT.MEDIA_REQ, { mediaId: meta.mediaId, offset: 20000 });
  });
  assert(first.offset === 20000, `media resumed at requested offset (got ${first.offset})`);
  let ok = true;
  for (let i = 0; i < first.bytes.length; i++) if (first.bytes[i] !== bytes[20000 + i]) { ok = false; break; }
  assert(ok, 'resumed chunk bytes match source at offset');

  console.log('\n[8] RTC SDP/ICE 信令经 WS 转发（DataChannel 建链信令不承载业务）');
  const rtcRoom = 'v3rtc-' + Date.now();
  const R1 = await v3Client({ roomId: rtcRoom, userId: 'rtc1' });
  await R1.join();
  const R2 = await v3Client({ roomId: rtcRoom, userId: 'rtc2' });
  await R2.join();
  await sleep(150); // 等 MEMBERS
  const sdpP = R2.waitFor(MT.RTC_SDP, (m) => m.from === 'rtc1', 2000);
  R1.link.send(MT.RTC_SDP, { from: 'rtc1', to: 'rtc2', sdp: JSON.stringify({ type: 'offer', sdp: 'v=0' }) });
  const sdp = await sdpP;
  assert(sdp.sdp.includes('offer'), 'SDP offer relayed rtc1 -> rtc2 over WS signaling');
  const iceP = R1.waitFor(MT.RTC_ICE, (m) => m.from === 'rtc2', 2000);
  R2.link.send(MT.RTC_ICE, { from: 'rtc2', to: 'rtc1', candidate: JSON.stringify({ candidate: 'cand:1' }) });
  const ice = await iceP;
  assert(ice.candidate.includes('cand:1'), 'ICE candidate relayed rtc2 -> rtc1');
  R1.close(); R2.close(); MA.close(); A.close(); B.close(); C.close(); D.close();

  console.log('\n[9] OfflineStore 离线编辑持久化 + 合并顺序');
  {
    const store = new N.OfflineStore(N.memoryStore(), 'off1');
    const clock = new WB.Clock('off1');
    const e1 = WB.makeEnvelope(clock, { kind: 'create', objects: [{ oid: 'a', type: 'rect', fields: {} }] });
    const e2 = WB.makeEnvelope(clock, { kind: 'create', objects: [{ oid: 'b', type: 'rect', fields: {} }] });
    await store.savePendingBatch([e1, e2]);
    const restored = await store.allPending();
    assert(restored.length === 2 && restored[0].id === e1.id && restored[1].id === e2.id,
      'unconfirmed envelopes survive reconnect, ordered by lamport');
    await store.removePendingById(e1.id);
    const rest = await store.allPending();
    assert(rest.length === 1 && rest[0].id === e2.id, 'ACK removes persisted envelope');
    await store.saveClock(clock);
    await sleep(550);
    const c2 = await store.loadClock();
    assert(c2 && c2.lamport === clock.lamport, `clock persisted (lamport=${c2.lamport})`);
  }

  console.log(`\n========================================`);
  console.log(`V3 SMOKE RESULT: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

let serverProc = null;
async function ensureServer() {
  // 让系统分配空闲端口（PORT=0），从子进程 stdout 的 LISTENING_PORT= 解析实际端口，
  // 避免沙箱/CI 残留进程占用固定端口导致 EADDRINUSE。
  const usePort = AUTO_PORT ? 0 : PORT;
  serverProc = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
    env: Object.assign({}, process.env, { PORT: String(usePort) }),
    stdio: ['ignore', 'pipe', 'pipe']
  });
  let portResolve;
  const portPromise = new Promise((r) => { portResolve = r; });
  serverProc.stdout.on('data', (d) => {
    const m = /LISTENING_PORT=(\d+)/.exec(d.toString());
    if (m) portResolve(parseInt(m[1], 10));
  });
  serverProc.stderr.on('data', (d) => { if (process.env.TEST_DEBUG) console.error('[srv]', d.toString().trim()); });
  serverProc.on('exit', (code) => { if (code !== 0 && code !== null) console.error('test server exited', code); });
  const timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('server port timeout')), 10000));
  actualPort = await Promise.race([portPromise, timeout]);
  // 等待 HTTP 就绪
  let lastErr = '';
  for (let i = 0; i < 50; i++) {
    try {
      const r = await httpGet('/api/rooms');
      if (r.status === 200 && Array.isArray(JSON.parse(r.body.toString()))) {
        console.log('spawned v3 test server on', actualPort);
        return;
      }
    } catch (e) { lastErr = e.message; }
    await sleep(100);
  }
  console.error('DEBUG actualPort=', actualPort, 'lastErr=', lastErr);
  throw new Error('test server failed to become ready');
}

ensureServer().then(main).catch((err) => { console.error('V3 SMOKE CRASHED:', err); process.exit(1); });
