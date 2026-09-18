'use strict';
/*
 * v3 传输层单元测试（无需服务端、零网络依赖）：
 *  - 二进制协议：全 op kind 信封/快照/点云/分块 roundtrip；Float32 坐标 / Uint32 时间；
 *    体积显著小于 JSON；魔数/坏帧拒绝
 *  - 协议版本化：major 不一致拒绝 / minor 降级 / 异名拒绝
 *  - 可靠序列层：30% 丢包 + 乱序 + 重复下精确一次有序投递；超时重传、NACK 补缺、
 *    stale/dup 丢弃、SACK
 *  - BlobAssembler：乱序收块/重复块/重组
 *  - 增量同步：VC 缺口选择
 *  - 背压：CoalescingQueue 合并/淘汰/阻塞；OutboundMeter 慢判定
 *  - 离线存储：内存实现 pending 幂等、时钟持久化
 */
const WBT = require('./public/transport.js');
const WBO = require('./public/offline.js');
const WB = require('./public/kernel.js');

let passed = 0, failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; console.log('  PASS -', msg); }
  else { failed++; console.error('  FAIL -', msg); }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* 确定性随机数（丢包模拟可重复） */
function rng(seed) { let s = seed; return () => { s = (s * 1103515245 + 12345) & 0x7fffffff; return s / 0x7fffffff; }; }

/* ----------------------- 确定性虚拟时钟网络 ----------------------- */
function makeSim(loss, jitter, dup) {
  let now = 1000;
  const timers = [];
  const net = new WBT.VirtualNetwork({
    loss: loss || 0, jitter: jitter || 0, dup: dup || 0, delay: 1,
    now: () => now, setTimeout: (fn, ms) => { timers.push({ t: now + (ms || 0), fn }); }
  });
  return {
    now, net, timers,
    pump(maxSteps) {
      for (let step = 0; step < (maxSteps || 5000); step++) {
        now++;
        const due = timers.filter((t) => t.t <= now).sort((a, b) => a.t - b.t);
        for (const d of due) { d.fn(); const i = timers.indexOf(d); if (i >= 0) timers.splice(i, 1); }
        if (la.sent.size === 0 && received.length >= total && timers.length === 0) break;
      }
    }
  };
}
let la, lb, ta, tb, received, total, sim;

function buildLinks(loss, jitter, dup, seed) {
  Math.random = rng(seed || 7);
  sim = makeSim(loss, jitter, dup);
  const net = sim.net;
  ta = net.makeEndpoint('A'); tb = net.makeEndpoint('B');
  la = new WBT.ReliableLink({ send: (b) => ta.send(b, 'B'), now: net.now });
  lb = new WBT.ReliableLink({ send: (b) => tb.send(b, 'A'), now: net.now });
  received = []; total = 0;
  tb.onmessage = (b) => {
    const out = lb.handle(WBT.frameFrom(b));
    for (const p of out) received.push(p[0]);
    const a = lb.buildAck(); if (a) tb.send(a, 'A');
  };
  ta.onmessage = (b) => { la.handle(WBT.frameFrom(b)); };
  return { la: () => la, lb: () => lb, ta, tb,
    send: (v) => { const w = new WBT.Writer(); w.u8(v); la.send(w.finish()); total++; },
    received: () => received, pump: (n) => {
      for (let step = 0; step < (n || 5000); step++) {
        sim.net.now; // advance
        step;
      }
    },
    run: async function (steps) {
      for (let step = 0; step < (steps || 8000); step++) {
        // advance virtual clock through timer pump
        const s = sim;
        s.net; // noop
        // manually advance now & flush timers
        simAdvance();
        la.tick(sim.now);
        const gaps = lb.gaps();
        if (gaps.length && step % 2 === 0) tb.send(WBT.encode({ type: 'dataNack', seqs: gaps }), 'A');
        if (received.length >= total && la.sent.size === 0) break;
      }
    }
  };
}
function simAdvance() {
  const s = sim;
  // step virtual ms in small increments
  const before = s.now;
  s.now = before + 1;
  const due = s.timers.filter((t) => t.t <= s.now).sort((a, b) => a.t - b.t);
  for (const d of due) { d.fn(); const i = s.timers.indexOf(d); if (i >= 0) s.timers.splice(i, 1); }
}

async function main() {
  console.log('\n[1] 二进制协议：八种 op kind 信封 roundtrip');
  const clock = new WB.Clock('u-a');
  const envs = [
    WB.makeEnvelope(clock, { kind: 'create', objects: [{ oid: 'o1', type: 'rect', fields: { x: 100.5, y: 200.25, z: '0.5' } }] }),
    WB.makeEnvelope(clock, { kind: 'set', oid: 'o1', fields: { tr: { tx: 10, ty: 20, sx: 1, sy: 1, r: 0 } }, prev: { tr: { tx: 0 } } }),
    WB.makeEnvelope(clock, { kind: 'delete', oids: ['o1', 'o2'] }),
    WB.makeEnvelope(clock, { kind: 'restore', oids: ['o1'] }),
    WB.makeEnvelope(clock, { kind: 'group', gid: 'g1', oids: ['o1', 'o2'] }, { txnId: 't1' }),
    WB.makeEnvelope(clock, { kind: 'ungroup', gid: 'g1', oids: ['o1', 'o2'] }),
    WB.makeEnvelope(clock, { kind: 'layer', oid: 'o1', z: '0.25' }),
    WB.makeEnvelope(clock, { kind: 'erase', chunks: [{ oid: 'o1', tx: -3, ty: 7, cells: [[0, 0], [15, 9]] }] })
  ];
  for (const e of envs) {
    const back = WBT.frameFrom(WBT.encode({ type: 'ops', fseq: 5, envelopes: [e] }));
    assert(back.type === 'ops' && back.fseq === 5 && back.envelopes[0].op.kind === e.op.kind,
      `op kind "${e.op.kind}" roundtrip`);
    assert(back.envelopes[0].id === e.id && back.envelopes[0].lamport === e.lamport &&
      back.envelopes[0].clientId === 'u-a', `envelope id/lamport/clientId preserved for ${e.op.kind}`);
  }
  // 逆操作
  const invEnv = WB.makeEnvelope(clock, { kind: 'set', oid: 'o1', fields: { x: 1 } },
    { inv: { originId: 'u-a:1', originLamport: 1, polarity: 0, wide: false } });
  const invBack = WBT.frameFrom(WBT.encode({ type: 'ops', envelopes: [invEnv] })).envelopes[0];
  assert(invBack.op.inv.originId === 'u-a:1' && invBack.op.inv.originLamport === 1,
    'selective-undo inverse metadata roundtrip');

  console.log('\n[2] 点云 Float32 / 时间 Uint32 + 体积优于 JSON');
  const pts = [];
  for (let i = 0; i < 500; i++) pts.push({ x: i * 1.25 + 0.1, y: Math.sin(i / 10) * 40, p: 0.2 + (i % 10) * 0.07, t: i * 16, w: 3 + (i % 5) * 0.4 });
  const strokeEnv = WB.makeEnvelope(clock, {
    kind: 'create', objects: [{ oid: 's1', type: 'stroke', fields: { stroke: { width: 4, color: '#1f2937', points: pts } } }]
  });
  const bin = WBT.encode({ type: 'ops', envelopes: [strokeEnv] });
  const json = Buffer.from(JSON.stringify({ type: 'ops', envelopes: [strokeEnv] }));
  console.log(`    bin=${bin.length}B json=${json.length}B ratio=${(bin.length / json.length).toFixed(2)}`);
  assert(bin.length < json.length * 0.45, `binary is <45% of JSON size (${bin.length} vs ${json.length})`);
  const backPts = WBT.frameFrom(bin).envelopes[0].op.objects[0].fields.stroke.points;
  assert(backPts.length === 500, 'all 500 points decoded');
  let maxErr = 0, tExact = true;
  for (let i = 0; i < 500; i++) {
    maxErr = Math.max(maxErr, Math.abs(backPts[i].x - pts[i].x), Math.abs(backPts[i].y - pts[i].y));
    if (backPts[i].t !== pts[i].t) tExact = false;
  }
  assert(maxErr < 1e-3, `coordinates roundtrip within Float32 epsilon (maxErr=${maxErr.toExponential(1)})`);
  assert(tExact, 'timestamps preserved exactly as Uint32');

  console.log('\n[3] 快照帧 / delta 帧 roundtrip（含 VC + blob refs）');
  const snapshot = {
    version: 2, known: { 'u-a': 8, 'u-b': 3 }, groups: [{ gid: 'g1', id: 'u-a:5', lamport: 5, clientId: 'u-a', members: ['o1'] }],
    objects: [{ oid: 'o1', regs: { type: { id: 'u-a:1', value: 'rect', lamport: 1, clientId: 'u-a', inv: null, squashKey: null } }, erases: {} }]
  };
  const snapBack = WBT.frameFrom(WBT.encode({
    type: 'snapshot', fseq: 9, watermark: 100, lastSeq: 142, snapshot,
    envelopes: [envs[1]], blobs: [{ blobId: 'ab12', size: 9999, chunksTotal: 2, hash: 'ab12', holders: [7], id: 'u-a:9' }]
  }));
  assert(snapBack.type === 'snapshot' && snapBack.fseq === 9 && snapBack.lastSeq === 142,
    'snapshot header roundtrip');
  assert(snapBack.snapshot.objects[0].regs.type.value === 'rect', 'snapshot registers roundtrip');
  assert(snapBack.blobs[0].blobId === 'ab12' && snapBack.blobs[0].holders[0] === 7, 'blob refs roundtrip');

  console.log('\n[4] 魔数 / 版本拒绝');
  const bad = new Uint8Array([1, 2, 3, 4, 5]);
  assert(WBT.frameFrom(bad).badMagic === true, 'bad magic detected');
  const badMaj = WBT.encode({ type: 'ping', t: 1 }); badMaj[2] = 9;
  assert(WBT.frameFrom(badMaj).badVersion === true, 'incompatible major version detected');

  console.log('\n[5] 协议协商：major 拒绝 / minor 降级 / 异名拒绝');
  assert(WBT.negotiate('wb3', 3, 0).ok === true, 'same major negotiated');
  assert(WBT.negotiate('wb3', 3, 5, 0).minor === 0, 'newer client minor downgrades to older server');
  assert(WBT.negotiate('wb3', 4, 0).ok === false, 'major mismatch rejected');
  assert(WBT.negotiate('other', 3, 0).ok === false, 'protocol family mismatch rejected');

  console.log('\n[6] 可靠层：30% 丢包 + 乱序，全部有序精确一次');
  {
    const ctx = buildLinks(0.3, 8, 0.05, 11);
    for (let i = 1; i <= 120; i++) ctx.send(i);
    await ctx.run(8000);
    const want = Array.from({ length: 120 }, (_, i) => i + 1);
    assert(received.length === 120, `all 120 delivered (got ${received.length})`);
    assert(JSON.stringify(received) === JSON.stringify(want), 'strict in-order delivery');
    assert(lb.stats.dupDropped > 0, `duplicate packets dropped (${lb.stats.dupDropped})`);
    assert(lb.stats.reordered > 0, `out-of-order packets reordered (${lb.stats.reordered})`);
    assert(la.stats.rexmited > 0, `lost packets retransmitted (${la.stats.rexmited})`);
  }

  console.log('\n[7] 可靠层：NACK 显式缺口重传（60% 丢包）');
  {
    const ctx = buildLinks(0.6, 4, 0, 23);
    for (let i = 1; i <= 40; i++) ctx.send(i);
    await ctx.run(20000);
    assert(received.length === 40 && JSON.stringify(received) === JSON.stringify(Array.from({ length: 40 }, (_, i) => i + 1)),
      `NACK gap recovery under 60% loss (got ${received.length}/40)`);
  }

  console.log('\n[8] 旧消息（stale seq）与重复包丢弃，不能覆盖新状态');
  {
    const ctx = buildLinks(0, 0, 0, 1);
    ctx.send(1); ctx.send(2);
    await ctx.run(50);
    const w = new WBT.Writer(); w.u8(99);
    const stale = WBT.encode({ type: 'data', seq: 1, payload: w.finish() });
    tb.onmessage(stale); // 旧 seq 重放
    assert(received.length === 2 && received[0] === 1 && received[1] === 2,
      'stale seq=1 replayed after seq=2 is dropped');
    const fresh = new WBT.Writer(); fresh.u8(3);
    tb.onmessage(WBT.encode({ type: 'data', seq: 3, payload: fresh.finish() }));
    assert(received.length === 3 && received[2] === 3, 'later seq still accepted after stale replay');
  }

  console.log('\n[9] 乱序窗口：先到 seq 3、再补 1、2，按 1,2,3 投递');
  {
    const ctx = buildLinks(0, 0, 0, 1);
    const mk = (n) => { const w = new WBT.Writer(); w.u8(n); return w.finish(); };
    tb.onmessage(WBT.encode({ type: 'data', seq: 3, payload: mk(30) }));
    tb.onmessage(WBT.encode({ type: 'data', seq: 2, payload: mk(20) }));
    assert(received.length === 0, 'gap holds delivery (nothing before seq 1)');
    tb.onmessage(WBT.encode({ type: 'data', seq: 1, payload: mk(10) }));
    assert(JSON.stringify(received) === JSON.stringify([10, 20, 30]), 'gap filled → ordered flush 1,2,3');
    // SACK 应包含乱序到达
    const ack = WBT.frameFrom(lb.buildAck());
    assert(ack.type === 'dataAck' && ack.next === 4, 'ACK watermark advances past gap fill');
  }

  console.log('\n[10] BlobAssembler：乱序收块 + 重复块丢弃');
  {
    const data = new Uint8Array(50000);
    for (let i = 0; i < data.length; i++) data[i] = i & 0xff;
    const { total, frames } = WBT.chunkBlob('h1', data, 16 * 1024);
    assert(total === 4, `blob split into 4 chunks (got ${total})`);
    const asm = new WBT.BlobAssembler();
    const decoded = frames.map((f) => WBT.frameFrom(f));
    let done = asm.add(decoded[3]); assert(!done, 'last chunk alone not complete');
    done = asm.add(decoded[3]); assert(!done, 'duplicate chunk ignored');
    done = asm.add(decoded[0]); assert(!done, '0,3 incomplete');
    done = asm.add(decoded[2]); assert(!done, '0,2,3 waiting for 1');
    done = asm.add(decoded[1]);
    assert(!!done && done.blobId === 'h1', 'final chunk completes assembly');
    let same = done.data.length === data.length;
    for (let i = 0; same && i < data.length; i++) if (done.data[i] !== data[i]) same = false;
    assert(same, 'assembled blob bytes identical to original');
  }

  console.log('\n[11] 增量同步：按版本向量只返回缺失信封');
  {
    const mkEnv = (cid, n) => ({ id: `${cid}:${n}`, clientId: cid, lamport: n, clock: { [cid]: n }, op: { kind: 'set' } });
    const log = [mkEnv('A', 1), mkEnv('B', 1), mkEnv('A', 2), mkEnv('B', 2), mkEnv('C', 1)];
    const missing = WBT.missingForPeer(log, { A: 2, B: 1 });
    assert(missing.map((e) => e.id).join(',') === 'B:2,C:1',
      `only envelopes past peer VC returned (${missing.map((e) => e.id).join(',')})`);
    assert(WBT.missingForPeer(log, { A: 9, B: 9, C: 9 }).length === 0, 'up-to-date peer gets no delta');
  }

  console.log('\n[12] 背压：CoalescingQueue 合并高频帧 / 高水位阻塞 / 淘汰');
  {
    const q = new WBT.CoalescingQueue({ highBytes: 100, lowBytes: 50 });
    const r1 = q.push({ squashKey: 'move:g:o1', size: 30, run: () => 1 });
    const r2 = q.push({ squashKey: 'move:g:o1', size: 30, run: () => 2 });
    assert(r1 === 'accepted' && r2 === 'coalesced' && q.length === 1, 'same squashKey coalesced to newest frame');
    assert(q.bytes === 30, 'coalesce replaces size accounting, does not double count');
    // 单独验证：合并后被冲刷的是“最新一帧”的载荷
    const exec0 = [];
    q.drain((j) => exec0.push(j.run()));
    assert(exec0.length === 1 && exec0[0] === 2, 'coalesced frame executes the newest payload only');
    // 重建队列继续测高水位
    q.push({ size: 30, run: () => 2 });
    // 再压一个不可合并的 70 字节帧：30+70=100 临界内可入队；80 字节触发背压，
    // 队列先淘汰可合并中间帧（move 手势中间帧最没价值），仍放不下则拒绝
    q.push({ size: 70 }); // 30 + 70 = 100
    assert(q.length === 2 && q.bytes === 100, 'queue accepts up to high water mark');
    assert(q.push({ size: 80 }) === 'blocked', 'over high-water blocks new work (producer backpressure)');
    // 无 squashKey 的帧不能被淘汰 → 新帧硬拒绝，原两帧保留
    const exec = [];
    q.drain((j) => exec.push(typeof j.run === 'function' ? j.run() : null));
    assert(exec.length === 2 && exec[0] === 2 && exec[1] === null,
      'rejected frame never enters queue; queued frames preserved');
    // 淘汰：压力下先丢弃可合并帧
    const q2 = new WBT.CoalescingQueue({ highBytes: 100, lowBytes: 50 });
    q2.push({ squashKey: 'a', size: 60 });
    const outcome = q2.push({ squashKey: 'b', size: 50 });
    assert(outcome === 'accepted' && !q2.jobs.some((j) => j.squashKey === 'a'),
      'squashable frames shed first under pressure');
  }

  console.log('\n[13] OutboundMeter：fseq 记账 / ACK 释放 / 慢客户端判定');
  {
    const m = new WBT.OutboundMeter(1000);
    const f1 = m.reserve(400), f2 = m.reserve(400);
    assert(f1 === 1 && f2 === 2 && m.slow === false, 'fseq monotonic; under threshold not slow');
    const f3 = m.reserve(400);
    assert(m.slow === true, 'outstanding bytes over high-water → slow client detected');
    m.ack(f2);
    assert(m.bytes === 400 && m.slow === false, 'ACK through fseq releases bytes (out-of-band throttle clears)');
    m.ack(1); // 旧 ACK 不倒退
    assert(m.ackedFseq === f2, 'stale ACK cannot move watermark backward');
    void f3;
  }

  console.log('\n[14] 离线存储：pending 持久化 / 幂等删除 / 时钟保存');
  {
    const store = await WBO.openOffline({ memory: true });
    const e1 = { id: 'u-a:1', lamport: 1 };
    const e2 = { id: 'u-a:2', lamport: 2 };
    await store.putPendingAll([e1, e2]);
    await store.putPending(e1); // 幂等
    assert(await store.pendingCount() === 2, 'pending ops persisted, duplicates merged by id');
    const all = await store.allPending();
    assert(all[0].id === 'u-a:1' && all[1].id === 'u-a:2', 'pending ops replayed in lamport order');
    await store.saveClock({ local: 2, lamport: 5, vc: { 'u-a': 2 } });
    const c = await store.loadClock();
    assert(c.local === 2 && c.lamport === 5, 'clock state persisted across sessions');
    await store.removePending(['u-a:1']);
    assert(await store.pendingCount() === 1, 'acked ops removed from offline log');
  }

  console.log(`\n========================================`);
  console.log(`TRANSPORT RESULT: ${passed} passed, ${failed} failed`);
  process.exit(failed ? 1 : 0);
}

main().catch((e) => { console.error('TRANSPORT CRASHED:', e); process.exit(1); });
