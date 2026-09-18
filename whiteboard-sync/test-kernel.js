'use strict';
/*
 * 内核一致性测试（纯 Node，无服务端/浏览器）：
 *  1. 三客户端并发 → 任意乱序投递，最终对象状态一致，无重复
 *  2. 选择性撤销：A 撤销自己的旧操作，B 已改过同一字段 → A 的撤销空转，B 的结果保留
 *  3. 事务原子性：多对象移动作为一个 txn，因果缓冲要么整组可见要么不可见
 *  4. 压感/速度宽度两端公式一致
 *  5. RDP / Catmull-Rom / B 样条 / 分数序 / 橡皮分块 / 压缩
 */
const WB = require('./public/kernel.js');

let passed = 0, failed = 0;
function assert(cond, msg) {
  if (cond) { passed++; console.log('  PASS -', msg); }
  else { failed++; console.error('  FAIL -', msg); }
}
function eq(a, b, msg) { assert(JSON.stringify(a) === JSON.stringify(b), `${msg} (got ${JSON.stringify(a)}, want ${JSON.stringify(b)})`); }

/* ---------------- 辅助：模拟三个节点 ---------------- */
function node(id) {
  const clock = new WB.Clock(id);
  const buf = new WB.CausalBuffer();
  const doc = new WB.Doc();
  const history = new Map();
  // 每个节点发的信封（模拟网络）：收齐后按各自因果缓冲投递
  const deliver = (env, applyToSelf) => {
    if (applyToSelf) {
      clock.mergeVC(env.clock);
      doc.apply(env);
      history.set(env.id, env);
    }
    // 交给其它节点的网络接口在测试里手动调用 node.receive
  };
  return {
    id, clock, buf, doc, history,
    issue(op, opts) {
      const env = WB.makeEnvelope(clock, op, opts);
      // 本地发出的信封同样经过自己的因果缓冲（真实节点行为），
      // 这样 buf.known 中自己的计数与后续 receive 的他人信封依赖才连续
      buf.push(env);
      history.set(env.id, env);
      doc.apply(env); // 本地立即生效（乐观预提交）
      return env;
    },
    /** 从网络收到一个信封：因果缓冲 → 物化 */
    receive(env) {
      const ready = buf.push(env);
      for (const e of ready) {
        clock.observeLamport(e.lamport);
        clock.mergeVC(e.clock);
        doc.apply(e);
        history.set(e.id, e);
      }
      return ready;
    },
    undoMgr() { return new WB.UndoManager(clock); }
  };
}

console.log('\n[1] 三客户端并发：同对象并发写入，乱序到达，最终一致');
{
  const A = node('A'), B = node('B'), C = node('C');
  // A 创建对象（三端先同步 create）
  const create = A.issue({ kind: 'create', objects: [{ oid: 'o1', type: 'rect',
    fields: { x: 0, y: 0, w: 100, h: 50, color: '#000' } }] });
  B.receive(create); C.receive(create);

  // 三端无因果依赖地并发移动同一对象（基于相同 VC 起点）
  // 为构造真并发：B、C 不经过彼此，直接在各自时钟上 tick（Lamport 都为 2）
  const moveB = B.issue({ kind: 'set', oid: 'o1', fields: { x: 10 }, prev: { x: 0 } });
  const moveC = C.issue({ kind: 'set', oid: 'o1', fields: { x: 20 }, prev: { x: 0 } });
  const moveA = A.issue({ kind: 'set', oid: 'o1', fields: { x: 30 }, prev: { x: 0 } });

  // 注意：三方真并发时 VC 互不含对方计数，CausalBuffer 不会阻塞
  // （规则只校验 sender 序号连续 + 不引用未知的更大他方计数）
  // 用 6 种不同的接收顺序重放，结果必须一致（LWW 按 (lamport, clientId)）
  const orders = [
    [moveA, moveB, moveC], [moveC, moveB, moveA], [moveB, moveA, moveC],
    [moveB, moveC, moveA], [moveA, moveC, moveB], [moveC, moveA, moveB]
  ];
  const finals = new Set();
  for (const order of orders) {
    const X = node('X');
    X.receive(create);
    for (const e of order) X.receive(e);
    finals.add(X.doc.get('o1').x);
  }
  assert(finals.size === 1, `all 6 delivery orders converge (values=${[...finals].join(',')})`);

  // 三端互相同步并发操作后，也必须收敛到同一值
  for (const e of [moveB, moveC]) A.receive(e);
  for (const e of [moveA, moveC]) B.receive(e);
  for (const e of [moveA, moveB]) C.receive(e);
  const xa = A.doc.get('o1').x, xb = B.doc.get('o1').x, xc = C.doc.get('o1').x;
  assert(xa === xb && xb === xc, `A/B/C converge after sync (x=${xa})`);
}

console.log('\n[2] 选择性撤销：A 的撤销不破坏 B 的后续修改');
{
  const A = node('A'), B = node('B');
  const create = A.issue({ kind: 'create', objects: [{ oid: 'o2', type: 'rect',
    fields: { x: 0, y: 0, w: 10, h: 10 } }] });
  B.receive(create);

  // A 移动到 50（带 prev 快照）
  const moveA = A.issue({ kind: 'set', oid: 'o2', fields: { x: 50 }, prev: { x: 0 } });
  B.receive(moveA);

  // B 随后（causal-after）再移动到 90
  const moveB = B.issue({ kind: 'set', oid: 'o2', fields: { x: 90 }, prev: { x: 50 } });
  A.receive(moveB);

  // A 撤销自己的 moveA：逆操作试图恢复 x=0
  const um = A.undoMgr();
  um.record([moveA], 'move');
  const undoGroup = um.undo(A.history);
  const undoEnv = undoGroup[0];
  A.receive(undoEnv); // 本地先生效
  B.receive(undoEnv);
  assert(A.doc.get('o2').x === 90, `A undo is void locally: B's x=90 preserved (got ${A.doc.get('o2').x})`);
  assert(B.doc.get('o2').x === 90, `B keeps x=90 after receiving A's undo (got ${B.doc.get('o2').x})`);

  // 对照组：B 没有改过的字段，A 撤销应当生效
  const moveAY = A.issue({ kind: 'set', oid: 'o2', fields: { y: 30 }, prev: { y: 0 } });
  B.receive(moveAY);
  um.record([moveAY], 'moveY');
  const undoY = um.undo(A.history)[0];
  A.receive(undoY);
  B.receive(undoY);
  assert(A.doc.get('o2').y === 0, 'A can undo untouched field y back to 0');
  assert(B.doc.get('o2').y === 0, 'B applies the valid undo for field y');
  assert(A.doc.get('o2').x === 90, 'x still 90: selective undo is per-field');
}

console.log('\n[2b] 撤销自己 create 的笔迹：若 B 已在该笔迹上擦除/修改，则撤销空转');
{
  const A = node('A'), B = node('B');
  const stroke = A.issue({ kind: 'create', objects: [{ oid: 's1', type: 'stroke',
    fields: { stroke: { width: 4, points: [{ x: 0, y: 0 }, { x: 10, y: 10 }] } } }] });
  B.receive(stroke);
  // B 对该笔迹做了像素擦除（erase 寄存器），属于对象上的他人写入
  const er = B.issue({ kind: 'erase', chunks: [{ oid: 's1', tx: 0, ty: 0, cells: [[0, 0]] }] });
  A.receive(er);
  const um = A.undoMgr();
  um.record([stroke], 'stroke');
  const undoDel = um.undo(A.history)[0];
  A.receive(undoDel);
  B.receive(undoDel);
  const live = A.doc.liveObjects().map((o) => o.oid);
  assert(live.includes('s1'), `stroke NOT deleted: A's create-undo void because B erased on it (live=${live})`);
}

console.log('\n[3] 事务原子性：多对象移动整组因果投递');
{
  const A = node('A'), B = node('B');
  const c1 = A.issue({ kind: 'create', objects: [
    { oid: 'g1', type: 'rect', fields: { x: 0, y: 0, w: 10, h: 10 } },
    { oid: 'g2', type: 'rect', fields: { x: 30, y: 0, w: 10, h: 10 } }
  ] });
  B.receive(c1);

  // 一次移动两个对象：两个 set 信封绑定同一 txnId
  const e1 = WB.makeEnvelope(A.clock, { kind: 'set', oid: 'g1', fields: { x: 100 }, prev: { x: 0 } });
  const e2 = WB.makeEnvelope(A.clock, { kind: 'set', oid: 'g2', fields: { x: 130 }, prev: { x: 30 } });
  WB.atomic([e1, e2]);
  A.doc.apply(e1); A.doc.apply(e2);
  A.history.set(e1.id, e1); A.history.set(e2.id, e2);

  // B 先收到 e2（后半条事务）：由于 e2 引用了 A 计数 2 而 B 只知道 1，因果缓冲必须挂起
  const ready2 = B.receive(e2);
  assert(ready2.length === 0, 'second half of txn held back by causal buffer (no half-move visible)');
  assert(B.doc.get('g2').x === 30, 'g2 still at old position while txn incomplete');
  // e1 到达：整组一次性投递
  const ready1 = B.receive(e1);
  assert(ready1.length === 2, `whole txn delivered atomically (got ${ready1.length})`);
  assert(B.doc.get('g1').x === 100 && B.doc.get('g2').x === 130, 'both objects moved: all-or-nothing');
}

console.log('\n[4] 压感/速度宽度：两端共享公式，结果逐点一致');
{
  const pts = [
    { x: 0, y: 0, p: 0.2, t: 0 },
    { x: 5, y: 1, p: 0.8, t: 10 },
    { x: 30, y: 2, p: 0.9, t: 20 }, // 高速 → 变细
    { x: 33, y: 2, p: 0.9, t: 30 }
  ];
  const w1 = WB.computeWidths(pts, 8);
  const w2 = WB.computeWidths(pts, 8);
  eq(w1, w2, 'deterministic widths on both ends');
  assert(w1[1] > w1[0], 'higher pressure makes stroke wider');
  assert(w1[2] < w1[1], 'higher speed makes stroke thinner');
  assert(w1.every((w) => w > 0 && w < 8 * 1.5), 'widths in sane range');
}

console.log('\n[5] RDP 简化保持端点与关键拐点');
{
  const pts = [];
  for (let i = 0; i <= 50; i++) pts.push({ x: i * 2, y: i % 2 === 0 ? 0 : 1 }); // 近直线
  pts[25].y = 80; // 一个明显拐点
  const simp = WB.rdp(pts, 3);
  assert(simp.length < pts.length, `rdp reduced points (${pts.length} -> ${simp.length})`);
  assert(simp[0] === pts[0] && simp[simp.length - 1] === pts[pts.length - 1], 'endpoints preserved');
  assert(simp.some((p) => p === pts[25]), 'sharp corner preserved');
}

console.log('\n[6] Catmull-Rom / B 样条输出贝塞尔段');
{
  const pts = [{ x: 0, y: 0 }, { x: 10, y: 10 }, { x: 20, y: 0 }, { x: 30, y: 10 }];
  const cr = WB.catmullRomToBezier(pts, 0.5);
  const bs = WB.bsplineToBezier(pts);
  assert(cr.length === pts.length && cr[1].c1x != null, 'catmull-rom emits n segments with control points');
  assert(bs.length === pts.length && bs[1].c2x != null, 'b-spline emits n bezier segments');
  const sm = WB.smoothPath(pts, 'catmull');
  eq(sm, cr, 'smoothPath catmull dispatch');
}

console.log('\n[7] 分数序：并发插入不冲突、排序正确');
{
  const z0 = WB.zBetween(null, null);
  const z1 = WB.zBetween(z0, null); // 追加
  const z2 = WB.zBetween(z0, z1);  // 并发插到中间
  assert(WB.fracCmp(z0, z2) < 0 && WB.fracCmp(z2, z1) < 0, `ordering 0 < mid < 1 (${z0} ${z2} ${z1})`);
  // 重复取中点 200 次，仍严格递增且不越界
  let a = z0;
  for (let i = 0; i < 200; i++) { const n = WB.zBetween(a, z2); assert(WB.fracCmp(a, n) < 0 && WB.fracCmp(n, z2) < 0, 'midpoint strictly between'); a = n; }
  // 同位置并发：不同节点各取一次中点得到相同 z → 用 (lamport, clientId) 平局，不报错
  const za = WB.zForInsert([z0, z1], 1);
  const zb = WB.zForInsert([z0, z1], 1);
  eq(za, zb, 'concurrent same-index inserts get same fractional key (tie-broken deterministically)');
}

console.log('\n[8] 橡皮分块：快速划动覆盖、单元分块');
{
  const path = [{ x: 0, y: 0 }, { x: 400, y: 300 }];
  const { cellSize, chunks } = WB.rasterizeErase(path, 16);
  assert(cellSize === 16, 'cell size = eraser size');
  assert(chunks.length >= 2, `erase path spans multiple tiles (${chunks.length})`);
  const total = chunks.reduce((n, c) => n + c.cells.length, 0);
  assert(total >= 20, `fast stroke stamps interpolated cells along path (${total})`);
  for (const c of chunks) assert(c.cells.every(([x, y]) => x >= 0 && x < 16 && y >= 0 && y < 16), 'cell coords inside tile');
}

console.log('\n[8b] 橡皮擦 CRDT：分块擦除两端一致，撤销可恢复');
{
  const A = node('A'), B = node('B');
  const cr = A.issue({ kind: 'create', objects: [{ oid: 'e1', type: 'stroke',
    fields: { stroke: { width: 4, points: [{ x: 0, y: 0 }, { x: 100, y: 100 }] } } }] });
  B.receive(cr);
  const { chunks } = WB.rasterizeErase([{ x: 0, y: 0 }, { x: 60, y: 60 }], 16);
  const erase = A.issue({ kind: 'erase', chunks: chunks.map((c) => Object.assign({ oid: 'e1' }, c)) });
  B.receive(erase);
  const ca = A.doc.erasedCells('e1'), cb = B.doc.erasedCells('e1');
  eq(Array.from(ca).sort(), Array.from(cb).sort(), 'erased cell set identical on both ends');

  const um = A.undoMgr(); um.record([erase], 'erase');
  const un = um.undo(A.history)[0];
  A.receive(un);  // 逆操作本地也立即提交（真实客户端行为）
  B.receive(un);
  assert(A.doc.erasedCells('e1').size === 0, 'undo eraser restores cells');
  assert(B.doc.erasedCells('e1').size === 0, 'remote undo eraser restores cells too');
}

console.log('\n[8c] 像素擦选择性撤销粒度：A/B 同块不同单元，A 撤销只恢复自己的格子');
{
  const A = node('A'), B = node('B');
  const cr = A.issue({ kind: 'create', objects: [{ oid: 'p1', type: 'stroke',
    fields: { stroke: { width: 4, points: [{ x: 0, y: 0 }, { x: 100, y: 100 }] } } }] });
  B.receive(cr);

  // A 先擦 tile(0,0) 的 (0,0)/(1,1)
  const eraseA = A.issue({ kind: 'erase', chunks: [{ oid: 'p1', tx: 0, ty: 0,
    cells: [[0, 0], [1, 1]] }] });
  B.receive(eraseA);
  // B 紧接着擦同一分块内的其它单元 (2,2)/(3,3)
  const eraseB = B.issue({ kind: 'erase', chunks: [{ oid: 'p1', tx: 0, ty: 0,
    cells: [[2, 2], [3, 3]] }] });
  A.receive(eraseB);
  // A 撤销自己的擦除：只应恢复 A 擦过的两格，B 的两格保留
  const um = A.undoMgr();
  um.record([eraseA], 'pixel erase');
  const undo = um.undo(A.history)[0];
  A.receive(undo);
  B.receive(undo);
  const want = ['0:0,2,2', '0:0,3,3'];
  eq(Array.from(A.doc.erasedCells('p1')).sort(), want,
    'A undo restores only A cells, B cells kept (A view)');
  eq(Array.from(B.doc.erasedCells('p1')).sort(), want,
    'A undo restores only A cells, B cells kept (B view)');

  // 撤销不能空转：A 的两格确实恢复（可被重新擦）
  assert(!A.doc.erasedCells('p1').has('0:0,0,0'), 'A cell (0,0) restored (undo did not no-op)');
  assert(!A.doc.erasedCells('p1').has('0:0,1,1'), 'A cell (1,1) restored (undo did not no-op)');
}

console.log('\n[8d] 同块并发 + 单元重叠：撤销只在“同一格被他人重擦”的单元上空转');
{
  // 真并发：A、B 的擦除 lamport 相同（互不感知），仍落同一分块
  const X = node('X'), A = node('A'), B = node('B');
  const cr = A.issue({ kind: 'create', objects: [{ oid: 'p2', type: 'stroke',
    fields: { stroke: { width: 4, points: [{ x: 0, y: 0 }, { x: 10, y: 10 }] } } }] });
  X.receive(cr); B.receive(cr);

  const eraseA = A.issue({ kind: 'erase', chunks: [{ oid: 'p2', tx: 0, ty: 0,
    cells: [[0, 0], [1, 1]] }] }); // lamport 2
  const eraseB = B.issue({ kind: 'erase', chunks: [{ oid: 'p2', tx: 0, ty: 0,
    cells: [[1, 1], [5, 5]] }] }); // lamport 2，与 A 真并发；(1,1) 重叠
  // 乱序到达 X：先 B 后 A
  X.receive(eraseB); X.receive(eraseA);

  const um = A.undoMgr();
  um.record([eraseA], 'pixel erase');
  const undo = um.undo(A.history)[0];
  X.receive(undo);
  const erased = Array.from(X.doc.erasedCells('p2')).sort();
  // (1,1) 被 B 的并发意图覆盖 → A 的恢复在该格空转，保持擦除；
  // (0,0) 没被别人碰 → 正常恢复；(5,5) 是 B 的，不受影响。
  eq(erased, ['0:0,1,1', '0:0,5,5'],
    'undo void only on the overlapping cell, distinct cells restored/kept');
}

console.log('\n[8e] 像素擦撤销经快照压缩后，单元级保护仍然成立（晚加入者一致）');
{
  const A = node('A'), B = node('B');
  const cr = A.issue({ kind: 'create', objects: [{ oid: 'p3', type: 'stroke',
    fields: { stroke: { width: 4, points: [{ x: 0, y: 0 }, { x: 10, y: 10 }] } } }] });
  B.receive(cr);
  const eraseA = A.issue({ kind: 'erase', chunks: [{ oid: 'p3', tx: 0, ty: 0,
    cells: [[0, 0], [1, 1]] }] });
  B.receive(eraseA);
  const eraseB = B.issue({ kind: 'erase', chunks: [{ oid: 'p3', tx: 0, ty: 0,
    cells: [[2, 2]] }] });
  A.receive(eraseB);
  const um = A.undoMgr();
  um.record([eraseA], 'pixel erase');
  const undo = um.undo(A.history)[0];
  A.receive(undo);
  B.receive(undo);
  const liveWant = Array.from(A.doc.erasedCells('p3')).sort();
  eq(liveWant, ['0:0,2,2'], 'live converged state before snapshot');

  // 服务端式压缩：导出快照并在全新副本上载入（模拟晚加入/水位裁剪后重放）
  const snap = A.doc.snapshot(A.clock.snapshotVC());
  const fresh = new WB.Doc().loadSnapshot(snap);
  eq(Array.from(fresh.erasedCells('p3')).sort(), liveWant,
    'late joiner from snapshot gets identical per-cell erase state');

  // 快照里保留了 B 的 protector：之后重放 A 的撤销（若来自水位前补发）也不改变结果
  const re = new WB.Doc().loadSnapshot(snap);
  re.apply(undo);
  eq(Array.from(re.erasedCells('p3')).sort(), ['0:0,2,2'],
    'replayed undo after snapshot still keeps B cell (protector retained)');
}

console.log('\n[8f] 像素擦撤销后重做：恢复自己的擦除，仍不碰 B 的格子');
{
  const A = node('A'), B = node('B');
  const cr = A.issue({ kind: 'create', objects: [{ oid: 'p4', type: 'stroke',
    fields: { stroke: { width: 4, points: [{ x: 0, y: 0 }, { x: 10, y: 10 }] } } }] });
  B.receive(cr);
  const eraseA = A.issue({ kind: 'erase', chunks: [{ oid: 'p4', tx: 0, ty: 0,
    cells: [[0, 0], [1, 1]] }] });
  B.receive(eraseA);
  const eraseB = B.issue({ kind: 'erase', chunks: [{ oid: 'p4', tx: 0, ty: 0,
    cells: [[2, 2]] }] });
  A.receive(eraseB);
  const um = A.undoMgr();
  um.record([eraseA], 'pixel erase');
  const undo = um.undo(A.history).flat();
  A.receive(undo[0]); B.receive(undo[0]);
  eq(Array.from(A.doc.erasedCells('p4')).sort(), ['0:0,2,2'], 'undo leaves only B cell');

  // redo 应重新擦 A 的两格（erase，而不是错误地再次反转成 unerase），B 的格子照旧保留
  const redo = um.redo(A.history).flat();
  A.receive(redo[0]); B.receive(redo[0]);
  eq(Array.from(A.doc.erasedCells('p4')).sort(), ['0:0,0,0', '0:0,1,1', '0:0,2,2'],
    'redo re-erases A cells, keeps B cell (A view)');
  eq(Array.from(B.doc.erasedCells('p4')).sort(), ['0:0,0,0', '0:0,1,1', '0:0,2,2'],
    'redo converges on B view too');
}

console.log('\n[8g] A 撤销后 B 继续擦/取消擦同一格：单格 LWW 与撤销保护按格独立');
{
  const A = node('A'), B = node('B');
  const cr = A.issue({ kind: 'create', objects: [{ oid: 'p5', type: 'stroke',
    fields: { stroke: { width: 4, points: [{ x: 0, y: 0 }, { x: 10, y: 10 }] } } }] });
  B.receive(cr);
  const eraseA = A.issue({ kind: 'erase', chunks: [{ oid: 'p5', tx: 0, ty: 0,
    cells: [[0, 0], [1, 1]] }] });
  B.receive(eraseA);
  // B 先只擦 (2,2)，随后又把它取消擦（unerase）
  const eraseB = B.issue({ kind: 'erase', chunks: [{ oid: 'p5', tx: 0, ty: 0,
    cells: [[2, 2]] }] });
  A.receive(eraseB);
  const uneraseB = B.issue({ kind: 'erase', unerase: true, chunks: [{ oid: 'p5', tx: 0, ty: 0,
    cells: [[2, 2]] }] });
  A.receive(uneraseB);
  const um = A.undoMgr();
  um.record([eraseA], 'pixel erase');
  const undo = um.undo(A.history).flat();
  A.receive(undo[0]); B.receive(undo[0]);
  // A 的两格恢复；(2,2) 因 B 自己取消擦而本来就是未擦状态 —— 三格全空
  eq(Array.from(A.doc.erasedCells('p5')).sort(), [],
    'A cells restored; B un-erased its own cell too => empty (A view)');
  eq(Array.from(B.doc.erasedCells('p5')).sort(), [],
    'per-cell LWW independent across users (B view)');
}

console.log('\n[9] 操作压缩：连续移动合并为最终状态');
{
  const C = new WB.Clock('Z');
  const mk = (x) => WB.makeEnvelope(C, { kind: 'set', oid: 'm1', fields: { x }, prev: { x: x - 1 } }, { squashKey: 'move:m1' });
  const createEnv = WB.makeEnvelope(C, { kind: 'create', objects: [{ oid: 'm1', type: 'rect', fields: { x: 0 } }] });
  const moves = [];
  for (let i = 1; i <= 100; i++) moves.push(mk(i));
  const log = [createEnv].concat(moves);
  const compact = WB.squash(log);
  assert(compact.length === 2, `100 continuous moves compact to 1 final op (${log.length} -> ${compact.length})`);
  const finalSet = compact.find((e) => e.op.kind === 'set');
  assert(finalSet.op.fields.x === 100, 'compacted op carries final x=100');
  // 不同对象的同 key 前缀不会被错误合并
  const other = WB.makeEnvelope(C, { kind: 'set', oid: 'm2', fields: { x: 5 } }, { squashKey: 'move:m2' });
  assert(WB.squash(log.concat([other])).filter((e) => e.op.kind === 'set').length === 2, 'different squashKeys stay separate');
}

console.log('\n[10] 因果缓冲：缺依赖挂起，补齐后按序冲刷');
{
  const A = node('R'), B = node('S');
  const e1 = A.issue({ kind: 'create', objects: [{ oid: 'z1', type: 'rect', fields: {} }] });
  const e2 = A.issue({ kind: 'set', oid: 'z1', fields: { x: 1 }, prev: {} });
  const e3 = A.issue({ kind: 'set', oid: 'z1', fields: { x: 2 }, prev: { x: 1 } });
  // B 只收到 e3：必须挂起
  assert(B.receive(e3).length === 0, 'gap detected: e3 buffered');
  assert(B.receive(e1).length === 1, 'e1 delivered, e2/e3 still wait');
  const flushed = B.receive(e2);
  assert(flushed.length === 2, `e2 + buffered e3 flushed in order (got ${flushed.length})`);
  assert(B.doc.get('z1').x === 2, 'state consistent after causal catch-up');
}

console.log('\n[11] 图层 z 与 liveObjects 排序、墓碑');
{
  const A = node('A');
  A.issue({ kind: 'create', objects: [
    { oid: 'l1', type: 'rect', fields: { z: '0.2' } },
    { oid: 'l2', type: 'rect', fields: { z: '0.8' } },
    { oid: 'l3', type: 'rect', fields: { z: '0.5' } }
  ] });
  const order = A.doc.liveObjects().map((o) => o.oid);
  eq(order, ['l1', 'l3', 'l2'], 'objects sorted by fractional z');
  A.issue({ kind: 'delete', oids: ['l3'] });
  assert(!A.doc.liveObjects().some((o) => o.oid === 'l3'), 'deleted object hidden from live set');
  A.issue({ kind: 'restore', oids: ['l3'] });
  assert(A.doc.liveObjects().some((o) => o.oid === 'l3'), 'restored object comes back');
}

console.log('\n[12] 图形识别 & 手写识别');
{
  // 圆：绕一圈的点
  const circle = [];
  for (let i = 0; i <= 40; i++) { const a = i / 40 * Math.PI * 2; circle.push({ x: 100 + 60 * Math.cos(a), y: 100 + 60 * Math.sin(a) }); }
  const r = WB.recognizeShape(circle);
  assert(r && r.type === 'ellipse', `closed round stroke recognized as ellipse (got ${r && r.type})`);
  // 直线
  const line = []; for (let i = 0; i <= 20; i++) line.push({ x: i * 5, y: 100 });
  assert(WB.recognizeShape(line).type === 'line', 'straight stroke recognized as line');
  // 手写数字 2（从模板生成的逆过程不易构造，这里只验证接口稳定 + 噪声返回 null）
  const scribble = [];
  for (let i = 0; i <= 30; i++) scribble.push({ x: 100 + 40 * Math.sin(i * 1.7), y: 100 + 40 * Math.cos(i * 2.3) });
  const h = WB.recognizeHandwriting(scribble);
  assert(h === null || typeof h.char === 'string', 'handwriting recognizer returns null or {char,score}');
}

console.log(`\n========================================`);
console.log(`KERNEL RESULT: ${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
