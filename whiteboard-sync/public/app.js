'use strict';

/* ===========================================================================
 * 协作白板 v2 - 前端（CRDT 协作内核 + 对象化渲染）
 *
 * 状态来源：WB.Doc（LWW CRDT 物化结果）。本地编辑乐观预提交 → 经 WB.Clock 签名
 * 成信封（clientId/lamport/clock 依赖向量）→ 服务端因果广播；seq 只做日志观测。
 *
 * 渲染：对象按分数 z 排序整帧重绘（requestAnimationFrame 合批）；
 *       压感笔迹发送前用 RDP 简化、Catmull-Rom/B 样条平滑，宽度随点传输。
 * 橡皮：像素擦按 16x16 单元分块增量同步；对象擦/整笔擦直接产生 delete 事务。
 * 撤销：WB.UndoManager 只记录自己的编辑；逆操作带 inv，被他人后续改过的字段自动空转。
 * ========================================================================= */

const $ = (id) => document.getElementById(id);
const boardWrap = $('boardWrap');
const mainCanvas = $('mainCanvas');
const ctx = mainCanvas.getContext('2d');
const textEditor = $('textEditor');

/* ------------------------------ 基础状态 ------------------------------ */
const userId = getOrCreateUserId();
let roomId = null;

const clock = new WB.Clock(userId);
const buf = new WB.CausalBuffer();
const doc = new WB.Doc();
const history = new Map();          // envId -> env（撤销时生成逆操作需要原操作体）
const undoMgr = new WB.UndoManager(clock, {
  makeEnv: (opBody, inv) => WB.makeEnvelope(clock, opBody, inv ? { inv } : {})
});

const pending = new Map();          // 未被服务端确认的信封 id -> env（断线重发，服务端幂等）
let lastSeq = 0;

let cssW = 0, cssH = 0, dpr = 1;
let renderQueued = false;
let version = 0;                    // 文档版本号，自增触发重绘

/* ------------------------------ 工具状态 ------------------------------ */
const state = {
  tool: 'select',
  color: '#1f2937',
  width: 4,
  shape: 'rect',
  eraseMode: 'pixel',
  recognize: true,
  hwr: false,
  smooth: 'catmull',
  selection: new Set(),             // oid 集合（选中对象）
  gesture: null                     // 当前指针手势（绘制/橡皮/框选/变换）
};

/* ============================== 工具函数 ============================== */
function getOrCreateUserId() {
  let id = null;
  try { id = localStorage.getItem('wb2_user_id'); } catch (_) { /* noop */ }
  if (!id) {
    id = 'u-' + Math.random().toString(36).slice(2, 10);
    try { localStorage.setItem('wb2_user_id', id); } catch (_) { /* noop */ }
  }
  return id;
}
const newId = (p) => WB.uid(p);
const sendMsg = (o) => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(o)); return true; } catch (_) { /* noop */ }
  }
  return false;
};
/* v3：存在二进制同步栈时启用（SyncClient + WebRTC mesh + IndexedDB 离线）。
 * 测试/无 WBNet 环境自动回退到 v2 JSON WebSocket 路径。 */
const USE_V3 = typeof WBNet !== 'undefined' && typeof WBSync !== 'undefined' &&
  typeof WebSocket !== 'undefined' && typeof RTCPeerConnection !== 'undefined';
let sync3 = null;
/** 信封统一出口：v3 走 SyncClient（P2P/服务端双通道 + 持久化 + 背压队列），否则 JSON */
function transmitEnvelopes(list) {
  if (sync3) { sync3.publish(list); return true; }
  return sendMsg({ type: 'ops', envelopes: list });
}
function bump() { version += 1; scheduleRender(); }
function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => { renderQueued = false; render(); });
}

/* ========================== 信封提交（网络层） ========================== */
/**
 * 提交一“次”编辑：1~N 个信封（事务自动绑定同一 txnId）。
 * 本地乐观物化，记录撤销历史，再发给服务端广播。
 */
function commitEnvelopes(envs, opts) {
  opts = opts || {};
  const list = Array.isArray(envs) ? envs : [envs];
  if (list.length > 1) WB.atomic(list);

  // 本地因果缓冲 + 物化（自己的操作立即生效）
  const ready = buf.push(list);
  for (const e of ready) {
    clock.observeLamport(e.lamport);
    clock.mergeVC(e.clock);
    doc.apply(e);
    history.set(e.id, e);
    if (!sync3) pending.set(e.id); // v3 的未确认持久化由 SyncClient/IndexedDB 负责
  }
  if (!opts.silent) {
    undoMgr.record(list, opts.summary || list[0].op.kind, opts.inverseOps || null);
    refreshUndoUI();
  }

  transmitEnvelopes(list);
  bump();
  return list;
}

/** 单对象字段更新（可带 squashKey：连续移动/缩放合并为最终状态） */
function commitSet(oid, fields, opts) {
  opts = opts || {};
  const env = WB.makeEnvelope(clock, { kind: 'set', oid, fields, prev: opts.prev || null },
    opts.squashKey ? { squashKey: opts.squashKey } : {});
  return commitEnvelopes([env], { summary: opts.summary, inverseOps: opts.inverseOps });
}

/** 多对象同字段事务（一次移动多个对象等）：要么全部可见，要么全部不可见 */
function commitSetMany(updates, opts) {
  opts = opts || {};
  const envs = updates.map((u) => WB.makeEnvelope(clock,
    { kind: 'set', oid: u.oid, fields: u.fields, prev: u.prev || null },
    opts.squashKey ? { squashKey: u.squashKey } : {}));
  return commitEnvelopes(envs, {
    summary: opts.summary, inverseOps: opts.inverseOps, silent: !!opts.silent
  });
}

/* ========================== 远端信封 / 快照 ========================== */
function ingestRemoteEnvelopes(envelopes) {
  let changed = false;
  // 整批先入队再冲刷：保证事务整组一起就绪，不渲染“半成品”
  buf.enqueue(envelopes);
  const ready = buf.drain();
  for (const e of ready) {
    clock.observeLamport(e.lamport);
    clock.mergeVC(e.clock);
    if (doc.apply(e)) changed = true;
    history.set(e.id, e);
  }
  if (changed) bump();
  return ready;
}

function loadSnapshot(msg) {
  doc.loadSnapshot(msg.snapshot);
  // 把快照基线版本向量并入因果缓冲，使后续信封的依赖判定连续
  for (const k of Object.keys(msg.snapshot.known || {})) {
    buf.known[k] = msg.snapshot.known[k];
    clock.mergeVC({ [k]: msg.snapshot.known[k] });
  }
  for (const e of msg.envelopes || []) history.set(e.id, e);
  // 叠加水位之后的信封（幂等，已在快照里的 id 不会重复 apply）
  ingestRemoteEnvelopes((msg.envelopes || []).slice());
  if (Number.isInteger(msg.lastSeq)) lastSeq = msg.lastSeq;
  updateStats();
  bump();
}

/* ============================== Canvas ============================== */
function resizeCanvas() {
  const rect = boardWrap.getBoundingClientRect();
  const w = Math.floor(rect.width), h = Math.floor(rect.height);
  if (w <= 0 || h <= 0) return;
  cssW = w; cssH = h;
  dpr = Math.min(window.devicePixelRatio || 1, 3);
  mainCanvas.width = Math.round(w * dpr);
  mainCanvas.height = Math.round(h * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  scheduleRender();
}

/* ------------------------------ 渲染 ------------------------------ */
function render() {
  ctx.clearRect(0, 0, cssW, cssH);
  const objs = doc.liveObjects();
  for (const obj of objs) drawObject(ctx, obj);
  // 当前正在绘制的临时笔迹（尚未提交）：实时叠加
  if (state.gesture && state.gesture.live) drawGesture(ctx, state.gesture);
  // 选择框 / 变换手柄
  if (state.selection.size && state.tool === 'select') drawSelection(ctx);
}

function normTr(obj) {
  const t = obj.tr || {};
  return { tx: t.tx || 0, ty: t.ty || 0, sx: t.sx == null ? 1 : t.sx, sy: t.sy == null ? 1 : t.sy, r: t.r || 0 };
}

function drawObject(c, obj) {
  c.save();
  // 统一仿射变换（移动/缩放/旋转产生），对笔迹与图形一视同仁
  const tr = normTr(obj);
  if (tr.tx || tr.ty || tr.sx !== 1 || tr.sy !== 1 || tr.r) {
    const b = bbox(obj);
    const cx = b.x + b.w / 2, cy = b.y + b.h / 2;
    c.translate(cx + tr.tx, cy + tr.ty);
    c.rotate(tr.r);
    c.scale(tr.sx, tr.sy);
    c.translate(-cx, -cy);
  }
  switch (obj.type) {
    case 'stroke': drawStroke(c, obj); break;
    case 'rect': drawShape(c, obj, 'rect'); break;
    case 'ellipse': drawShape(c, obj, 'ellipse'); break;
    case 'triangle': drawShape(c, obj, 'triangle'); break;
    case 'arrow': drawShape(c, obj, 'arrow'); break;
    case 'line': drawShape(c, obj, 'line'); break;
    case 'text': drawText(c, obj); break;
    case 'note': drawNote(c, obj); break;
    case 'image': drawImageObj(c, obj); break;
  }
  c.restore();
}

/** 把屏幕坐标逆变换到对象局部坐标（命中测试用） */
function toLocal(x, y, obj) {
  const tr = normTr(obj);
  if (!tr.tx && !tr.ty && tr.sx === 1 && tr.sy === 1 && !tr.r) return { x, y };
  const b = bbox(obj);
  const cx = b.x + b.w / 2, cy = b.y + b.h / 2;
  let px = x - (cx + tr.tx), py = y - (cy + tr.ty);
  if (tr.r) {
    const cos = Math.cos(-tr.r), sin = Math.sin(-tr.r);
    const rx = px * cos - py * sin, ry = px * sin + py * cos;
    px = rx; py = ry;
  }
  px = px / tr.sx + cx; py = py / tr.sy + cy;
  return { x: px, y: py };
}

function strokeStyle(s) {
  return s || { brush: 'pen', color: state.color, width: state.width, smooth: state.smooth };
}

/** 压感笔迹：沿平滑曲线按每点宽度填充带状多边形（发送端预算好的 w 随点传输） */
function drawStroke(c, obj) {
  const s = obj.stroke || strokeStyle();
  const pts = s.points || [];
  if (!pts.length) return;
  const erased = doc.erasedCells(obj.oid);
  const brush = s.brush || 'pen';

  c.save();
  if (brush === 'highlighter') { c.globalAlpha = 0.35; c.globalCompositeOperation = 'multiply'; }

  const path = WB.smoothPath(pts, s.smooth || 'catmull');
  const widths = pts.map((p) => Number.isFinite(p.w) ? p.w : (s.width || 4));
  const cellSize = s.cellSize || s.width || 4;
  const tileSpan = cellSize * WB.TILE_CELLS;

  const cellErased = (p) => {
    if (!erased.size) return false;
    const tx = Math.floor(p.x / tileSpan), ty = Math.floor(p.y / tileSpan);
    const cx = Math.floor((p.x - tx * tileSpan) / cellSize);
    const cy = Math.floor((p.y - ty * tileSpan) / cellSize);
    return erased.has(tx + ',' + ty + ',' + cx + ',' + cy);
  };

  c.strokeStyle = s.color || '#000';
  c.fillStyle = s.color || '#000';
  c.lineCap = 'round';
  c.lineJoin = 'round';

  if (pts.length === 1) {
    if (!cellErased(pts[0])) {
      c.beginPath(); c.arc(pts[0].x, pts[0].y, widths[0] / 2, 0, Math.PI * 2); c.fill();
    }
    c.restore();
    return;
  }

  if (brush === 'dashed') c.setLineDash([Math.max(4, s.width * 1.6), Math.max(3, s.width)]);

  // 沿曲线逐段画“变宽圆头线段”，被擦单元跳过（像素擦除是分块增量同步，不触发整笔重绘数据）
  for (let i = 1; i < path.length; i++) {
    const prev = path[i - 1], seg = path[i];
    const w = widths[Math.min(i, widths.length - 1)];
    if (cellErased(seg)) continue;
    c.lineWidth = w;
    c.beginPath();
    if (seg.q) {
      c.moveTo(prev.x, prev.y);
      c.quadraticCurveTo(seg.cx, seg.cy, seg.x, seg.y);
    } else if (seg.linear) {
      c.moveTo(prev.x, prev.y); c.lineTo(seg.x, seg.y);
    } else {
      c.moveTo(prev.x, prev.y);
      c.bezierCurveTo(seg.c1x, seg.c1y, seg.c2x, seg.c2y, seg.x, seg.y);
    }
    c.stroke();
  }

  if (brush === 'texture') {
    // 纹理笔：沿线盖点
    for (const p of pts) {
      if (cellErased(p)) continue;
      const r = (Number.isFinite(p.w) ? p.w : s.width || 4) / 2;
      c.beginPath(); c.arc(p.x, p.y, r * 0.7, 0, Math.PI * 2); c.fill();
    }
  }
  c.restore();
}

function drawShape(c, obj, kind) {
  const s = obj.stroke || strokeStyle();
  c.strokeStyle = obj.color || s.color || '#1f2937';
  c.fillStyle = obj.fill || (s.fill || 'rgba(0,0,0,0)');
  c.lineWidth = s.width || 3;
  c.lineJoin = 'round';
  c.beginPath();
  if (kind === 'rect') c.rect(obj.x, obj.y, obj.w, obj.h);
  else if (kind === 'ellipse') c.ellipse(obj.x + obj.w / 2, obj.y + obj.h / 2, Math.abs(obj.w / 2), Math.abs(obj.h / 2), 0, 0, Math.PI * 2);
  else if (kind === 'triangle') {
    c.moveTo(obj.x + obj.w / 2, obj.y);
    c.lineTo(obj.x + obj.w, obj.y + obj.h);
    c.lineTo(obj.x, obj.y + obj.h);
    c.closePath();
  } else if (kind === 'line') {
    c.moveTo(obj.x, obj.y); c.lineTo(obj.x + obj.w, obj.y + obj.h);
  } else if (kind === 'arrow') {
    drawArrowHeadPath(c, obj.x, obj.y, obj.x + obj.w, obj.y + obj.h, s.width || 3);
  }
  if (obj.fill) c.fill();
  c.stroke();
}

function drawArrowHeadPath(c, x1, y1, x2, y2, w) {
  const ang = Math.atan2(y2 - y1, x2 - x1);
  const head = Math.max(10, w * 3.2);
  c.moveTo(x1, y1); c.lineTo(x2, y2);
  c.moveTo(x2, y2);
  c.lineTo(x2 - head * Math.cos(ang - Math.PI / 6), y2 - head * Math.sin(ang - Math.PI / 6));
  c.moveTo(x2, y2);
  c.lineTo(x2 - head * Math.cos(ang + Math.PI / 6), y2 - head * Math.sin(ang + Math.PI / 6));
}

function drawText(c, obj) {
  const size = obj.fontSize || 20;
  c.fillStyle = obj.color || '#111827';
  c.font = `${obj.bold ? '700' : '400'} ${size}px system-ui, sans-serif`;
  c.textBaseline = 'top';
  const lines = String(obj.content || '').split('\n');
  lines.forEach((line, i) => c.fillText(line, obj.x, obj.y + i * size * 1.25));
}

function drawNote(c, obj) {
  c.fillStyle = obj.color || '#fde68a';
  c.fillRect(obj.x, obj.y, obj.w, obj.h);
  c.strokeStyle = 'rgba(0,0,0,0.15)';
  c.strokeRect(obj.x, obj.y, obj.w, obj.h);
  c.fillStyle = '#1f2937';
  c.font = '15px system-ui, sans-serif';
  c.textBaseline = 'top';
  String(obj.content || '').split('\n').forEach((line, i) => c.fillText(line, obj.x + 8, obj.y + 8 + i * 19));
}

const imageCache = new Map();
function drawImageObj(c, obj) {
  const url = obj.src;
  const cached = imageCache.get(obj.oid);
  if (cached && cached.img.complete) {
    c.drawImage(cached.img, obj.x, obj.y, obj.w, obj.h);
    return;
  }
  // 未加载完成：占位
  c.fillStyle = '#e5e7eb';
  c.fillRect(obj.x, obj.y, obj.w, obj.h);
  if (url) {
    const img = new Image();
    img.onload = scheduleRender;
    img.src = url;
    imageCache.set(obj.oid, { img });
  }
}

/* ------------------------------ 选择框 ------------------------------ */
function drawSelection(c) {
  for (const oid of state.selection) {
    const obj = doc.get(oid);
    if (!obj || obj.deleted === true) continue;
    c.save();
    c.strokeStyle = '#2563eb';
    c.lineWidth = 1.5;
    c.setLineDash([5, 4]);
    let { x, y, w, h } = bbox(obj);
    c.strokeRect(x - 4, y - 4, w + 8, h + 8);
    c.setLineDash([]);
    // 四角缩放手柄 + 顶部旋转手柄
    c.fillStyle = '#fff';
    for (const [hx, hy] of [[x - 4, y - 4], [x + w + 4, y - 4], [x - 4, y + h + 4], [x + w + 4, y + h + 4]]) {
      c.fillRect(hx - 4, hy - 4, 8, 8); c.strokeRect(hx - 4, hy - 4, 8, 8);
    }
    c.beginPath(); c.arc(x + w / 2, y - 18, 4.5, 0, Math.PI * 2); c.fill(); c.stroke();
    c.beginPath(); c.moveTo(x + w / 2, y - 14); c.lineTo(x + w / 2, y - 4); c.stroke();
    c.restore();
  }
}

function bbox(obj) {
  let x0, y0, x1, y1;
  if (obj.type === 'stroke') {
    const pts = (obj.stroke && obj.stroke.points) || [];
    if (!pts.length) return { x: 0, y: 0, w: 0, h: 0 };
    x0 = Math.min.apply(null, pts.map((p) => p.x)); y0 = Math.min.apply(null, pts.map((p) => p.y));
    x1 = Math.max.apply(null, pts.map((p) => p.x)); y1 = Math.max.apply(null, pts.map((p) => p.y));
  } else {
    // line/arrow 用向量 (x,y)->(x+w,y+h)，w/h 可能为负
    x0 = Math.min(obj.x, obj.x + (obj.w || 0)); y0 = Math.min(obj.y, obj.y + (obj.h || 0));
    x1 = Math.max(obj.x, obj.x + (obj.w || 0)); y1 = Math.max(obj.y, obj.y + (obj.h || 0));
  }
  const tr = normTr(obj);
  // 平移/缩放直接作用于包围盒；旋转后取四角并集（近似 AABB）
  const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
  const hw = Math.abs((x1 - x0) / 2) * Math.abs(tr.sx), hh = Math.abs((y1 - y0) / 2) * Math.abs(tr.sy);
  if (tr.r) {
    const cos = Math.abs(Math.cos(tr.r)), sin = Math.abs(Math.sin(tr.r));
    const w = 2 * (hw * cos + hh * sin), h = 2 * (hw * sin + hh * cos);
    return { x: cx + tr.tx - w / 2, y: cy + tr.ty - h / 2, w, h };
  }
  return { x: cx + tr.tx - hw, y: cy + tr.ty - hh, w: 2 * hw, h: 2 * hh };
}

/* ============================== 指针输入 ============================== */
function eventPoint(e) {
  const rect = mainCanvas.getBoundingClientRect();
  return { x: e.clientX - rect.left, y: e.clientY - rect.top };
}
function samplePoint(e) {
  const p = eventPoint(e);
  p.p = Number.isFinite(e.pressure) && e.pressure > 0 ? e.pressure : 0.5;
  p.tx = Number.isFinite(e.tiltX) ? e.tiltX : 0;
  p.ty = Number.isFinite(e.tiltY) ? e.tiltY : 0;
  p.t = e.timeStamp | 0;
  return p;
}

let hitHandle = null;

mainCanvas.addEventListener('pointerdown', (e) => {
  if (e.button !== undefined && e.button !== 0) return;
  e.preventDefault();
  try { mainCanvas.setPointerCapture(e.pointerId); } catch (_) { /* noop */ }
  const p = samplePoint(e);

  if (state.tool === 'select') return beginSelectGesture(e, p);
  if (state.tool === 'eraser') return beginEraseGesture(e, p);
  if (state.tool === 'text' || state.tool === 'note') return beginPlaceObject(e, p);
  // pen / highlighter / dashed / texture / shape 都是“一笔”手势
  beginStrokeGesture(e, p);
});

mainCanvas.addEventListener('pointermove', (e) => {
  const g = state.gesture;
  if (!g) { updateCursor(e); return; }
  e.preventDefault();
  const events = typeof e.getCoalescedEvents === 'function' && e.getCoalescedEvents().length
    ? e.getCoalescedEvents() : [e];
  if (g.kind === 'transform') {
    // 变换只取最终指针位置：每个 move 事件提交一帧，避免高频小信封洪流
    applyTransform(g, samplePoint(e));
  } else {
    for (const ev of events) {
      const p = samplePoint(ev);
      if (g.kind === 'stroke') {
        const last = g.points[g.points.length - 1];
        if (last && Math.abs(last.x - p.x) < 0.01 && Math.abs(last.y - p.y) < 0.01) continue;
        g.points.push(p);
      } else if (g.kind === 'erase') {
        g.points.push(p);
      } else if (g.kind === 'marquee' || g.kind === 'place') {
        g.cur = p;
      }
    }
  }
  g.live = true;
  scheduleRender();
});

mainCanvas.addEventListener('pointerup', (e) => {
  const g = state.gesture;
  if (!g) return;
  e.preventDefault();
  try { mainCanvas.releasePointerCapture(e.pointerId); } catch (_) { /* noop */ }
  state.gesture = null;
  if (g.kind === 'transform') finalizeTransform(g);
  else finishGesture(g, samplePoint(e));
});
mainCanvas.addEventListener('pointercancel', () => { state.gesture = null; scheduleRender(); });
mainCanvas.addEventListener('contextmenu', (e) => e.preventDefault());

/* ------------------------------ 笔迹手势 ------------------------------ */
function beginStrokeGesture(e, p) {
  state.gesture = {
    kind: 'stroke',
    points: [p],
    live: true,
    brush: ['pen', 'highlighter', 'dashed', 'texture'].includes(state.tool) ? state.tool
      : (state.tool === 'shape' ? 'pen' : 'pen')
  };
}

function drawGesture(c, g) {
  if (g.kind === 'stroke') {
    const fake = {
      stroke: {
        brush: state.tool === 'shape' ? 'pen' : state.tool,
        color: state.color, width: state.width, smooth: state.smooth,
        points: withLiveWidths(g.points, state.width), cellSize: state.width
      }
    };
    drawStroke(c, fake);
  } else if (g.kind === 'erase') {
    // 橡皮路径预览
    c.save();
    c.strokeStyle = 'rgba(37,99,235,0.5)'; c.lineWidth = g.eraserSize || 12;
    c.lineCap = 'round';
    c.beginPath();
    g.points.forEach((p, i) => i ? c.lineTo(p.x, p.y) : c.moveTo(p.x, p.y));
    c.stroke();
    c.restore();
  } else if (g.kind === 'marquee') {
    const { x, y, w, h } = rectOf(g.start, g.cur);
    c.save(); c.strokeStyle = '#2563eb'; c.setLineDash([4, 3]);
    c.strokeRect(x, y, w, h); c.restore();
  } else if (g.kind === 'place') {
    const { x, y, w, h } = rectOf(g.start, g.cur);
    c.save(); c.strokeStyle = '#2563eb'; c.setLineDash([4, 3]);
    c.strokeRect(x, y, w, h); c.restore();
  }
}

function withLiveWidths(points, base) {
  // 本地实时笔迹：用内核同一公式即时算宽（提交前 RDP 简化后再正式算一次）
  return points.map((p, i) => ({ x: p.x, y: p.y, w: WB.pointWidth(p, points[i - 1], base) }));
}

function finishGesture(g, endPoint) {
  if (g.kind === 'stroke') finishStroke(g);
  else if (g.kind === 'erase') finishErase(g);
  else if (g.kind === 'marquee') finishMarquee(g);
  else if (g.kind === 'place') finishPlace(g);
}

/**
 * 收笔：
 *  1. RDP 简化点集（保留压感/倾斜/速度/时间戳）；
 *  2. 重新按共享公式计算每点宽度（压感变宽、速度变细）；
 *  3. 图形识别 / 手写转文字（命中则产生对应对象，原笔迹不入库，无重复笔迹）；
 *  4. create 信封同步给所有端。
 */
function finishStroke(g) {
  let raw = g.points;
  if (raw.length < 1) return;
  const epsilon = Math.max(0.8, state.width * 0.25);
  let points = raw.length > 2 ? WB.rdp(raw, epsilon) : raw.slice();
  const widths = WB.computeWidths(points, state.width);
  points = points.map((p, i) => ({ x: round2(p.x), y: round2(p.y), p: round3(p.p), tx: p.tx || 0, ty: p.ty || 0, t: p.t, w: round2(widths[i]) }));

  // 手写转文字（仅在开启时）
  if (state.hwr && points.length >= 8) {
    const hr = WB.recognizeHandwriting(raw);
    if (hr) { createTextObject(raw[0].x, raw[0].y, hr.char, { recognized: true }); return; }
  }

  // 图形识别（笔工具/图形工具下，开启识别时）
  if (state.recognize && state.tool !== 'shape') {
    const rec = WB.recognizeShape(raw);
    if (rec) { createShapeObject(rec); return; }
  }
  // 图形工具：直接插入所选形状。line/arrow 用起终点向量，其余用包围盒
  if (state.tool === 'shape') {
    const a = raw[0], z = raw[raw.length - 1];
    const fields = (state.shape === 'line' || state.shape === 'arrow')
      ? { x: a.x, y: a.y, w: z.x - a.x, h: z.y - a.y }
      : bboxFields(raw);
    createShapeObject({ type: state.shape, fields });
    return;
  }

  const oid = newId('obj');
  const stroke = {
    brush: g.brush, color: state.color, width: state.width,
    smooth: state.smooth, cellSize: state.width, points
  };
  commitEnvelopes([WB.makeEnvelope(clock, {
    kind: 'create',
    objects: [{ oid, type: 'stroke', fields: { stroke, z: nextZ(), color: state.color } }]
  })], { summary: `${brushName(g.brush)}笔迹` });
}

function brushName(b) { return ({ pen: '钢笔', highlighter: '荧光笔', dashed: '虚线', texture: '纹理' })[b] || '笔'; }
const round2 = (v) => Math.round(v * 100) / 100;
const round3 = (v) => Math.round(v * 1000) / 1000;

function bboxFields(points) {
  const xs = points.map((p) => p.x), ys = points.map((p) => p.y);
  const x = Math.min.apply(null, xs), y = Math.min.apply(null, ys);
  return { x, y, w: Math.max.apply(null, xs) - x, h: Math.max.apply(null, ys) - y };
}

function createShapeObject(rec) {
  const oid = newId('obj');
  const fields = Object.assign({
    x: round2(rec.fields.x), y: round2(rec.fields.y),
    w: round2(rec.fields.w), h: round2(rec.fields.h),
    stroke: { width: state.width },
    color: state.color, z: nextZ()
  }, rec.fields.rot ? { rot: rec.fields.rot } : null);
  commitEnvelopes([WB.makeEnvelope(clock, {
    kind: 'create', objects: [{ oid, type: rec.type, fields }]
  })], { summary: '图形:' + rec.type });
}

function selectOnly(oid) {
  state.selection = new Set([oid]);
  refreshObjectOps();
  scheduleRender();
}

function createTextObject(x, y, text, extra) {  const oid = newId('obj');
  commitEnvelopes([WB.makeEnvelope(clock, {
    kind: 'create',
    objects: [{ oid, type: 'text', fields: { x, y, w: 200, h: 40, content: text, color: state.color, fontSize: 22, z: nextZ() } }]
  })], { summary: extra && extra.recognized ? '手写转文字' : '文本' });
  selectOnly(oid);
}

function createNoteObject(x, y, w, h) {
  const oid = newId('obj');
  commitEnvelopes([WB.makeEnvelope(clock, {
    kind: 'create',
    objects: [{ oid, type: 'note', fields: { x, y, w: Math.max(80, w), h: Math.max(80, h), content: '', color: '#fde68a', z: nextZ() } }]
  })], { summary: '便签' });
  selectOnly(oid);
  openTextEditor(doc.get(oid));
}

/* ------------------------------ 选择 / 变换 ------------------------------ */
function beginSelectGesture(e, p) {
  // 1. 手柄优先（缩放/旋转）
  hitHandle = hitTestHandles(p);
  if (hitHandle) {
    state.gesture = {
      kind: 'transform', mode: hitHandle, start: p,
      origs: snapshotSelection(), token: newId('g'), envIds: [],
      summary: hitHandle === 'rotate' ? '旋转' : '缩放'
    };
    return;
  }
  // 2. 命中对象：开始移动
  const top = topObjectAt(p.x, p.y);
  if (top) {
    if (!e.shiftKey && !state.selection.has(top.oid)) state.selection = new Set([top.oid]);
    else if (e.shiftKey) {
      if (state.selection.has(top.oid)) state.selection.delete(top.oid);
      else state.selection.add(top.oid);
    }
    const origs = snapshotSelection();
    state.gesture = {
      kind: 'transform', mode: 'move', start: p, origs,
      token: newId('g'),
      envIds: [],
      summary: '移动'
    };
    refreshObjectOps();
    scheduleRender();
    return;
  }
  // 3. 空白：框选
  if (!e.shiftKey) state.selection.clear();
  state.gesture = { kind: 'marquee', start: p, cur: p };
  refreshObjectOps();
  scheduleRender();
}

function snapshotSelection() {
  const m = new Map();
  for (const oid of state.selection) {
    const o = doc.get(oid);
    if (o) {
      const tr = normTr(o);
      m.set(oid, { tr, w0: bbox(o).w, h0: bbox(o).h });
    }
  }
  return m;
}

/**
 * 一次移动/缩放/旋转手势结束：
 * 把整段高频帧在撤销栈中登记为“一条”操作。逆操作恢复手势前快照（原子事务），
 * 且 originLamport 取手势内最新帧 —— 他人若在本次移动之后改过对象，撤销自动空转。
 */
function finalizeTransform(g) {
  if (!g.envIds.length) return;
  const inverseOps = [];
  for (const [oid, orig] of g.origs) {
    inverseOps.push({ kind: 'set', oid, fields: { tr: orig.tr } });
  }
  undoMgr.record(g.envIds.map((id) => history.get(id)).filter(Boolean), g.summary, inverseOps);
  refreshUndoUI();
}

function applyTransform(g, p) {
  const dx = p.x - g.start.x, dy = p.y - g.start.y;
  const updates = [];
  for (const [oid, orig] of g.origs) {
    const t0 = orig.tr;
    let fields;
    if (g.mode === 'move') {
      fields = { tr: { tx: round2(t0.tx + dx), ty: round2(t0.ty + dy), sx: t0.sx, sy: t0.sy, r: t0.r } };
    } else if (g.mode === 'rotate') {
      const o = doc.get(oid);
      const b = bbox(o);
      // 几何中心随当前变换（首帧 bbox 已含 t0，位移后中心也正确）
      const cx = b.x + b.w / 2, cy = b.y + b.h / 2;
      // 用“当前指针角 - 手势起始角”的增量，避免中心偏移造成跳角
      const a0 = Math.atan2(g.start.y - cy, g.start.x - cx);
      const a1 = Math.atan2(p.y - cy, p.x - cx);
      fields = { tr: { tx: t0.tx, ty: t0.ty, sx: t0.sx, sy: t0.sy, r: round2(t0.r + (a1 - a0)) } };
    } else {
      // 以包围盒中心为锚缩放：手柄在右/底侧增大正方向尺寸
      const signX = g.mode.includes('r') ? 1 : -1;
      const signY = g.mode.includes('b') ? 1 : -1;
      const sx = Math.max(0.05, (orig.w0 + signX * dx) / orig.w0);
      const sy = Math.max(0.05, (orig.h0 + signY * dy) / orig.h0);
      fields = { tr: { tx: t0.tx, ty: t0.ty, sx: round3(sx), sy: round3(sy), r: t0.r } };
    }
    updates.push({ oid, fields, prev: { tr: t0 }, squashKey: `${g.mode}:${g.token}:${oid}` });
  }
  if (!updates.length) return;
  // 高频帧静默提交（不进撤销栈）；squashKey 精确到手势×对象，服务端/日志压缩为最终状态
  const envs = commitSetMany(updates, { squashKey: true, silent: true, summary: g.summary });
  for (const e of envs) if (!g.envIds.includes(e.id)) g.envIds.push(e.id);
}

function finishMarquee(g) {
  const r = rectOf(g.start, g.cur);
  if (r.w < 4 && r.h < 4) { scheduleRender(); return; }
  const hits = WB.objectsInRect(r.x, r.y, r.w, r.h, doc.liveObjects()).map((o) => o.oid);
  state.selection = new Set(hits);
  refreshObjectOps();
  scheduleRender();
}

function beginPlaceObject(e, p) {
  if (state.tool === 'text') {
    // 单击落点创建文本并就地编辑
    createTextObject(p.x, p.y, '');
    queueMicrotask(() => openTextEditor(doc.get([...state.selection][0])));
    state.gesture = null;
  } else {
    state.gesture = { kind: 'place', start: p, cur: p };
  }
}
function finishPlace(g) {
  const r = rectOf(g.start, g.cur);
  if (state.tool === 'note') createNoteObject(r.x, r.y, r.w, r.h);
}

/* ------------------------------ 橡皮擦 ------------------------------ */
function beginEraseGesture(e, p) {
  state.gesture = { kind: 'erase', points: [p], eraserSize: 20, live: true, sentCells: new Set() };
}

function finishErase(g) {
  if (!g.points.length) return;
  const mode = state.eraseMode;
  const live = doc.liveObjects();

  if (mode === 'object' || mode === 'stroke') {
    // 对象擦 / 整笔擦：命中的对象一次性 delete（多对象也是原子事务）
    const radius = g.eraserSize / 2;
    let hits;
    if (mode === 'stroke') hits = WB.hitStrokes(g.points, live.filter((o) => o.type === 'stroke'), radius);
    else hits = new Set(WB.objectsInRect(
      Math.min.apply(null, g.points.map((p) => p.x)) - radius,
      Math.min.apply(null, g.points.map((p) => p.y)) - radius,
      Math.max.apply(null, g.points.map((p) => p.x)) - Math.min.apply(null, g.points.map((p) => p.x)) + radius * 2,
      Math.max.apply(null, g.points.map((p) => p.y)) - Math.min.apply(null, g.points.map((p) => p.y)) + radius * 2,
      live).map((o) => o.oid));
    // 对象擦也用路径命中（更跟手）
    if (mode === 'object') {
      // 沿整条擦除路径判定命中（跟手），并把屏幕点逆变换到对象局部坐标
      hits = new Set(live.filter((o) => g.points.some((q) => {
        const loc = toLocal(q.x, q.y, o);
        return WB.pointInObject(loc.x, loc.y, o, radius);
      })).map((o) => o.oid));
    }
    if (hits.size) deleteObjects([...hits], mode === 'stroke' ? '整笔擦除' : '对象擦除');
    return;
  }

  // 像素擦：分块。只同步被触碰的块+块内单元，增量不全量。
  const perOid = collectPixelChunks(g.points, live, g.eraserSize);
  const envs = [];
  for (const [oid, chunks] of perOid) {
    // 过滤本手势已发送的单元（连续手势合并）
    const fresh = chunks.filter((ch) => {
      return ch.cells.some((c) => !g.sentCells.has(chunkKey(oid, ch, c)));
    });
    fresh.forEach((ch) => ch.cells.forEach((c) => g.sentCells.add(chunkKey(oid, ch, c))));
    if (!fresh.length) continue;
    envs.push(WB.makeEnvelope(clock, { kind: 'erase', chunks: fresh.map((ch) => Object.assign({ oid }, ch)) }));
  }
  if (envs.length) commitEnvelopes(envs, { summary: '像素擦除' });
}

function chunkKey(oid, ch, c) { return oid + ':' + ch.tx + ':' + ch.ty + ':' + c[0] + ',' + c[1]; }

/**
 * 把擦除路径对每条受影响笔迹光栅化为分块单元。
 * cellSize 取笔迹自身宽度（与笔迹创建时一致），保证两端坐标对齐。
 */
function collectPixelChunks(path, live, eraserSize) {
  const perOid = new Map();
  for (const obj of live) {
    if (obj.type !== 'stroke') continue;
    const s = obj.stroke || {};
    const cellSize = s.cellSize || s.width || 4;
    // 快速过滤：擦除路径需真正划过笔迹（按笔迹折线 + 橡皮半径命中测试）
    if (!hitStrokeWithRadius(obj.stroke.points || [], path, Math.max(eraserSize / 2, cellSize))) continue;
    const { chunks } = WB.rasterizeErase(path, cellSize);
    // 单元级别只保留落在笔迹宽度走廊内的，避免擦到大片空白单元
    const kept = [];
    const tileSpan = cellSize * WB.TILE_CELLS;
    for (const ch of chunks) {
      const cells = ch.cells.filter(([cx, cy]) => {
        const wx = ch.tx * tileSpan + (cx + 0.5) * cellSize;
        const wy = ch.ty * tileSpan + (cy + 0.5) * cellSize;
        return hitStrokeWithRadius(obj.stroke.points || [], [{ x: wx, y: wy }], cellSize * 0.9);
      });
      if (cells.length) kept.push({ tx: ch.tx, ty: ch.ty, cells });
    }
    if (kept.length) perOid.set(obj.oid, kept);
  }
  return perOid;
}

/** 点/折线段是否在 radius 距离内命中笔迹折线（内核命中工具的细粒度版） */
function hitStrokeWithRadius(strokePts, pathPts, radius) {
  if (!strokePts.length || !pathPts.length) return false;
  for (const q of pathPts) {
    for (let i = 0; i < strokePts.length - 1; i++) {
      const a = strokePts[i], b = strokePts[i + 1];
      const dx = b.x - a.x, dy = b.y - a.y;
      const l2 = dx * dx + dy * dy || 1;
      let t = ((q.x - a.x) * dx + (q.y - a.y) * dy) / l2;
      t = Math.max(0, Math.min(1, t));
      if (Math.hypot(q.x - (a.x + t * dx), q.y - (a.y + t * dy)) <= radius) return true;
    }
    const p0 = strokePts[0];
    if (strokePts.length === 1 && Math.hypot(q.x - p0.x, q.y - p0.y) <= radius) return true;
  }
  return false;
}

/* ------------------------------ 对象操作 ------------------------------ */
function topObjectAt(x, y) {
  const objs = doc.liveObjects();
  for (let i = objs.length - 1; i >= 0; i--) {
    const local = toLocal(x, y, objs[i]);
    if (WB.pointInObject(local.x, local.y, objs[i])) return objs[i];
  }
  return null;
}

function rectOf(a, b) {
  return { x: Math.min(a.x, b.x), y: Math.min(a.y, b.y), w: Math.abs(b.x - a.x), h: Math.abs(b.y - a.y) };
}

function nextZ() {
  const zs = doc.liveObjects().map((o) => o.z).filter(Boolean).sort(WB.fracCmp);
  return WB.zForInsert(zs, zs.length);
}

function deleteObjects(oids, summary) {
  const env = WB.makeEnvelope(clock, { kind: 'delete', oids });
  commitEnvelopes([env], { summary: summary || '删除' });
  for (const id of oids) state.selection.delete(id);
  refreshObjectOps();
}

function layerMove(where) {
  const objs = doc.liveObjects();
  const zs = objs.map((o) => o.z).filter(Boolean).sort(WB.fracCmp);
  const updates = [];
  for (const oid of state.selection) {
    const obj = doc.get(oid);
    if (!obj || obj.deleted === true) continue;
    const idx = zs.findIndex((z) => z === obj.z);
    let z;
    if (where === 'top') z = WB.zForInsert(zs, zs.length);
    else if (where === 'bottom') z = WB.zForInsert(zs, 0);
    else if (where === 'up') z = WB.zForInsert(zs, Math.min(idx + 2, zs.length));
    else z = WB.zForInsert(zs, Math.max(idx, 0));
    updates.push(WB.makeEnvelope(clock, { kind: 'layer', oid, z, prevZ: obj.z || null }));
  }
  if (updates.length) commitEnvelopes(updates, { summary: '图层调整' });
}

function groupSelected() {
  const oids = [...state.selection];
  if (oids.length < 2) return;
  const gid = newId('grp');
  const env = WB.makeEnvelope(clock, { kind: 'group', gid, oids });
  commitEnvelopes([env], { summary: '组合' });
  state.selection = new Set(oids);
}

function ungroupSelected() {
  // 对每个 group id 发一条 ungroup（成员取该组全部对象），整组一条事务
  const groups = new Set();
  for (const oid of state.selection) { const o = doc.get(oid); if (o && o.group) groups.add(o.group); }
  const out = [];
  for (const gid of groups) {
    const members = doc.liveObjects().filter((o) => o.group === gid).map((o) => o.oid);
    out.push(WB.makeEnvelope(clock, { kind: 'ungroup', gid, oids: members }));
  }
  if (out.length) commitEnvelopes(out, { summary: '解组' });
}

/* ------------------------------ 文本编辑 ------------------------------ */
let editingOid = null;
function openTextEditor(obj) {
  if (!obj || (obj.type !== 'text' && obj.type !== 'note')) return;
  editingOid = obj.oid;
  textEditor.hidden = false;
  textEditor.style.left = obj.x + 'px';
  textEditor.style.top = obj.y + 'px';
  textEditor.style.width = (obj.w || 200) + 'px';
  textEditor.style.height = Math.max(obj.h || 40, 40) + 'px';
  textEditor.value = obj.content || '';
  if (obj.type === 'note') textEditor.classList.add('note-editor');
  else textEditor.classList.remove('note-editor');
  textEditor.focus();
}
textEditor.addEventListener('blur', commitTextEdit);
textEditor.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' || (e.key === 'Enter' && (e.ctrlKey || e.metaKey))) {
    commitTextEdit(); textEditor.blur();
  }
});
function commitTextEdit() {
  if (textEditor.hidden || !editingOid) return;
  const value = textEditor.value;
  textEditor.hidden = true;
  const oid = editingOid; editingOid = null;
  commitSet(oid, { content: value }, { summary: '编辑文本' });
}
mainCanvas.addEventListener('dblclick', (e) => {
  const p = eventPoint(e);
  const obj = topObjectAt(p.x, p.y);
  if (obj && (obj.type === 'text' || obj.type === 'note')) openTextEditor(obj);
});

/* ------------------------------ 命中手柄 ------------------------------ */
function hitTestHandles(p) {
  for (const oid of state.selection) {
    const b = bbox(doc.get(oid));
    const handles = {
      tl: [b.x - 4, b.y - 4], tr: [b.x + b.w + 4, b.y - 4],
      bl: [b.x - 4, b.y + b.h + 4], br: [b.x + b.w + 4, b.y + b.h + 4],
      rotate: [b.x + b.w / 2, b.y - 18]
    };
    for (const [name, [hx, hy]] of Object.entries(handles)) {
      if (Math.abs(p.x - hx) < 7 && Math.abs(p.y - hy) < 7) return name;
    }
  }
  return null;
}
function updateCursor(e) {
  if (state.tool !== 'select') { mainCanvas.style.cursor = 'crosshair'; return; }
  const p = eventPoint(e);
  const h = hitTestHandles(p);
  mainCanvas.style.cursor = h ? (h === 'rotate' ? 'grab' : 'nwse-resize') :
    (topObjectAt(p.x, p.y) ? 'move' : 'default');
}

/* ============================== 撤销 / 重做 ============================== */
function doUndo(selectiveEnvId) {
  const groups = selectiveEnvId
    ? undoMgr.undoSelective(history, selectiveEnvId)
    : undoMgr.undo(history);
  dispatchInverse(groups);
}
function doRedo() {
  const groups = undoMgr.redo(history);
  dispatchInverse(groups);
}
function dispatchInverse(groups) {
  if (!groups || !groups.length) { refreshUndoUI(); return; }
  const flat = groups.flat();
  const ready = buf.push(flat);
  for (const e of ready) {
    clock.observeLamport(e.lamport);
    clock.mergeVC(e.clock);
    doc.apply(e);
    history.set(e.id, e);
    if (!sync3) pending.set(e.id, e);
  }
  transmitEnvelopes(flat);
  refreshUndoUI();
  bump();
}
function refreshUndoUI() {
  $('undoBtn').disabled = !undoMgr.canUndo();
  $('redoBtn').disabled = !undoMgr.canRedo();
}

/* ============================== 工具栏 UI ============================== */
$('tools').addEventListener('click', (e) => {
  const btn = e.target.closest('.tool');
  if (!btn) return;
  const tool = btn.dataset.tool;
  if (tool === 'image') {
    // 图片是一次性动作：打开文件选择，但不把当前工具切成 image（否则之后点击会被当成画笔）
    pickImage();
    return;
  }
  state.tool = tool;
  document.querySelectorAll('.tool').forEach((b) => b.classList.toggle('active', b === btn));
  $('shapeTools').hidden = state.tool !== 'shape';
  $('eraserTools').hidden = state.tool !== 'eraser';
  refreshObjectOps();
});
$('shapeTools').addEventListener('click', (e) => {
  const btn = e.target.closest('.subtool'); if (!btn) return;
  state.shape = btn.dataset.shape;
  document.querySelectorAll('#shapeTools .subtool').forEach((b) => b.classList.toggle('active', b === btn));
});
$('eraserTools').addEventListener('click', (e) => {
  const btn = e.target.closest('.subtool'); if (!btn) return;
  state.eraseMode = btn.dataset.erase;
  document.querySelectorAll('#eraserTools .subtool').forEach((b) => b.classList.toggle('active', b === btn));
});
$('colors').addEventListener('click', (e) => {
  const btn = e.target.closest('.swatch'); if (!btn) return;
  state.color = btn.dataset.color;
  document.querySelectorAll('.swatch').forEach((b) => b.classList.toggle('active', b === btn));
});
$('widthInput').addEventListener('input', () => {
  state.width = Number($('widthInput').value);
  $('widthValue').textContent = String(state.width);
});
$('recognizeBtn').addEventListener('click', () => {
  state.recognize = !state.recognize;
  $('recognizeBtn').classList.toggle('active', state.recognize);
});
$('hwrBtn').addEventListener('click', () => {
  state.hwr = !state.hwr;
  $('hwrBtn').classList.toggle('active', state.hwr);
});

$('deleteBtn').addEventListener('click', () => deleteObjects([...state.selection]));
$('groupBtn').addEventListener('click', groupSelected);
$('ungroupBtn').addEventListener('click', ungroupSelected);
$('layerTop').addEventListener('click', () => layerMove('top'));
$('layerBottom').addEventListener('click', () => layerMove('bottom'));
$('layerUp').addEventListener('click', () => layerMove('up'));
$('layerDown').addEventListener('click', () => layerMove('down'));
$('undoBtn').addEventListener('click', () => doUndo());
$('redoBtn').addEventListener('click', () => doRedo());

function refreshObjectOps() {
  $('objectOps').hidden = state.selection.size === 0;
}

/* ------------------------------ 图片导入 ------------------------------ */
function pickImage() {
  $('imageFile').click();
}
$('imageFile').addEventListener('change', (e) => {
  const file = e.target.files && e.target.files[0];
  if (!file) return;
  const reader = new FileReader();
  reader.onload = () => {
    const img = new Image();
    img.onload = () => {
      // 超大图缩到最长边 1280，降低同步体积（dataURL）
      const maxD = 1280;
      const scale = Math.min(1, maxD / Math.max(img.width, img.height));
      const w = Math.round(img.width * scale), h = Math.round(img.height * scale);
      const cv = document.createElement('canvas');
      cv.width = w; cv.height = h;
      cv.getContext('2d').drawImage(img, 0, 0, w, h);
      const dataUrl = cv.toDataURL('image/jpeg', 0.82);
      const oid = newId('img');
      const x = 60, y = 60;
      commitEnvelopes([WB.makeEnvelope(clock, {
        kind: 'create',
        objects: [{ oid, type: 'image', fields: { x, y, w, h, src: dataUrl, z: nextZ() } }]
      })], { summary: '图片' });
      e.target.value = '';
    };
    img.src = reader.result;
  };
  reader.readAsDataURL(file);
});

/* ------------------------------ 历史面板（选择性撤销） ------------------------------ */
$('historyBtn').addEventListener('click', () => {
  const panel = $('historyPanel');
  panel.hidden = !panel.hidden;
  if (!panel.hidden) renderHistory();
});
$('historyClose').addEventListener('click', () => { $('historyPanel').hidden = true; });
function renderHistory() {
  const ul = $('historyList');
  ul.innerHTML = '';
  undoMgr.undoStack.slice().reverse().forEach((entry) => {
    const li = document.createElement('li');
    li.textContent = `${entry.summary} · ${new Date(entry.time).toLocaleTimeString()}`;
    li.title = '撤销这条自己的操作（若他人已改过相关字段会自动空转，不破坏他人结果）';
    li.addEventListener('click', () => {
      doUndo(entry.envIds[0]);
      renderHistory();
    });
    ul.appendChild(li);
  });
}

/* ------------------------------ 键盘快捷键 ------------------------------ */
window.addEventListener('keydown', (e) => {
  if (document.activeElement === textEditor || document.activeElement === $('roomInput') || document.activeElement === $('nameInput')) return;
  const meta = e.ctrlKey || e.metaKey;
  if (meta && e.key.toLowerCase() === 'z' && !e.shiftKey) { e.preventDefault(); doUndo(); renderHistory(); }
  else if (meta && (e.key.toLowerCase() === 'y' || (e.key.toLowerCase() === 'z' && e.shiftKey))) { e.preventDefault(); doRedo(); }
  else if (meta && e.key.toLowerCase() === 'g' && !e.shiftKey) { e.preventDefault(); groupSelected(); }
  else if (meta && e.shiftKey && e.key.toLowerCase() === 'g') { e.preventDefault(); ungroupSelected(); }
  else if (e.key === 'Delete' || e.key === 'Backspace') { if (state.selection.size) { e.preventDefault(); deleteObjects([...state.selection]); } }
  else if (e.key.toLowerCase() === 'v') setTool('select');
  else if (e.key.toLowerCase() === 'p') setTool('pen');
  else if (e.key.toLowerCase() === 'h') setTool('highlighter');
  else if (e.key.toLowerCase() === 'e') setTool('eraser');
  else if (e.key.toLowerCase() === 't') setTool('text');
});
function setTool(t) {
  const btn = document.querySelector(`.tool[data-tool="${t}"]`);
  if (btn) btn.click();
}

/* ============================== 网络层 ============================== */
let ws = null;
let connState = 'offline';
let reconnectAttempts = 0;
let everConnected = false;
let reconnectTimer = null;
let heartbeatTimer = null;
let watchdogTimer = null;
let lastMessageAt = 0;

function setStatus(s) {
  connState = s;
  const dot = $('statusDot');
  dot.classList.remove('online', 'connecting', 'offline');
  if (s === 'online') { dot.classList.add('online'); $('statusText').textContent = '已连接'; }
  else if (s === 'connecting') { dot.classList.add('connecting'); $('statusText').textContent = everConnected ? '重连中…' : '连接中…'; }
  else { dot.classList.add('offline'); $('statusText').textContent = '离线'; }
}
function updateStats() {
  $('seqText').textContent = `seq ${lastSeq}`;
  $('clockText').textContent = `lc ${clock.lamport}`;
}

function connect() {
  if (USE_V3) { connectV3(); return; }
  connectV2Legacy();
}

function connectV2Legacy() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
  setStatus('connecting');
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const url = `${proto}://${location.host}/ws`;
  let socket;
  try { socket = new WebSocket(url); } catch (err) { scheduleReconnect(); return; }
  ws = socket;

  socket.addEventListener('open', () => {
    sendMsg({ type: 'join', roomId, userId, lastSeq });
    startHeartbeat();
  });
  socket.addEventListener('message', (ev) => {
    lastMessageAt = Date.now();
    let msg;
    try { msg = JSON.parse(ev.data); } catch (_) { return; }
    switch (msg.type) {
      case 'joined':
        reconnectAttempts = 0; everConnected = true; setStatus('online');
        lastSeq = msg.lastSeq; updateStats();
        flushPending();
        break;
      case 'snapshot':
        loadSnapshot(msg);
        break;
      case 'ops':
        ingestRemoteEnvelopes(msg.envelopes || []);
        break;
      case 'ack':
        for (const id of msg.ids || []) pending.delete(id);
        lastSeq = Math.max(lastSeq, msg.lastSeq || 0);
        updateStats();
        break;
      case 'pong': break;
      case 'error':
        console.warn('[server error]', msg.message, msg.envId || '');
        break;
    }
  });
  socket.addEventListener('close', () => {
    stopTimers();
    if (ws === socket) ws = null;
    setStatus(navigator.onLine ? 'connecting' : 'offline');
    scheduleReconnect();
  });
  socket.addEventListener('error', () => { try { socket.close(); } catch (_) { /* noop */ } });
  lastMessageAt = Date.now();
  startWatchdog();
}

/* ------------------------- v3 二进制/P2P 同步栈 ------------------------- */

/**
 * v3 连接：WBSync.SyncClient 负责 HELLO 版本协商、JOIN(lastSeq+VC)、增量同步、
 * 服务端权威链路 + WebRTC mesh P2P 旁路、发送队列背压、慢客户端快照降级，
 * 以及 IndexedDB 离线持久化与断线合并重传；内部自带指数退避重连。
 */
async function connectV3() {
  setStatus('connecting');
  if (!sync3) {
    sync3 = new WBSync.SyncClient({
      userId, roomId,
      connectWS: () => new WebSocket(
        (location.protocol === 'https:' ? 'wss' : 'ws') + '://' + location.host + '/ws'),
      doc, buf, clock, history
    });
    bindSync3(sync3);
  }
  try { await sync3.start(); }
  catch (err) {
    console.warn('[sync v3] start failed, fallback to JSON v2', err);
    connectV2Fallback();
  }
}

function bindSync3(s) {
  // 状态：offline/hello/joining/online/cached（本地快照秒开）
  s.on('status', (st) => {
    everConnected = everConnected || st === 'online';
    if (st === 'online') { reconnectAttempts = 0; setStatus('online'); }
    else if (st === 'offline') setStatus(navigator.onLine ? 'connecting' : 'offline');
    else setStatus('connecting');
  });
  s.on('protocol', (p) => {
    if (p.rejected || p.degraded) {
      console.warn('[protocol]', p);
    }
  });
  // 远端信封经服务端/P2P 到达：SyncClient 已做 seq 校验 + id 幂等 + 因果重排 + apply，
  // 这里只需补撤销历史与刷新（doc 是共享实例，直接 bump）。
  s.on('ops', (ready) => {
    for (const e of ready || []) history.set(e.id, e);
    lastSeq = Math.max(lastSeq, s.lastSeq | 0);
    updateStats();
    if (ready && ready.length) bump();
  });
  // 全量/增量快照应用完成（SyncClient 已 loadSnapshot + 叠加 delta）
  s.on('snapshot', (payload, cached) => {
    lastSeq = Math.max(lastSeq, payload.lastSeq | 0);
    updateStats();
    bump();
  });
  s.on('ack', (msg) => {
    lastSeq = Math.max(lastSeq, msg.lastSeq | 0);
    updateStats();
  });
  // 慢客户端：进入/退出快照降级模式
  s.on('mode', (mode) => {
    $('statusText').textContent = mode === 'snapshot' ? '已连接·快照同步中' : '已连接';
  });
  // P2P peer 数量 / 背压（仅在状态栏轻量提示）
  s.on('peers', (peers) => {
    $('clockText').textContent = `lc ${clock.lamport} · p2p ${peers.length}`;
  });
  s.on('backpressure', (n, where) => {
    console.debug('[backpressure]', n, where || 'server');
  });
  s.on('flush', (n) => { console.log('[offline] merged', n, 'pending envelopes on reconnect'); });
  s.on('error', (err) => console.warn('[sync error]', err && err.message));
}

/** v3 不可用时的兜底：直接走 v2 JSON WebSocket */
function connectV2Fallback() {
  sync3 = null;
  connectV2Legacy();
}

/** 重连后把所有未确认信封重发（服务端按 env.id 幂等，不会重复入库） */
function flushPending() {
  if (!pending.size) return;
  const list = [...pending.values()].filter(Boolean);
  // 按 lamport 排序，保证因果顺序完整
  list.sort((a, b) => a.lamport - b.lamport || (a.id < b.id ? -1 : 1));
  // 快照重置可能清掉了“尚未入库服务端”的本地操作：本地重新物化一次（幂等）
  const missing = list.filter((env) => !doc.has(env.id));
  if (missing.length) ingestRemoteEnvelopes(missing);
  // 分批重发（每批最多 200 条），服务端按 env.id 幂等
  for (let i = 0; i < list.length; i += 200) {
    sendMsg({ type: 'ops', envelopes: list.slice(i, i + 200) });
  }
}

function scheduleReconnect() {
  if (!roomId || reconnectTimer) return;
  const base = Math.min(500 * Math.pow(2, reconnectAttempts), 10000);
  const delay = base + Math.random() * 300;
  reconnectAttempts += 1;
  reconnectTimer = setTimeout(() => { reconnectTimer = null; connect(); }, delay);
}
function startHeartbeat() {
  stopTimers();
  heartbeatTimer = setInterval(() => sendMsg({ type: 'ping', ts: Date.now() }), 20000);
}
function startWatchdog() {
  watchdogTimer = setInterval(() => {
    if (Date.now() - lastMessageAt > 40000 && ws) { try { ws.close(); } catch (_) { /* noop */ } }
  }, 10000);
}
function stopTimers() {
  if (heartbeatTimer) { clearInterval(heartbeatTimer); heartbeatTimer = null; }
  if (watchdogTimer) { clearInterval(watchdogTimer); watchdogTimer = null; }
}
window.addEventListener('online', () => {
  if (sync3) return; // v3 内部自动重连
  if (roomId && (!ws || ws.readyState !== WebSocket.OPEN)) {
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    reconnectAttempts = Math.max(reconnectAttempts - 1, 0);
    connect();
  }
});
window.addEventListener('offline', () => { if (!sync3) setStatus('offline'); });

/* ============================== 加入房间 ============================== */
$('joinForm').addEventListener('submit', (e) => {
  e.preventDefault();
  const rid = $('roomInput').value.trim();
  if (!rid) return;
  roomId = rid;
  $('joinScreen').classList.add('hidden');
  $('app').classList.remove('hidden');
  $('roomLabel').textContent = rid;
  $('userLabel').textContent = `${$('nameInput').value.trim() || '匿名'} · ${userId}`;
  requestAnimationFrame(() => { resizeCanvas(); connect(); });
});
$('roomInput').focus();

if (typeof ResizeObserver !== 'undefined') new ResizeObserver(resizeCanvas).observe(boardWrap);
window.addEventListener('resize', resizeCanvas);
window.addEventListener('orientationchange', () => setTimeout(resizeCanvas, 200));

refreshUndoUI();
