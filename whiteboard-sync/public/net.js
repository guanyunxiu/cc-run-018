'use strict';

/* ===========================================================================
 * 协作白板 v3 - 网络协议层（客户端 / 服务端共享，零依赖，UMD）
 *
 * 设计目标（与 v2 JSON/WebSocket 的本质区别）：
 *  - 二进制自定义 ArrayBuffer 协议：坐标 Float32、时间 Uint32、压感 Uint8、
 *    倾斜 Int8，笔迹点 19B/点（JSON 约 80B+），整包带类型标签，无 schema 依赖；
 *  - 协议版本化：HELLO/WELCOME 协商 major/minor，major 不一致拒绝、minor 高则降级；
 *  - Link：在任意“数据报传输”（WebSocket 二进制帧 / WebRTC DataChannel /
 *    内存虚拟传输）之上提供 分片 + 严格序号 + 累积 ACK + 选择性重传 + PING 心跳，
 *    天然解决 去重 / 乱序重排 / 重复丢弃 / 丢包重传 / 旧包不能覆盖新状态；
 *  - 发送/接收窗口背压：window 满或缓存字节超水位时阻塞并发出 backpressure/drain；
 *  - 快照二进制化：周期快照 + lastSeq/vector clock 增量判定；
 *  - 媒体层：分块 + offset 选择性请求，断线后从已收偏移续传、重复块丢弃；
 *  - OfflineStore：IndexedDB 持久化未确认信封与时钟（Node/测试用内存适配器）。
 *
 * WebSocket 只承担信令/控制（join、ACK 路径、RTC SDP/ICE 转发、媒体元数据）；
 * 大操作与媒体在浏览器中走 WebRTC DataChannel（见 mesh.js / sync.js）。
 * =========================================================================== */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.WBNet = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /* ============================== 协议常量 ============================== */

  const PROTO = { name: 'WB', major: 3, minor: 0 };

  /** 应用消息类型（Link 重组后交付） */
  const MT = {
    HELLO: 1,        // C→S 协议握手 {name,major,minor,session}
    WELCOME: 2,      // S→C {ok,action,major,minor,reason,session}
    JOIN: 3,         // C→S {roomId,userId,lastSeq,vc,relay}
    JOINED: 4,       // S→C {roomId,userId,lastSeq,host}
    OPS: 5,          // 双向 {envelopes:[...]}
    ACK: 6,          // S→C {ids:[...],lastSeq}
    SNAPSHOT: 7,     // S→C {hasSnapshot,lastSeq,watermark,snapshot,envelopes}
    REQ_SYNC: 8,     // C→S {lastSeq,vc} 慢客户端恢复 / 重连增量请求
    MODE: 9,         // S→C {mode:0 流式|1 快照降级}
    MEDIA: 10,       // 媒体元数据 {mediaId,oid,mime,totalBytes,chunkSize,nChunks,url}
    MEDIA_REQ: 11,   // 媒体拉取/续传 {mediaId,offset}
    MEDIA_DATA: 12,  // 媒体数据块 {mediaId,offset,last,bytes}
    RTC_SDP: 13,     // P2P 信令（WS 控制面）{from,to,sdp}
    RTC_ICE: 14,     // P2P 信令 {from,to,candidate}
    MEMBERS: 15,     // S→C {ids:[...],host}
    BYE: 16,
    ERROR: 17,       // {code,message}
    PING: 18,
    PONG: 19
  };

  const ERROR = {
    PROTO_MISMATCH: 1,   // 主版本不一致：拒绝
    PROTO_DEGRADED: 2,   // 次版本偏高：降级运行
    NOT_JOINED: 3,
    BAD_MESSAGE: 4,
    SLOW: 5,
    UNAUTHORIZED: 6
  };

  /** Link 帧类型（低 3 bit）；bit7 = FIN */
  const PKT = { DATA: 0, ACK: 1, NACK: 2, PING: 3, PONG: 4, RESET: 5, CTRL: 6 };
  const FIN_BIT = 0x80;

  const MAX_PKT = 16 * 1024;                  // 单帧目标尺寸
  const DATA_HDR = 12;                        // type1 + seq4 + fragId4 + fragSeq2 + msgType1
  const FRAG_PAYLOAD = MAX_PKT - DATA_HDR;    // ~16KB/帧，适配 DataChannel 16KiB 安全阈值

  const KIND = { create: 1, set: 2, delete: 3, restore: 4, group: 5, ungroup: 6, layer: 7, erase: 8 };
  const KIND_NAME = ['', 'create', 'set', 'delete', 'restore', 'group', 'ungroup', 'layer', 'erase'];

  /* --------------------------- 微型事件发射器 --------------------------- */
  class EE {
    constructor() { this._h = Object.create(null); }
    on(ev, fn) { (this._h[ev] = this._h[ev] || []).push(fn); return this; }
    off(ev, fn) {
      const a = this._h[ev];
      if (a) { const i = a.indexOf(fn); if (i >= 0) a.splice(i, 1); }
      return this;
    }
    removeAllListeners(ev) {
      if (ev) delete this._h[ev];
      else this._h = Object.create(null);
      return this;
    }
    listenerCount(ev) { return (this._h[ev] || []).length; }
    emit(ev) {
      const a = this._h[ev];
      if (!a) return;
      const args = Array.prototype.slice.call(arguments, 1);
      for (const fn of a.slice()) fn.apply(null, args);
    }
  }

  /* ============================== 二进制读写 ============================== */

  const te = new TextEncoder();
  const td = new TextDecoder();

  class BinW {
    constructor(size) {
      size = size || 256;
      this.buf = new Uint8Array(size);
      this.dv = new DataView(this.buf.buffer);
      this.pos = 0;
    }
    ensure(n) {
      if (this.pos + n <= this.buf.length) return;
      let len = this.buf.length || 128;
      while (len < this.pos + n) len = len * 2;
      const nu = new Uint8Array(len);
      nu.set(this.buf);
      this.buf = nu;
      this.dv = new DataView(nu.buffer);
    }
    u8(v) { this.ensure(1); this.dv.setUint8(this.pos, v); this.pos += 1; }
    u16(v) { this.ensure(2); this.dv.setUint16(this.pos, v, true); this.pos += 2; }
    u32(v) { this.ensure(4); this.dv.setUint32(this.pos, v >>> 0, true); this.pos += 4; }
    i32(v) { this.ensure(4); this.dv.setInt32(this.pos, v | 0, true); this.pos += 4; }
    f32(v) { this.ensure(4); this.dv.setFloat32(this.pos, v, true); this.pos += 4; }
    f64(v) { this.ensure(8); this.dv.setFloat64(this.pos, v, true); this.pos += 8; }
    i8(v) { this.ensure(1); this.dv.setInt8(this.pos, v | 0); this.pos += 1; }
    raw(u) { this.ensure(u.length); this.buf.set(u, this.pos); this.pos += u.length; }
    str(s) {
      s = s == null ? '' : String(s);
      const b = te.encode(s);
      this.u32(b.length);
      this.raw(b);
    }
    bytes(u) { this.u32(u.length); this.raw(u); }
    finish() { return this.buf.slice(0, this.pos); } // 复制为定长 ArrayBuffer 视图
  }

  class BinR {
    constructor(u8) {
      this.buf = u8 instanceof Uint8Array ? u8 : new Uint8Array(u8);
      this.dv = new DataView(this.buf.buffer, this.buf.byteOffset, this.buf.byteLength);
      this.pos = 0;
    }
    get remaining() { return this.buf.length - this.pos; }
    u8() { const v = this.dv.getUint8(this.pos); this.pos += 1; return v; }
    u16() { const v = this.dv.getUint16(this.pos, true); this.pos += 2; return v; }
    u32() { const v = this.dv.getUint32(this.pos, true); this.pos += 4; return v; }
    i32() { const v = this.dv.getInt32(this.pos, true); this.pos += 4; return v; }
    f32() { const v = this.dv.getFloat32(this.pos, true); this.pos += 4; return v; }
    f64() { const v = this.dv.getFloat64(this.pos, true); this.pos += 8; return v; }
    i8() { const v = this.dv.getInt8(this.pos); this.pos += 1; return v; }
    raw(n) { const v = this.buf.subarray(this.pos, this.pos + n); this.pos += n; return v; }
    str() {
      const n = this.u32();
      const v = td.decode(this.raw(n));
      return v;
    }
    bytes() {
      const n = this.u32();
      // 返回拷贝（脱离重组缓冲），媒体分块可直接持有
      return Array.from ? Array.from(this.raw(n)) : Array.prototype.slice.call(this.raw(n));
    }
  }

  /* ============================== 通用值编码 ============================== */

  // 标签化动态值：几何浮点一律 Float32（坐标/线宽/变换，画布量级误差 <1e-3），
  // 整数走 8/32 位整型，时间在调用点使用 Uint32。
  const TAG = { NULL: 0, TRUE: 1, FALSE: 2, U8: 3, U32: 4, I32: 5, F32: 6, F64: 7, STR: 8, ARR: 9, MAP: 10, STROKE: 11 };

  function isInt(v) { return Number.isInteger(v) && Math.abs(v) <= 0x7fffffff; }

  function writeVal(w, v) {
    if (v === null || v === undefined) { w.u8(TAG.NULL); return; }
    const t = typeof v;
    if (t === 'boolean') { w.u8(v ? TAG.TRUE : TAG.FALSE); return; }
    if (t === 'string') { w.u8(TAG.STR); w.str(v); return; }
    if (t === 'number') {
      if (v >= 0 && v <= 255 && Number.isInteger(v)) { w.u8(TAG.U8); w.u8(v); }
      else if (Number.isInteger(v) && v >= 0 && v <= 0xffffffff) { w.u8(TAG.U32); w.u32(v); }
      else if (isInt(v)) { w.u8(TAG.I32); w.i32(v); }
      else { w.u8(TAG.F32); w.f32(v); } // 坐标/宽度等几何量
      return;
    }
    if (Array.isArray(v)) {
      w.u8(TAG.ARR); w.u32(v.length);
      for (const item of v) writeVal(w, item);
      return;
    }
    if (t === 'object') {
      // 笔迹结构走 19B/点的紧凑布局
      if (v && Array.isArray(v.points)) { writeStroke(w, v); return; }
      const keys = Object.keys(v).filter((k) => v[k] !== undefined);
      w.u8(TAG.MAP); w.u16(keys.length);
      for (const k of keys) { w.str(k); writeVal(w, v[k]); }
      return;
    }
    w.u8(TAG.NULL);
  }

  function readVal(r) {
    const tag = r.u8();
    switch (tag) {
      case TAG.NULL: return null;
      case TAG.TRUE: return true;
      case TAG.FALSE: return false;
      case TAG.U8: return r.u8();
      case TAG.U32: return r.u32();
      case TAG.I32: return r.i32();
      case TAG.F32: return r.f32();
      case TAG.F64: return r.f64();
      case TAG.STR: return r.str();
      case TAG.ARR: {
        const n = r.u32();
        const out = new Array(n);
        for (let i = 0; i < n; i++) out[i] = readVal(r);
        return out;
      }
      case TAG.MAP: {
        const n = r.u16();
        const out = Object.create(null);
        for (let i = 0; i < n; i++) { const k = r.str(); out[k] = readVal(r); }
        return out;
      }
      case TAG.STROKE: return readStroke(r);
      default: throw new Error('bad value tag ' + tag);
    }
  }

  /**
   * 紧凑笔迹：
   *   brush/color/smooth 字符串，width/cellSize Float32，
   *   每点 19 字节：x/y/w 各 Float32，t Uint32，p Uint8（0..255），tx/ty Int8。
   * 压感量化误差 <0.004；时间为 Uint32（DOM timeStamp 在收笔时已做 32 位处理）。
   */
  function writeStroke(w, s) {
    w.u8(TAG.STROKE);
    w.str(s.brush || 'pen');
    w.str(s.color || '');
    w.f32(Number(s.width) || 0);
    w.str(s.smooth || 'catmull');
    w.f32(Number(s.cellSize || s.width) || 0);
    const pts = s.points || [];
    w.u32(pts.length);
    for (const p of pts) {
      w.f32(p.x);
      w.f32(p.y);
      w.f32(Number.isFinite(p.w) ? p.w : (Number(s.width) || 0));
      w.u32((p.t | 0) >>> 0);
      w.u8(Math.max(0, Math.min(255, Math.round((Number.isFinite(p.p) ? p.p : 0.5) * 255))));
      w.i8(Math.max(-127, Math.min(127, p.tx | 0)));
      w.i8(Math.max(-127, Math.min(127, p.ty | 0)));
    }
  }

  function readStroke(r) {
    const brush = r.str();
    const color = r.str();
    const width = r.f32();
    const smooth = r.str();
    const cellSize = r.f32();
    const n = r.u32();
    const points = new Array(n);
    for (let i = 0; i < n; i++) {
      const x = r.f32(), y = r.f32(), pw = r.f32();
      const t = r.u32();
      const p = r.u8() / 255;
      const tx = r.i8(), ty = r.i8();
      points[i] = { x, y, w: pw, t, p, tx, ty };
    }
    return { brush, color, width, smooth, cellSize, points };
  }

  /* ============================== 信封编解码 ============================== */

  function writeVC(w, vc) {
    const keys = Object.keys(vc || {});
    w.u16(keys.length);
    for (const k of keys) { w.str(k); w.u32(vc[k] | 0); }
  }
  function readVC(r) {
    const n = r.u16();
    const vc = Object.create(null);
    for (let i = 0; i < n; i++) vc[r.str()] = r.u32();
    return vc;
  }

  function writeInv(w, inv) {
    if (!inv) { w.u8(0); return; }
    w.u8(1);
    w.str(inv.originId || '');
    w.u32(inv.originLamport | 0);
    w.u8(inv.polarity | 0);
    w.u8(inv.wide ? 1 : 0);
  }
  function readInv(r) {
    if (!r.u8()) return null;
    const inv = { originId: r.str(), originLamport: r.u32(), polarity: r.u8(), wide: r.u8() === 1 };
    return inv;
  }

  function writeFields(w, fields) {
    const keys = Object.keys(fields || {});
    w.u16(keys.length);
    for (const k of keys) {
      w.str(k);
      // 字段级笔迹仍然走紧凑点布局
      const v = fields[k];
      if (k === 'stroke' && v && Array.isArray(v.points)) writeStroke(w, v);
      else writeVal(w, v);
    }
  }

  function readFields(r) {
    const n = r.u16();
    const out = Object.create(null);
    for (let i = 0; i < n; i++) {
      const k = r.str();
      out[k] = readVal(r);
    }
    return out;
  }

  function writeOp(w, op) {
    const kindId = KIND[op.kind];
    if (!kindId) throw new Error('bad op kind: ' + op.kind);
    w.u8(kindId);
    switch (op.kind) {
      case 'create': {
        const objs = op.objects || [];
        w.u16(objs.length);
        for (const o of objs) {
          w.str(o.oid); w.str(o.type || '');
          writeFields(w, o.fields || {});
        }
        break;
      }
      case 'set': {
        w.str(op.oid);
        writeFields(w, op.fields || {});
        writeFields(w, op.prev || {});
        break;
      }
      case 'delete':
      case 'restore': {
        const oids = op.oids || [];
        w.u16(oids.length);
        for (const id of oids) w.str(id);
        break;
      }
      case 'group':
      case 'ungroup': {
        w.str(op.gid);
        const oids = op.oids || [];
        w.u16(oids.length);
        for (const id of oids) w.str(id);
        break;
      }
      case 'layer': {
        w.str(op.oid);
        w.str(op.z || '');
        w.str(op.prevZ || '');
        break;
      }
      case 'erase': {
        w.u8(op.unerase ? 1 : 0);
        const chunks = op.chunks || [];
        w.u16(chunks.length);
        for (const ch of chunks) {
          w.str(ch.oid);
          w.i32(ch.tx | 0);
          w.i32(ch.ty | 0);
          const cells = ch.cells || [];
          w.u16(cells.length);
          for (const c of cells) { w.u8(c[0] & 255); w.u8(c[1] & 255); }
        }
        break;
      }
    }
    writeInv(w, op.inv);
  }

  function readOp(r) {
    const kindId = r.u8();
    const kind = KIND_NAME[kindId];
    if (!kind) throw new Error('bad op kind id ' + kindId);
    const op = { kind };
    switch (kind) {
      case 'create': {
        const n = r.u16();
        op.objects = new Array(n);
        for (let i = 0; i < n; i++) {
          op.objects[i] = { oid: r.str(), type: r.str(), fields: readFields(r) };
        }
        break;
      }
      case 'set': {
        op.oid = r.str();
        op.fields = readFields(r);
        op.prev = readFields(r);
        break;
      }
      case 'delete':
      case 'restore': {
        const n = r.u16();
        op.oids = new Array(n);
        for (let i = 0; i < n; i++) op.oids[i] = r.str();
        break;
      }
      case 'group':
      case 'ungroup': {
        op.gid = r.str();
        const n = r.u16();
        op.oids = new Array(n);
        for (let i = 0; i < n; i++) op.oids[i] = r.str();
        break;
      }
      case 'layer': {
        op.oid = r.str();
        op.z = r.str();
        op.prevZ = r.str();
        break;
      }
      case 'erase': {
        op.unerase = r.u8() === 1;
        const cn = r.u16();
        op.chunks = new Array(cn);
        for (let i = 0; i < cn; i++) {
          const oid = r.str(), tx = r.i32(), ty = r.i32();
          const cellsN = r.u16();
          const cells = new Array(cellsN);
          for (let j = 0; j < cellsN; j++) cells[j] = [r.u8(), r.u8()];
          op.chunks[i] = { oid, tx, ty, cells };
        }
        break;
      }
    }
    const inv = readInv(r);
    if (inv) op.inv = inv;
    return op;
  }

  function writeEnvelope(w, env) {
    w.str(env.id);
    w.str(env.clientId);
    w.u32(env.lamport | 0);
    writeVC(w, env.clock);
    w.str(env.txnId || '');
    w.str(env.squashKey || '');
    writeOp(w, env.op);
    w.u32(env.seq | 0); // 服务端排序序号（观测/重连用，无则 0）
  }

  function readEnvelope(r, withSeq) {
    const env = {
      id: r.str(),
      clientId: r.str(),
      lamport: r.u32(),
      clock: readVC(r),
      txnId: r.str() || null,
      squashKey: r.str() || null,
      op: readOp(r)
    };
    if (env.txnId === '') env.txnId = null;
    if (env.squashKey === '') env.squashKey = null;
    if (withSeq) env.seq = r.u32();
    return env;
  }

  /* ============================== 快照编解码 ============================== */

  function writeSnapshotPayload(w, data) {
    // 全量快照 or 增量（hasSnapshot=0 时只携带 envelopes）
    w.u8(data.hasSnapshot ? 1 : 0);
    w.u32(data.lastSeq | 0);
    w.u32(data.watermark | 0);
    if (data.hasSnapshot) {
      const snap = data.snapshot || {};
      w.u8(snap.version || 2);
      writeVC(w, snap.known);

      const groups = snap.groups || [];
      w.u16(groups.length);
      for (const g of groups) {
        w.str(g.gid); w.str(g.id); w.u32(g.lamport | 0); w.str(g.clientId);
        w.u16((g.members || []).length);
        for (const m of g.members || []) w.str(m);
      }

      const objects = snap.objects || [];
      w.u32(objects.length);
      for (const so of objects) {
        w.str(so.oid);
        const regKeys = Object.keys(so.regs || {});
        w.u16(regKeys.length);
        for (const f of regKeys) {
          const rec = so.regs[f];
          w.str(f);
          w.str(rec.id);
          writeVal(w, rec.value);
          w.u32(rec.lamport | 0);
          w.str(rec.clientId);
          writeInv(w, rec.inv);
          w.str(rec.squashKey || '');
        }
        const tileKeys = Object.keys(so.erases || {});
        w.u16(tileKeys.length);
        for (const tk of tileKeys) {
          w.str(tk);
          const cells = so.erases[tk];
          w.u16(cells.length);
          for (const e of cells) {
            w.u8(e.cell[0] & 255); w.u8(e.cell[1] & 255);
            w.str(e.id); w.u32(e.lamport | 0); w.str(e.clientId);
            w.u8(e.unerase ? 1 : 0);
            writeInv(w, e.inv);
          }
        }
      }
    }
    const envs = data.envelopes || [];
    w.u32(envs.length);
    for (const e of envs) writeEnvelope(w, e);
  }

  function readSnapshotPayload(r) {
    const out = { hasSnapshot: r.u8() === 1, lastSeq: r.u32(), watermark: r.u32() };
    if (out.hasSnapshot) {
      const snap = { version: r.u8(), known: readVC(r), groups: [], objects: [] };
      const gn = r.u16();
      for (let i = 0; i < gn; i++) {
        const g = { gid: r.str(), id: r.str(), lamport: r.u32(), clientId: r.str(), members: [] };
        const mn = r.u16();
        for (let j = 0; j < mn; j++) g.members.push(r.str());
        snap.groups.push(g);
      }
      const on = r.u32();
      snap.objects = new Array(on);
      for (let i = 0; i < on; i++) {
        const so = { oid: r.str(), regs: {}, erases: {} };
        const rn = r.u16();
        for (let j = 0; j < rn; j++) {
          const f = r.str();
          so.regs[f] = {
            id: r.str(), value: readVal(r), lamport: r.u32(),
            clientId: r.str(), inv: readInv(r), squashKey: r.str() || null
          };
        }
        const tn = r.u16();
        for (let j = 0; j < tn; j++) {
          const tk = r.str();
          const cn = r.u16();
          const cells = new Array(cn);
          for (let k = 0; k < cn; k++) {
            cells[k] = {
              cell: [r.u8(), r.u8()], id: r.str(), lamport: r.u32(),
              clientId: r.str(), unerase: r.u8() === 1, inv: readInv(r)
            };
          }
          so.erases[tk] = cells;
        }
        snap.objects[i] = so;
      }
      out.snapshot = snap;
    }
    const en = r.u32();
    out.envelopes = new Array(en);
    for (let i = 0; i < en; i++) out.envelopes[i] = readEnvelope(r, true);
    return out;
  }

  /* ============================== 消息编解码 ============================== */

  function encodeMessage(type, msg) {
    msg = msg || {};
    const w = new BinW(512);
    switch (type) {
      case MT.HELLO:
        w.str(PROTO.name); w.u8(msg.major | 0); w.u8(msg.minor | 0); w.u32(msg.session | 0);
        w.u16(msg.capabilities || 0);
        break;
      case MT.WELCOME:
        w.u8(msg.ok ? 1 : 0); w.u8(msg.action | 0); w.u8(msg.major | 0); w.u8(msg.minor | 0);
        w.str(msg.reason || ''); w.u32(msg.session | 0);
        break;
      case MT.JOIN:
        w.str(msg.roomId || ''); w.str(msg.userId || '');
        w.u32(msg.lastSeq | 0); writeVC(w, msg.vc);
        w.u8(msg.relay ? 1 : 0);
        break;
      case MT.JOINED:
        w.str(msg.roomId || ''); w.str(msg.userId || ''); w.u32(msg.lastSeq | 0); w.str(msg.host || '');
        break;
      case MT.OPS: {
        const list = msg.envelopes || [];
        w.u32(list.length);
        for (const e of list) writeEnvelope(w, e);
        break;
      }
      case MT.ACK:
        w.u32(msg.lastSeq | 0);
        w.u16((msg.ids || []).length);
        for (const id of msg.ids || []) w.str(id);
        break;
      case MT.SNAPSHOT:
        writeSnapshotPayload(w, msg);
        break;
      case MT.REQ_SYNC:
        w.u32(msg.lastSeq | 0); writeVC(w, msg.vc);
        break;
      case MT.MODE:
        w.u8(msg.mode | 0);
        break;
      case MT.MEDIA:
        w.str(msg.mediaId || ''); w.str(msg.oid || ''); w.str(msg.mime || 'image/jpeg');
        w.u32(msg.totalBytes | 0); w.u32(msg.chunkSize | 0); w.u32(msg.nChunks | 0);
        w.str(msg.url || ''); w.str(msg.from || '');
        break;
      case MT.MEDIA_REQ:
        w.str(msg.mediaId || ''); w.u32(msg.offset | 0);
        break;
      case MT.MEDIA_DATA:
        w.str(msg.mediaId || ''); w.u32(msg.offset | 0); w.u8(msg.last ? 1 : 0);
        w.bytes(msg.bytes || new Uint8Array(0));
        break;
      case MT.RTC_SDP:
        w.str(msg.from || ''); w.str(msg.to || ''); w.str(msg.sdp || '');
        break;
      case MT.RTC_ICE:
        w.str(msg.from || ''); w.str(msg.to || ''); w.str(msg.candidate || '');
        break;
      case MT.MEMBERS:
        w.u16((msg.ids || []).length);
        for (const id of msg.ids || []) w.str(id);
        w.str(msg.host || '');
        break;
      case MT.ERROR:
        w.u8(msg.code | 0); w.str(msg.message || '');
        break;
      case MT.PING:
      case MT.PONG:
        w.u32(msg.ts | 0);
        break;
      case MT.BYE:
        w.str(msg.reason || '');
        break;
      default:
        throw new Error('unknown message type ' + type);
    }
    return w.finish();
  }

  function decodeMessage(type, u8) {
    const r = new BinR(u8);
    let msg;
    switch (type) {
      case MT.HELLO:
        msg = { name: r.str(), major: r.u8(), minor: r.u8(), session: r.u32(), capabilities: r.u16() };
        break;
      case MT.WELCOME:
        msg = { ok: r.u8() === 1, action: r.u8(), major: r.u8(), minor: r.u8(), reason: r.str(), session: r.u32() };
        break;
      case MT.JOIN:
        msg = { roomId: r.str(), userId: r.str(), lastSeq: r.u32(), vc: readVC(r), relay: r.u8() === 1 };
        break;
      case MT.JOINED:
        msg = { roomId: r.str(), userId: r.str(), lastSeq: r.u32(), host: r.str() };
        break;
      case MT.OPS: {
        const n = r.u32();
        msg = { envelopes: new Array(n) };
        for (let i = 0; i < n; i++) msg.envelopes[i] = readEnvelope(r, true);
        break;
      }
      case MT.ACK: {
        const lastSeq = r.u32();
        const n = r.u16();
        const ids = new Array(n);
        for (let i = 0; i < n; i++) ids[i] = r.str();
        msg = { lastSeq, ids };
        break;
      }
      case MT.SNAPSHOT:
        msg = readSnapshotPayload(r);
        break;
      case MT.REQ_SYNC:
        msg = { lastSeq: r.u32(), vc: readVC(r) };
        break;
      case MT.MODE:
        msg = { mode: r.u8() };
        break;
      case MT.MEDIA:
        msg = { mediaId: r.str(), oid: r.str(), mime: r.str(), totalBytes: r.u32(),
          chunkSize: r.u32(), nChunks: r.u32(), url: r.str(), from: r.str() };
        break;
      case MT.MEDIA_REQ:
        msg = { mediaId: r.str(), offset: r.u32() };
        break;
      case MT.MEDIA_DATA: {
        const mediaId = r.str(), offset = r.u32(), last = r.u8() === 1;
        msg = { mediaId, offset, last, bytes: Uint8Array.from(r.raw(r.u32())) };
        break;
      }
      case MT.RTC_SDP:
        msg = { from: r.str(), to: r.str(), sdp: r.str() };
        break;
      case MT.RTC_ICE:
        msg = { from: r.str(), to: r.str(), candidate: r.str() };
        break;
      case MT.MEMBERS: {
        const n = r.u16();
        const ids = new Array(n);
        for (let i = 0; i < n; i++) ids[i] = r.str();
        msg = { ids, host: r.str() };
        break;
      }
      case MT.ERROR:
        msg = { code: r.u8(), message: r.str() };
        break;
      case MT.PING:
      case MT.PONG:
        msg = { ts: r.u32() };
        break;
      case MT.BYE:
        msg = { reason: r.str() };
        break;
      default:
        throw new Error('unknown message type ' + type);
    }
    if (r.remaining > 0) throw new Error('trailing bytes in message type ' + type + ' (' + r.remaining + ')');
    return msg;
  }

  /* ============================== 传输抽象 ============================== */

  /**
   * 流式数据报传输基类（WebSocket / Node ws 通用）。
   * 注入 sendRaw(Uint8Array) 与事件钩子即可；协议层只认 Uint8Array 帧。
   */
  class StreamTransport extends EE {
    constructor(hooks) {
      super();
      this._send = hooks.send;
      this._closed = false;
      this.bufferedAmount = 0;
      if (hooks.attach) hooks.attach(this);
    }
    /** 底层收到二进制帧时调用 */
    feed(u8) {
      if (!(u8 instanceof Uint8Array)) u8 = new Uint8Array(u8);
      this.emit('packet', u8);
    }
    setBuffered(n) { this.bufferedAmount = n; this.emit('buffered', n); }
    noteDrain() { this.bufferedAmount = 0; this.emit('drain'); }
    send(u8) {
      if (this._closed) return false;
      try { return this._send(u8) !== false; } catch (_) { return false; }
    }
    close() {
      if (this._closed) return;
      this._closed = true;
      this.emit('close');
    }
  }

  /** 浏览器/Node ws 适配：构造后自行把 ws 的 message/drain/close 接上 */
  function wrapWS(ws) {
    const t = new StreamTransport({
      send(u8) {
        if (ws.bufferedAmount !== undefined) t.bufferedAmount = ws.bufferedAmount + u8.length;
        // 浏览器 WebSocket 接受 ArrayBuffer；Node ws 接受 Buffer，二者皆可
        ws.send(u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength));
        if (ws.bufferedAmount !== undefined) t.bufferedAmount = ws.bufferedAmount;
        return ws.bufferedAmount === undefined || ws.bufferedAmount < 16 * 1024;
      }
    });
    if (ws.binaryType !== undefined) { try { ws.binaryType = 'arraybuffer'; } catch (_) {} }
    return t;
  }

  /** 一对直连内存传输（测试/虚拟网络） */
  function memoryPair(opts) {
    opts = opts || {};
    const a = new StreamTransport({ send(u) { b.feed(u); return true; } });
    const b = new StreamTransport({ send(u) { a.feed(u); return true; } });
    a.bufferedAmount = b.bufferedAmount = 0;
    return [a, b];
  }

  /**
   * 损伤链路包装器：按概率丢包 / 重复 / 乱序 / 延时，用于在内存中验证
   * Link 与媒体层的 去重-重排-重传 能力。
   * 双向拦截：egress 走 inner.send；ingress 通过替换 inner.feed 实现，
   * 因此一条底层链路的两端各包一次即可同时损伤两个方向。
   */
  function lossy(inner, opts) {
    opts = opts || {};
    const drop = opts.drop || 0, dup = opts.dup || 0, reorder = opts.reorder || 0;
    const delay = opts.delay || 0;
    const rnd = opts.random || Math.random;
    const held = [];
    function deliver(u, target) {
      if (delay) setTimeout(() => target(u), typeof delay === 'function' ? delay() : delay);
      else target(u);
    }
    function damage(u8, outFn) {
      const roll = rnd();
      if (roll < drop) return;            // 静默丢弃
      const again = roll < drop + dup;
      if (reorder && rnd() < reorder) held.push({ pkt: u8, out: outFn, again });
      else {
        deliver(u8, outFn);
        if (again) setTimeout(() => deliver(u8.slice(), outFn), 5 + rnd() * 20);
      }
    }
    const out = new StreamTransport({
      send(u8) { damage(u8, (p) => inner.send(p)); return true; }
    });
    // 入站：截获底层 feed（对端 send 最终调用的就是它）
    inner.feed = (u8) => damage(u8, (p) => out.emit('packet', p));
    out._timer = setInterval(() => {
      if (!held.length) return;
      const i = Math.floor(rnd() * held.length);
      const h = held.splice(i, 1)[0];
      deliver(h.pkt, h.out);
      if (h.again) setTimeout(() => deliver(h.pkt.slice(), h.out), 1);
    }, opts.tick || 15);
    out.stop = () => clearInterval(out._timer);
    out.inner = inner;
    return out;
  }

  /* ============================== Link 可靠层 ============================== */

  function rand32() { return (Math.random() * 0xffffffff) >>> 0; }

  /**
   * 在数据报传输之上提供：
   *  - 消息自动分片（每帧 ≤16KB，适配 DataChannel）；
   *  - 每方向严格递增 32 位序号；接收端旧序号/重复序号一律丢弃，缺口缓冲并 NACK；
   *  - 累积 ACK + SACK 选择性确认；发送端 RTO 重传（含乱序重复副本去重）；
   *  - 滑窗背压：在途窗口满或待发字节超水位时 send 排队，drain 时继续；
   *  - PING/PONG 心跳与超时 RESET。
   *
   * 事件：message(type, msg|Uint8Array, rawType)、drain、backpressure、reset、pong
   */
  class Link extends EE {
    constructor(transport, opts) {
      super();
      opts = opts || {};
      this.t = transport;
      this.session = opts.session || rand32();
      this.window = opts.window || 64;             // 在途帧窗口
      this.highWater = opts.highWater || 2 * 1024 * 1024;
      this.rtoMin = opts.rto || 250;
      this.rtoMax = opts.rtoMax || 3000;
      this.maxRetries = opts.maxRetries || 10;
      this.pingInterval = opts.pingInterval || 15000;
      this.timeoutMs = opts.timeout || 45000;
      this.decode = opts.decode !== false;        // false 时 message 交付原始字节

      this.nextSeq = 1;
      this.sent = new Map();                      // seq -> {pkt,time,retries,size}
      this.inflightBytes = 0;
      this.queued = [];                           // 待发送分片 [{msgType,chunk,fragId,fragSeq,fin}]
      this.queuedBytes = 0;
      this.fragId = 1;

      this.rcvNext = 1;
      this.rcvBuf = new Map();                    // seq -> pkt（乱序到达）
      this.rcvRecent = new Map();                 // fragId -> 总片数（重组完成去重，LRU）
      this.rx = new Map();                        // fragId -> {map:Map,total}
      this.sack = new Set();
      this.ackScheduled = false;

      this.closed = false;
      this.lastRx = Date.now();
      this.rtoTimer = setInterval(() => this._tick(), opts.tickMs || 80);
      if (this.pingInterval > 0) {
        this.pingTimer = setInterval(() => this._ping(), this.pingInterval);
      }
      this._drainSent = false;

      transport.on('packet', (u) => { try { this.feed(u); } catch (e) { this.emit('error', e); } });
      transport.on('drain', () => this._pump());
      transport.on('close', () => this.close('transport closed'));
    }

    /* ------------------------------ 发送 ------------------------------ */

    /** 排队一条消息（自动分片）；返回是否已全部进入窗口（false=有背压） */
    send(type, msgOrBytes) {
      if (this.closed) throw new Error('link closed');
      const bytes = msgOrBytes instanceof Uint8Array ? msgOrBytes : encodeMessage(type, msgOrBytes);
      const fid = this.fragId = (this.fragId + 1) >>> 0 || 1;
      const n = Math.max(1, Math.ceil(bytes.length / FRAG_PAYLOAD));
      if (n > 0xffff) throw new Error('message too large');
      for (let i = 0; i < n; i++) {
        const chunk = bytes.subarray(i * FRAG_PAYLOAD, Math.min(bytes.length, (i + 1) * FRAG_PAYLOAD));
        const frag = { msgType: type, chunk, fragId: fid, fragSeq: i, fin: i === n - 1 };
        this.queued.push(frag);
        this.queuedBytes += chunk.length;
      }
      this._pump();
      return !this.blocked;
    }

    get blocked() {
      return this.sent.size >= this.window ||
        this.inflightBytes + this.queuedBytes > this.highWater;
    }
    get bufferedAmount() { return this.inflightBytes + this.queuedBytes; }

    /**
     * 优先控制帧：不进发送窗口、不占序号、不可靠（小而幂等，丢了由对端再次 REQ 触发）。
     * 用途：MODE 降级信号——满窗口背压时普通数据发不出去，控制信号必须即时到达。
     * 帧体：CTRL|type(1)|payload，payload 限定 ≤ 256 字节。
     */
    sendControl(type, msgOrBytes) {
      const bytes = msgOrBytes instanceof Uint8Array ? msgOrBytes : encodeMessage(type, msgOrBytes);
      if (bytes.length > 256) throw new Error('control frame too large');
      const w = new BinW(2 + bytes.length);
      w.u8(PKT.CTRL);
      w.u8(type & 255);
      w.raw(bytes);
      this.t.send(w.finish());
    }

    whenDrained() {
      if (!this.blocked && this.queued.length === 0) return Promise.resolve();
      return new Promise((res) => {
        const handler = () => { this.off('drain', handler); res(); };
        this.on('drain', handler);
      });
    }

    _pump() {
      let progressed = false;
      let socketFull = false;
      while (this.queued.length && this.sent.size < this.window) {
        const f = this.queued[0];
        const w = new BinW(DATA_HDR + f.chunk.length);
        w.u8((f.fin ? FIN_BIT : 0) | PKT.DATA);
        const seq = this.nextSeq++;
        w.u32(seq);
        w.u32(f.fragId);
        w.u16(f.fragSeq);
        w.u8(f.msgType & 255);
        w.raw(f.chunk);
        const pkt = w.finish();
        // 传输层报告背压（socket 缓冲满）：占回序号与消息，等 drain 后续发，
        // 不把它计入在途窗口，避免向停摆对端无限写入。
        const ok = this.t.send(pkt);
        if (!ok) { this.nextSeq = seq; socketFull = true; break; }
        this.queued.shift();
        this.queuedBytes -= f.chunk.length;
        this.sent.set(seq, { pkt, time: Date.now(), retries: 0, size: pkt.length });
        this.inflightBytes += pkt.length;
        progressed = true;
      }
      if (socketFull) {
        this.emit('backpressure', this.bufferedAmount);
        // 等传输层 drain 再续发（drain 由 StreamTransport 在 socket 可写时发出）
        if (!this._waitingDrain) {
          this._waitingDrain = true;
          const go = () => { this._waitingDrain = false; this.off('drain', go); this._pump(); };
          this.t.on('drain', go);
          // 兜底：即便没有 drain 事件，下个 tick 也重试（传输层可能不发 drain）
          setTimeout(() => { if (this._waitingDrain) { this._waitingDrain = false; this.off('drain', go); this._pump(); } }, 50);
        }
      }
      if (this.blocked) this.emit('backpressure', this.bufferedAmount);
      if (!this.blocked && this.queued.length === 0 && !socketFull) this.emit('drain');
      return progressed;
    }

    _tick() {
      if (this.closed) return;
      if (this.pingInterval > 0 && Date.now() - this.lastRx > this.timeoutMs) {
        this.close('timeout');
        return;
      }
      const now = Date.now();
      let retried = 0;
      for (const [seq, s] of this.sent) {
        const rto = Math.min(this.rtoMax, this.rtoMin * Math.pow(1.6, s.retries));
        if (now - s.time > rto) {
          // 传输层背压时跳过重传（socket 满），下个 RTO 周期再试，防止放大风暴
          const ok = this.t.send(s.pkt);
          if (!ok) continue;
          if (s.retries >= this.maxRetries) { this.emit('error', new Error('retry exhausted #' + seq)); this.close('retry'); return; }
          s.retries += 1;
          s.time = now;
          retried++;
        }
      }
      void retried;
    }

    _ping() {
      if (this.closed) return;
      const w = new BinW(8);
      w.u8(PKT.PING); w.u32(Date.now() >>> 0);
      this.t.send(w.finish());
    }

    /* ------------------------------ 接收 ------------------------------ */

    feed(u8) {
      if (this.closed || u8.length < 1) return;
      this.lastRx = Date.now();
      const r = new BinR(u8);
      const head = r.u8();
      const type = head & 7;
      const fin = (head & FIN_BIT) !== 0;

      if (type === PKT.PING) {
        const ts = r.u32();
        const w = new BinW(9);
        w.u8(PKT.PONG); w.u32(ts);
        this.t.send(w.finish());
        return;
      }
      if (type === PKT.PONG) { this.emit('pong', r.u32()); return; }
      if (type === PKT.RESET) { this.close('peer reset'); return; }

      // CTRL：优先控制消息（无序号、不入窗口），type 在第 2 字节
      if (type === PKT.CTRL) {
        if (u8.length < 2) return;
        const msgType = r.u8();
        const payload = u8.subarray(2);
        if (this.decode) {
          let msg;
          try { msg = decodeMessage(msgType, payload); }
          catch (e) { this.emit('error', e); return; }
          this.emit('message', msgType, msg, payload);
        } else {
          this.emit('message', msgType, payload, payload);
        }
        return;
      }

      if (type === PKT.ACK) {
        const base = r.u32();
        const n = r.u16();
        this._recvAck(base, r, n, false);
        return;
      }
      if (type === PKT.NACK) {
        const base = r.u32();
        const n = r.u16();
        // 立即重传列出的缺失序号
        for (let i = 0; i < n; i++) {
          const miss = r.u32();
          const s = this.sent.get(miss);
          if (s) { s.retries += 1; s.time = Date.now(); this.t.send(s.pkt); }
        }
        if (base > 0) this._recvAck(base, null, 0, true);
        return;
      }

      // DATA：head 之后第一个 u32 才是帧序号
      if (type !== PKT.DATA) return;
      const seq = r.u32();
      if (seq < this.rcvNext) {
        this.emit('duplicate', seq);            // 重复包丢弃（副本不覆盖任何状态）
        // 对端重传说明我们的 ACK 丢了：立即重发累积 ACK
        this._sendAck();
        return;
      }
      if (seq > this.rcvNext) {
        if (!this.rcvBuf.has(seq)) {
          this.rcvBuf.set(seq, u8);
          this._scheduleNack();
        } else {
          this._scheduleNack(); // 副本：重报缺口（我们的 NACK 可能丢了）
        }
        return;
      }
      // seq === rcvNext：交付并连锁释放乱序缓冲
      this._deliverData(u8, fin, r);
      let progressed = true;
      while (progressed) {
        progressed = false;
        const next = this.rcvBuf.get(this.rcvNext);
        if (next) {
          this.rcvBuf.delete(this.rcvNext);
          this.sack.delete(this.rcvNext);
          const rr = new BinR(next);
          const h = rr.u8();
          rr.u32(); // seq
          this._deliverData(next, (h & FIN_BIT) !== 0, rr);
          progressed = true;
        }
      }
      this._scheduleAck();
    }

    _deliverData(pkt, fin, r) {
      // r 的 pos 已在 seq 之后
      const fragId = r.u32();
      const fragSeq = r.u16();
      const msgType = r.u8();
      const payload = r.raw(pkt.length - r.pos);

      // 已完成重组的消息：重复整帧直接丢弃
      const doneTotal = this.rcvRecent.get(fragId);
      if (doneTotal !== undefined && fragSeq <= doneTotal) { this.emit('duplicate', fragId + ':' + fragSeq); return; }

      let asm = this.rx.get(fragId);
      if (!asm) { asm = { map: new Map(), total: null }; this.rx.set(fragId, asm); }
      if (!asm.map.has(fragSeq)) asm.map.set(fragSeq, { msgType, payload });
      if (fin) asm.total = fragSeq;

      if (asm.total === null) { this.rcvNext += 1; return; }
      // 需要 0..total 全部收齐
      for (let i = 0; i <= asm.total; i++) {
        if (!asm.map.has(i)) { this.rcvNext += 1; return; }
      }
      const total = asm.total;
      const parts = [];
      let type = msgType;
      let len = 0;
      for (let i = 0; i <= total; i++) {
        const part = asm.map.get(i);
        type = part.msgType;
        parts.push(part.payload);
        len += part.payload.length;
      }
      const full = new Uint8Array(len);
      let off = 0;
      for (const p of parts) { full.set(p, off); off += p.length; }
      this.rx.delete(fragId);
      // 记录最近完成的 fragId（防迟到重复帧），有界 LRU
      this.rcvRecent.set(fragId, total);
      if (this.rcvRecent.size > 64) {
        const firstKey = this.rcvRecent.keys().next().value;
        this.rcvRecent.delete(firstKey);
      }
      this.rcvNext += 1;
      if (this.decode) {
        let msg;
        try { msg = decodeMessage(type, full); }
        catch (e) { this.emit('error', e); return; }
        this.emit('message', type, msg, full);
      } else {
        this.emit('message', type, full, full);
      }
    }

    _recvAck(base, r, n, baseOnly) {
      for (const seq of Array.from(this.sent.keys())) {
        if (seq <= base) this._ackOne(seq);
      }
      if (!baseOnly) {
        for (let i = 0; i < n; i++) {
          const s = r.u32();
          this._ackOne(s);
        }
      }
      this._pump();
    }
    _ackOne(seq) {
      const s = this.sent.get(seq);
      if (!s) return;
      this.sent.delete(seq);
      this.inflightBytes -= s.size;
    }

    _scheduleAck() {
      if (this.ackScheduled) return;
      this.ackScheduled = true;
      setTimeout(() => {
        this.ackScheduled = false;
        this._sendAck();
      }, 20);
    }
    _sendAck() {
      const w = new BinW(9 + this.sack.size * 4);
      w.u8(PKT.ACK);
      w.u32(this.rcvNext - 1);
      const extra = Array.from(this.sack).sort((a, b) => a - b).slice(0, 1024);
      w.u16(extra.length);
      for (const s of extra) w.u32(s);
      this.t.send(w.finish());
    }
    _scheduleNack() {
      // 把当前乱序窗口里持有的序号加入 SACK，并立即请求缺口
      if (this._nackScheduled) return;
      this._nackScheduled = true;
      setTimeout(() => {
        this._nackScheduled = false;
        for (const seq of this.rcvBuf.keys()) this.sack.add(seq);
        const missing = [];
        let cursor = this.rcvNext;
        const held = Array.from(this.rcvBuf.keys()).sort((a, b) => a - b);
        for (const h of held) {
          while (cursor < h) { missing.push(cursor); cursor++; }
          cursor = h + 1;
        }
        if (!missing.length) return;
        const w = new BinW(9 + missing.length * 4);
        w.u8(PKT.NACK);
        w.u32(this.rcvNext - 1);
        w.u16(Math.min(missing.length, 1024));
        for (const m of missing.slice(0, 1024)) w.u32(m);
        this.t.send(w.finish());
      }, 10);
    }

    close(reason) {
      if (this.closed) return;
      this.closed = true;
      clearInterval(this.rtoTimer);
      if (this.pingTimer) clearInterval(this.pingTimer);
      const w = new BinW(6);
      w.u8(PKT.RESET); w.u32(0);
      try { this.t.send(w.finish()); } catch (_) { /* noop */ }
      this.emit('reset', reason || 'closed');
    }
  }

  /* ============================== 发送队列（客户端背压） ============================== */

  /**
   * 客户端发送队列：
   *  - OPS 消息在排队区按 squashKey 合并（同手势的中间帧只保留最终态）；
   *  - 链路窗口阻塞时按 Promise 等待 drain，缓存超水位发 backpressure；
   *  - 队列序号单调：提交的信封按 lamport 排序后编码，杜绝旧消息排在新消息后。
   */
  class SendQueue extends EE {
    constructor(link, opts) {
      super();
      this.link = link;
      this.opts = opts || {};
      this.highWater = this.opts.highWater || 1024 * 1024;
      this.pendingOps = [];                 // 待发信封
      this.flushing = false;
      this.closed = false;
      link.on('backpressure', (n) => this.emit('backpressure', n));
      link.on('drain', () => { if (!this.flushing) this._flush(); this.emit('drain'); });
      link.on('reset', () => { this.closed = true; this.emit('reset'); });
    }

    /** 合并一批信封到待发区（同 squashKey 保留 lamport 更大者） */
    submitOps(envelopes) {
      const byKey = new Map();
      for (const e of envelopes) if (e.squashKey) byKey.set(e.squashKey, e);
      if (byKey.size) {
        this.pendingOps = this.pendingOps.filter((e) => {
          const rep = byKey.get(e.squashKey);
          return !(rep && rep.id !== e.id);
        });
      }
      const known = new Set(this.pendingOps.map((e) => e.id));
      for (const e of envelopes) if (!known.has(e.id)) this.pendingOps.push(e);
      // 同一发送者：lamport 即本地序号，严格升序防止旧消息覆盖新状态
      this.pendingOps.sort((a, b) => (a.lamport - b.lamport) || (a.id < b.id ? -1 : 1));
      return this._flush();
    }

    pendingBytes() {
      return this.pendingOps.reduce((n, e) => n + estimateEnvBytes(e), 0) + this.link.bufferedAmount;
    }

    async _flush() {
      if (this.flushing || this.closed || !this.pendingOps.length) return;
      this.flushing = true;
      try {
        while (this.pendingOps.length) {
          if (this.link.blocked) {
            this.emit('backpressure', this.pendingBytes());
            await new Promise((res) => {
              const go = () => { this.link.off('drain', go); res(); };
              this.link.on('drain', go);
            });
            if (this.closed) return;
          }
          // 每批最多 256 条，避免单消息过大
          const batch = this.pendingOps.splice(0, Math.min(256, this.pendingOps.length));
          this.link.send(MT.OPS, { envelopes: batch });
          if (this.pendingBytes() > this.highWater) this.emit('backpressure', this.pendingBytes());
        }
      } finally {
        this.flushing = false;
      }
      this.emit('drain');
    }
  }

  function estimateEnvBytes(e) {
    let n = 60 + (e.id ? e.id.length : 0) + (e.clientId ? e.clientId.length : 0);
    const pts = e.op && e.op.objects && e.op.objects[0] &&
      e.op.objects[0].fields && e.op.objects[0].fields.stroke &&
      e.op.objects[0].fields.stroke.points;
    if (pts) n += pts.length * 19;
    return n;
  }

  /* ============================== 服务端出站队列（慢客户端） ============================== */

  /**
   * 每个客户端一条 Outbox：
   *  - 统计待广播字节（Link 在途 + 待发）；
   *  - 超过 highWater：进入 snapshot 模式（后续 OPS 不再逐帧堆积），由 onSlow 回调；
   *  - 客户端排空后发 REQ_SYNC，服务端补发一份快照恢复流式（MODE 0）。
   */
  class Outbox {
    constructor(link, opts) {
      this.link = link;
      this.highWater = (opts && opts.highWater) || 512 * 1024;
      this.mode = 'stream';
      this.skipped = 0;
      this._lastBytes = 0;
    }
    get bytes() { return this.link.bufferedAmount; }
    /**
     * 慢客户端两种形态都要识别：
     *  - 已降级快照模式；
     *  - Link 可靠层发送窗口被打满（对端 ACK 跟不上，在途帧占满 window）；
     *  - 在途+排队字节超过水位。
     */
    get slow() {
      return this.mode === 'snapshot' ||
        this.link.blocked ||
        this.bytes > this.highWater;
    }

    /** 广播一条消息；返回 true=已投递，false=已降级丢弃 */
    enqueue(type, msg) {
      if (this.mode === 'snapshot') { this.skipped++; return false; }
      this.link.send(type, msg);
      if (this.slow) {
        this.mode = 'snapshot';
        // MODE=1 走优先控制帧：满窗口背压时也能即时送达（丢失则客户端保持流式，
        // 下次广播仍会再判定，故可接受不可靠）
        try { this.link.sendControl(MT.MODE, { mode: 1 }); } catch (_) {}
        return false;
      }
      return true;
    }
    /**
     * 客户端 REQ_SYNC 排空后恢复流式。MODE=0 走可靠数据帧（与紧随的快照同通道，
     * 保证不丢，客户端一定能看到 1→0 的完整状态迁移）。
     */
    resume() {
      const was = this.mode;
      this.mode = 'stream';
      this.skipped = 0;
      if (was === 'snapshot') { try { this.link.send(MT.MODE, { mode: 0 }); } catch (_) {} }
    }
  }

  /* ============================== 增量同步判定 ============================== */

  function vcDominates(va, vb) {
    if (!vb) return true;
    for (const k of Object.keys(vb)) {
      if ((va[k] | 0) < (vb[k] | 0)) return false;
    }
    return true;
  }

  /**
   * 给定房间的快照链（按 seq 升序）、当前日志与客户端 lastSeq/VC，
   * 计算应发送的同步载荷：
   *  - lastSeq 落在日志保留范围内：只发 seq>lastSeq 的信封（纯 delta，按 seq 权威判定，
   *    VC 仅在快照基线场景做进一步裁剪，避免把已折叠信封误判为缺失）；
   *  - 否则选 seq<=lastSeq 的最近快照作为基线，发快照 + 其后信封；
   *  - 无可用基线时发当前全量快照 + 日志。
   */
  function planSync(opts) {
    const snapshots = opts.snapshots || [];      // [{seq,data}]
    const log = opts.log;                        // [{seq,...}]
    const currentSnapshot = opts.currentSnapshot;
    const currentVC = opts.currentVC || {};
    const lastSeq = opts.lastSeq | 0;
    const vc = opts.vc || {};
    const currentSeq = opts.currentSeq | 0;
    void currentVC;

    // 1) 纯增量：缺失信封仍全部在日志中（lastSeq 是权威水位，直接按 seq 取）
    const logBase = log.length ? log[0].seq : currentSeq;
    if (lastSeq >= logBase && lastSeq <= currentSeq) {
      const envelopes = log.filter((e) => e.seq > lastSeq);
      return {
        hasSnapshot: false,
        lastSeq: currentSeq,
        watermark: opts.watermark | 0,
        envelopes
      };
    }

    // 2) 基线快照 + 增量（VC 覆盖的信封可安全跳过）
    let base = null;
    for (let i = snapshots.length - 1; i >= 0; i--) {
      if (snapshots[i].seq <= lastSeq) { base = snapshots[i]; break; }
    }
    if (base) {
      const envelopes = log.filter((e) => e.seq > base.seq && !vcDominates(vc, e.clock));
      return { hasSnapshot: true, snapshot: base.data, lastSeq: currentSeq,
        watermark: base.seq, envelopes };
    }

    // 3) 全量：优先用快照链中最新基线（新客户端“先加载快照、再重放少量操作”）；
    //    尚无任何快照（房间操作数未达拍快照阈值）时退回当前物化全量 + 日志。
    if (snapshots.length) {
      const base = snapshots[snapshots.length - 1];
      const envelopes = log.filter((e) => e.seq > base.seq);
      return {
        hasSnapshot: true, snapshot: base.data, lastSeq: currentSeq,
        watermark: base.seq, envelopes
      };
    }
    const envelopes = log.filter((e) => !vcDominates(vc, e.clock));
    return {
      hasSnapshot: true, snapshot: currentSnapshot, lastSeq: currentSeq,
      watermark: opts.watermark | 0, envelopes
    };
  }

  /* ====================== 离线存储（IndexedDB / 内存） ====================== */

  /** 内存适配器（Node / 测试 / 隐私模式兜底） */
  function memoryStore() {
    const m = new Map();
    return {
      get(k) { return Promise.resolve(m.has(k) ? m.get(k) : null); },
      put(k, v) { m.set(k, v); return Promise.resolve(); },
      del(k) { m.delete(k); return Promise.resolve(); },
      all(prefix) {
        const out = [];
        for (const [k, v] of m) if (!prefix || k.startsWith(prefix)) out.push([k, v]);
        return Promise.resolve(out);
      },
      clear() { m.clear(); return Promise.resolve(); }
    };
  }

  /** IndexedDB 适配器（浏览器）；环境不支持时 Promise reject，由上层退回内存 */
  function idbStore(name) {
    name = name || 'wb3';
    const indexedDB = (typeof indexedDB !== 'undefined') ? indexedDB
      : (typeof window !== 'undefined' ? window.indexedDB : null);
    if (!indexedDB) return Promise.reject(new Error('no indexedDB'));
    function open() {
      return new Promise((res, rej) => {
        const req = indexedDB.open(name, 1);
        req.onupgradeneeded = () => req.result.createObjectStore('kv');
        req.onsuccess = () => res(req.result);
        req.onerror = () => rej(req.error);
      });
    }
    function tx(db, mode) { return db.transaction('kv', mode).objectStore('kv'); }
    let dbp = open();
    dbp.catch(() => {});
    return {
      async get(k) {
        const db = await dbp;
        return new Promise((res, rej) => {
          const r = tx(db, 'readonly').get(k);
          r.onsuccess = () => res(r.result === undefined ? null : r.result);
          r.onerror = () => rej(r.error);
        });
      },
      async put(k, v) {
        const db = await dbp;
        return new Promise((res, rej) => {
          const r = tx(db, 'readwrite').put(v, k);
          r.onsuccess = () => res(); r.onerror = () => rej(r.error);
        });
      },
      async del(k) {
        const db = await dbp;
        return new Promise((res, rej) => {
          const r = tx(db, 'readwrite').delete(k);
          r.onsuccess = () => res(); r.onerror = () => rej(r.error);
        });
      },
      async all(prefix) {
        const db = await dbp;
        return new Promise((res, rej) => {
          const r = tx(db, 'readonly').getAllKeys();
          r.onsuccess = () => {
            const keys = r.result.filter((k) => !prefix || String(k).startsWith(prefix));
            Promise.all(keys.map((k) => new Promise((a, b) => {
              const g = tx(db, 'readonly').get(k);
              g.onsuccess = () => a([String(k), g.result]); g.onerror = () => b(g.error);
            }))).then(res, rej);
          };
          r.onerror = () => rej(r.error);
        });
      },
      async clear() {
        const db = await dbp;
        return new Promise((res, rej) => {
          const r = tx(db, 'readwrite').clear();
          r.onsuccess = () => res(); r.onerror = () => rej(r.error);
        });
      }
    };
  }

  /**
   * 离线编辑仓库：
   *   op:<clientId>:<local>  未确认信封（断线/关页不丢，重连后合并上传）
   *   clock:<clientId>      Lamport/local/VC（跨刷新继续，不与历史冲突）
   *   snap:<roomId>         最近一次快照（秒开渲染）
   * 信封以结构化对象存储（IndexedDB structured clone）。
   */
  class OfflineStore extends EE {
    constructor(adapter, clientId) {
      super();
      this.a = adapter;
      this.clientId = clientId;
      this._clockSaveTimer = null;
    }
    static create(clientId) {
      return idbStore('wb3-' + clientId).then((a) => new OfflineStore(a, clientId))
        .catch(() => new OfflineStore(memoryStore(), clientId));
    }
    keyEnv(e) { return 'op:' + this.clientId + ':' + (e.clock[this.clientId] || e.lamport); }

    savePending(env) { return this.a.put(this.keyEnv(env), env); }
    savePendingBatch(envs) { return Promise.all(envs.map((e) => this.savePending(e))); }
    removePending(env) { return this.a.del(this.keyEnv(env)); }
    removePendingById(id) {
      return this.allPending().then((list) => {
        const hit = list.find((e) => e.id === id);
        if (hit) return this.removePending(hit);
      });
    }
    async allPending() {
      const rows = await this.a.all('op:' + this.clientId + ':');
      return rows.map(([, v]) => v)
        .sort((a, b) => (a.lamport - b.lamport) || (a.id < b.id ? -1 : 1));
    }
    saveClock(clock) {
      // 高频写合并（移动帧很多）
      if (this._clockSaveTimer) return;
      this._clockSaveTimer = setTimeout(() => {
        this._clockSaveTimer = null;
        this.a.put('clock:' + this.clientId, {
          lamport: clock.lamport, local: clock.local, vc: clock.vc
        }).catch(() => {});
      }, 500);
    }
    loadClock() { return this.a.get('clock:' + this.clientId); }
    saveSnapshot(roomId, snapMsg) { return this.a.put('snap:' + roomId, snapMsg); }
    getSnapshot(roomId) { return this.a.get('snap:' + roomId); }
  }

  /* ============================== 媒体层（分块/续传） ============================== */

  /**
   * 媒体接收端（接收端驱动，天然支持断点续传）：
   *  - 发出 MEDIA_REQ{offset}；offset 前的块视为已收（断线重连续传）；
   *  - 收到的块严格按 offset 拼接：迟到旧块/重复块丢弃，乱序块暂存，缺口触发重请求；
   *  - 收齐 last 块后 emit complete(Uint8Array)。
   * 可运行在任意消息通道（mesh DataChannel Link 或损伤虚拟链路）之上。
   */
  class MediaReceiver extends EE {
    constructor(sendFn, opts) {
      super();
      this.send = sendFn;
      this.chunkSize = (opts && opts.chunkSize) || 16 * 1024;
      this.items = new Map(); // mediaId -> {meta,next,buf:Map(offset->bytes),lastOff,timer}
      this._handlers = [];
    }

    /** 请求一个媒体（prefix 为本地已收前缀，用于断线续传） */
    request(meta, prefix) {
      const item = {
        meta,
        next: prefix ? prefix.length : 0,
        buf: new Map(),
        done: false,
        parts: prefix ? [Uint8Array.from(prefix)] : [],
        lastOff: null
      };
      this.items.set(meta.mediaId, item);
      this.send(MT.MEDIA_REQ, { mediaId: meta.mediaId, offset: item.next });
      this._arm(meta.mediaId);
      return meta.mediaId;
    }

    _arm(id) {
      const item = this.items.get(id);
      if (!item || item.done) return;
      clearTimeout(item.timer);
      item.timer = setTimeout(() => {
        // 超时未推进：从缺口重新请求
        const it = this.items.get(id);
        if (it && !it.done) {
          this.send(MT.MEDIA_REQ, { mediaId: id, offset: it.next });
          this._arm(id);
        }
      }, 1500);
    }

    /** 处理 MEDIA_DATA（由上层在 message 事件中喂入） */
    feed(msg) {
      const item = this.items.get(msg.mediaId);
      if (!item) return false;
      const bytes = msg.bytes;
      // 重复/旧块丢弃（offset 严格校验：只允许等于期望值或暂存未来块）
      if (msg.offset < item.next) { this.emit('duplicate', msg.mediaId, msg.offset); return true; }
      if (!item.buf.has(msg.offset)) item.buf.set(msg.offset, bytes);
      if (msg.last) item.lastOff = msg.offset;

      let advanced = false;
      // 严格顺序拼接：乱序块等缺口
      while (item.buf.has(item.next)) {
        const b = item.buf.get(item.next);
        item.buf.delete(item.next);
        item.parts.push(b);
        item.next += b.length;
        advanced = true;
      }
      if (advanced) this._arm(msg.mediaId);

      const total = item.meta.totalBytes;
      if ((item.lastOff !== null && item.next >= item.lastOff) ||
        (total && item.next >= total)) {
        item.done = true;
        clearTimeout(item.timer);
        let len = 0;
        for (const p of item.parts) len += p.length;
        const full = new Uint8Array(len);
        let off = 0;
        for (const p of item.parts) { full.set(p, off); off += p.length; }
        this.items.delete(msg.mediaId);
        this.emit('complete', msg.mediaId, full, item.meta);
      } else if (!advanced) {
        // 收到乱序未来块：立即请求缺口（选择性重传）
        this.send(MT.MEDIA_REQ, { mediaId: msg.mediaId, offset: item.next });
      }
      return true;
    }

    wants(id) { return this.items.has(id); }
  }

  /**
   * 媒体发送端：响应 MEDIA_REQ，从请求 offset 开始分块推送；
   * 同一 offset 的重复请求重发（幂等），通道背压时等待 drain。
   */
  class MediaSender extends EE {
    constructor(sendFn, mediaId, bytes, meta, opts) {
      super();
      this.send = sendFn;
      this.mediaId = mediaId;
      this.bytes = bytes;
      this.meta = Object.assign({ mediaId, totalBytes: bytes.length }, meta || {});
      this.chunkSize = (opts && opts.chunkSize) || 16 * 1024;
      this.busy = false;
    }

    feedRequest(msg) {
      if (msg.mediaId !== this.mediaId) return false;
      this._stream(msg.offset | 0).catch((e) => this.emit('error', e));
      return true;
    }

    async _stream(offset) {
      if (this.busy) return;
      this.busy = true;
      try {
        while (offset < this.bytes.length) {
          const end = Math.min(offset + this.chunkSize, this.bytes.length);
          const chunk = this.bytes.subarray(offset, end);
          this.send(MT.MEDIA_DATA, {
            mediaId: this.mediaId, offset, last: end === this.bytes.length,
            bytes: chunk
          });
          this.emit('progress', offset / this.bytes.length);
          offset = end;
          // 让出事件循环，配合背压
          await new Promise((r) => setTimeout(r, 0));
        }
      } finally {
        this.busy = false;
      }
    }
  }

  /* ============================== 版本协商 ============================== */

  /**
   * @returns {{ok:boolean,action?:string,reason?:string}}
   * action: 'ok' | 'reject'(major 不一致) | 'degrade'(客户端 minor 更新)
   */
  function negotiate(major, minor) {
    if (major !== PROTO.major) {
      return { ok: false, action: 'reject', reason: `protocol major ${major} != ${PROTO.major}` };
    }
    if (minor > PROTO.minor) return { ok: true, action: 'degrade' };
    return { ok: true, action: 'ok' };
  }

  /* ============================== 导出 ============================== */

  return {
    PROTO, MT, PKT, ERROR, KIND,
    FRAG_PAYLOAD, MAX_PKT,
    EE, BinW, BinR,
    writeVal, readVal,
    encodeMessage, decodeMessage,
    writeEnvelope, readEnvelope,
    encodeEnvelope(e) { const w = new BinW(256); writeEnvelope(w, e); return w.finish(); },
    decodeEnvelope(u8) { return readEnvelope(new BinR(u8), false); },
    writeSnapshotPayload, readSnapshotPayload,
    StreamTransport, wrapWS, memoryPair, lossy,
    Link, SendQueue, Outbox, planSync, vcDominates,
    OfflineStore, memoryStore, idbStore,
    MediaReceiver, MediaSender,
    negotiate,
    estimateEnvBytes
  };
});
