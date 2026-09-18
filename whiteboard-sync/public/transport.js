'use strict';

/* ===========================================================================
 * 协作白板 v3 - 传输层（客户端 / 服务端共享，零依赖）
 *
 * 职责：
 *  1. 二进制协议（自定义 ArrayBuffer 帧）：坐标 Float32、时间 Uint32、
 *     笔迹点云/橡皮分块紧凑打包，键名字典 + varint，带宽远低于 JSON。
 *  2. 协议版本化：magic + major/minor 握手，major 不一致拒绝，minor 向下协商。
 *  3. 可靠序列层（ReliableLink）：单调序号、ACK/SACK、NACK 缺口重传、
 *     乱序重排、重复包丢弃、旧消息（stale）丢弃 —— 跑在无序/不可靠的
 *     WebRTC DataChannel 之上也能精确一次投递。
 *  4. 大对象分块（Blob）+ 背压队列（CoalescingQueue / OutboundMeter）。
 *  5. 增量同步工具：按版本向量（VC）挑出缺失信封。
 *  6. VirtualNetwork：内存有损/乱序网络模拟器，Node 下无 WebRTC 也能
 *     对整套 P2P 可靠传输做确定性测试。
 *
 * 同文件 UMD：Node(require) / 浏览器(<script>) 均可加载。
 * ========================================================================= */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.WBT = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  /* ============================== 协议版本 ============================== */

  const PROTO = {
    NAME: 'wb3',
    MAJOR: 3,
    MINOR: 0,
    /** 能力位 */
    CAPS: { BIN: 1 << 0, DC: 1 << 1, SNAP3: 1 << 2, BLOB: 1 << 3 },
    /** WS close code：协议不一致 */
    CLOSE_INCOMPATIBLE: 4001
  };

  function protoString(major, minor) { return PROTO.NAME + '/' + major + '.' + minor; }

  /**
   * 版本协商：
   *  - 只认同名协议；major 必须等于服务端（不做跨大版本兼容）；
   *  - 客户端 minor 更新时降级到服务端 minor（旧服务器，新客户端）；
   *  - 返回 {ok, major, minor, reason}。
   */
  function negotiate(name, major, minor, serverMinor) {
    if (name && name !== PROTO.NAME) return { ok: false, reason: 'name' };
    if (major !== PROTO.MAJOR) return { ok: false, reason: 'major' };
    if (!Number.isInteger(minor) || minor < 0) return { ok: false, reason: 'minor' };
    return { ok: true, major: PROTO.MAJOR, minor: Math.min(minor, serverMinor == null ? PROTO.MINOR : serverMinor) };
  }

  /** 二进制帧前导魔数：'W','B', major */
  const MAGIC0 = 0x57, MAGIC1 = 0x42;

  /** 帧类型 */
  const T = {
    HELLO: 1, HELLO_ACK: 2,
    JOIN: 3, JOINED: 4,
    OPS: 5, ACK: 6,
    SNAPSHOT: 8, DELTA: 9,
    PING: 10, PONG: 11,
    SIGNAL: 12, PEERS: 13,
    BIG_ANNOUNCE: 14, BIG_REQ: 15, BIG_CHUNK: 16, BIG_ACK: 17,
    DEGRADE: 18, RESUME: 19, RATE: 20, ERROR: 21,
    PEER_HELLO: 22,
    DATA: 23, DATA_ACK: 24, DATA_NACK: 25
  };

  /** 大载荷走 DataChannel 的默认阈值（编码后字节）与分块大小 */
  const LARGE_BYTES = 8 * 1024;
  const CHUNK_BYTES = 16 * 1024;

  /* ============================== 字节读写器 ============================== */

  class Writer {
    constructor(cap) { this.buf = new Uint8Array(cap || 256); this.len = 0; }
    _ensure(n) {
      if (this.len + n <= this.buf.length) return;
      let size = this.buf.length * 2;
      while (size < this.len + n) size *= 2;
      const next = new Uint8Array(size);
      next.set(this.buf);
      this.buf = next;
    }
    u8(v) { this._ensure(1); this.buf[this.len++] = v & 0xff; return this; }
    u16(v) { this._ensure(2); this.buf[this.len++] = v & 0xff; this.buf[this.len++] = (v >> 8) & 0xff; return this; }
    u32(v) {
      this._ensure(4);
      this.buf[this.len++] = v & 0xff; this.buf[this.len++] = (v >>> 8) & 0xff;
      this.buf[this.len++] = (v >>> 16) & 0xff; this.buf[this.len++] = (v >>> 24) & 0xff;
      return this;
    }
    f32(v) { this._ensure(4); new DataView(this.buf.buffer).setFloat32(this.len, v, true); this.len += 4; return this; }
    f64(v) { this._ensure(8); new DataView(this.buf.buffer).setFloat64(this.len, v, true); this.len += 8; return this; }
    /** 无符号 LEB128 varint */
    varUint(v) {
      this._ensure(5);
      v = v >>> 0;
      while (v >= 0x80) { this.buf[this.len++] = (v & 0x7f) | 0x80; v >>>= 7; }
      this.buf[this.len++] = v;
      return this;
    }
    /** 有符号 zigzag varint */
    varInt(v) { return this.varUint(((v << 1) ^ (v >> 31)) >>> 0); }
    bytes(b) {
      b = b || new Uint8Array(0);
      this.varUint(b.length);
      this._ensure(b.length);
      this.buf.set(b, this.len);
      this.len += b.length;
      return this;
    }
    rawNoLen(b) { this._ensure(b.length); this.buf.set(b, this.len); this.len += b.length; return this; }
    str(s) {
      s = s == null ? '' : String(s);
      if (typeof TextEncoder !== 'undefined') return this.bytes(new TextEncoder().encode(s));
      return this.bytes(Buffer.from(s, 'utf8')); // Node 兜底
    }
    finish() { return this.buf.slice(0, this.len); }
  }

  class Reader {
    constructor(u8) {
      this.b = u8;
      this.i = 0;
      this.dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
    }
    get eof() { return this.i >= this.b.length; }
    u8() { const v = this.b[this.i++]; this._ok(); return v; }
    u16() { const v = this.b[this.i] | (this.b[this.i + 1] << 8); this.i += 2; this._ok(); return v; }
    u32() {
      const v = (this.b[this.i] | (this.b[this.i + 1] << 8) |
        (this.b[this.i + 2] << 16) | (this.b[this.i + 3] << 24)) >>> 0;
      this.i += 4; this._ok(); return v;
    }
    f32() { const v = this.dv.getFloat32(this.i, true); this.i += 4; this._ok(); return v; }
    f64() { const v = this.dv.getFloat64(this.i, true); this.i += 8; this._ok(); return v; }
    varUint() {
      let shift = 0, v = 0, byte;
      do {
        byte = this.b[this.i++];
        v |= (byte & 0x7f) << shift;
        shift += 7;
      } while (byte & 0x80);
      return v >>> 0;
    }
    varInt() { const z = this.varUint(); return (z >>> 1) ^ -(z & 1); }
    bytes() {
      const n = this.varUint();
      const v = this.b.subarray(this.i, this.i + n);
      this.i += n; this._ok();
      return v;
    }
    str() {
      const b = this.bytes();
      if (typeof TextDecoder !== 'undefined') return new TextDecoder().decode(b);
      return Buffer.from(b).toString('utf8');
    }
    _ok() { if (this.i > this.b.length) throw new Error('truncated binary frame'); }
  }

  /* ====================== 值编码（通用 + 专用紧凑形态） ====================== */

  const V = {
    NULL: 0, FALSE: 1, TRUE: 2, INT: 3, F32: 4, F64: 5,
    STR: 6, ARR: 7, OBJ: 8, BIN: 9, POINTS: 11, CHUNKS: 12
  };

  /** 对象键名字典：热键用 1 字节，未收录键退化为长度前缀字符串 */
  const KEY_DICT = [
    'kind', 'id', 'clientId', 'lamport', 'clock', 'txnId', 'squashKey', 'op',
    'oid', 'type', 'fields', 'objects', 'oids', 'gid', 'z', 'chunks',
    'cells', 'prev', 'x', 'y', 'w', 'h', 'p', 't',
    'tx', 'ty', 'stroke', 'color', 'width', 'brush', 'smooth', 'cellSize',
    'points', 'content', 'fontSize', 'bold', 'fill', 'rot', 'deleted', 'group',
    'src', 'tr', 'sx', 'sy', 'r', 'inv', 'originId', 'originLamport',
    'polarity', 'wide', 'unerase', 'known', 'version', 'groups', 'members',
    'regs', 'erases', 'value', 'erases', 'snapshot', 'envelopes', 'watermark',
    'lastSeq', 'blobs', 'refs', 'size', 'chunksTotal', 'hash', 'holders',
    'fromSeq', 'reason', 'supported', 'code', 'message', 'roomId', 'userId',
    'major', 'minor', 'caps', 'sessionId', 'peers', 'blobId', 'index', 'total',
    'fromChunk', 'contiguous', 'sacks', 'ids', 'fseq', 'next', 'seqs',
    'windowMs', 'bytes', 'msgs', 'serverTime', 'snapEvery', 'from', 'to',
    'payload', 'seq', 'flags', 'vc', 'opKind', 'summary'
  ];
  const KEY_ID = new Map();
  KEY_DICT.forEach((k, i) => { if (!KEY_ID.has(k)) KEY_ID.set(k, i + 1); });

  /** 非整数优先用 Float32（坐标/宽度/压感）：精度足够且省一半字节；偏差大时退 f64 */
  function prefersF32(v) {
    const f = Math.fround(v);
    if (f === v) return true;
    return Math.abs(f - v) <= 1e-6 * Math.max(1, Math.abs(v));
  }

  const POINT_KEYS = { x: 1, y: 1, p: 1, t: 1, w: 1, tx: 1, ty: 1 };
  function isPoint(o) {
    return o && typeof o === 'object' && !Array.isArray(o) &&
      Number.isFinite(o.x) && Number.isFinite(o.y) &&
      Object.keys(o).every((k) => POINT_KEYS[k]);
  }
  function isChunk(o) {
    return o && typeof o === 'object' && !Array.isArray(o) &&
      typeof o.oid === 'string' && Number.isInteger(o.tx) && Number.isInteger(o.ty) &&
      Array.isArray(o.cells);
  }

  function writeKey(w, k) {
    const id = KEY_ID.get(k);
    if (id) w.u8(id); else { w.u8(0); w.str(k); }
  }
  function readKey(r) {
    const id = r.u8();
    if (id) return KEY_DICT[id - 1] || ('k' + id);
    return r.str();
  }

  function writeVal(w, v) {
    if (v === null || v === undefined) { w.u8(V.NULL); return; }
    if (typeof v === 'boolean') { w.u8(v ? V.TRUE : V.FALSE); return; }
    if (typeof v === 'number') {
      if (Number.isInteger(v) && Math.abs(v) <= 0x7fffffff) { w.u8(V.INT); w.varInt(v); return; }
      if (prefersF32(v)) { w.u8(V.F32); w.f32(v); return; }
      w.u8(V.F64); w.f64(v); return;
    }
    if (typeof v === 'string') { w.u8(V.STR); w.str(v); return; }
    if (v instanceof Uint8Array) { w.u8(V.BIN); w.bytes(v); return; }
    if (Array.isArray(v)) {
      // 笔迹点云：属性位掩码 + 列式连续存储（Float32 坐标 / Uint32 时间）
      if (v.length && isPoint(v[0]) && v.every(isPoint)) { w.u8(V.POINTS); writePointsRaw(w, v); return; }
      // 橡皮分块：cells 的 cx/cy ∈ 0..15，两个坐标压进 1 字节
      if (v.length && isChunk(v[0]) && v.every(isChunk)) { w.u8(V.CHUNKS); writeChunksRaw(w, v); return; }
      w.u8(V.ARR);
      w.varUint(v.length);
      for (const item of v) writeVal(w, item);
      return;
    }
    w.u8(V.OBJ);
    const keys = Object.keys(v);
    w.varUint(keys.length);
    for (const k of keys) { writeKey(w, k); writeVal(w, v[k]); }
  }

  function readVal(r) {
    const tag = r.u8();
    switch (tag) {
      case V.NULL: return null;
      case V.FALSE: return false;
      case V.TRUE: return true;
      case V.INT: return r.varInt();
      case V.F32: return r.f32();
      case V.F64: return r.f64();
      case V.STR: return r.str();
      case V.BIN: return r.bytes();
      case V.POINTS: return readPoints(r);
      case V.CHUNKS: return readChunks(r);      case V.ARR: {
        const n = r.varUint(); const out = new Array(n);
        for (let i = 0; i < n; i++) out[i] = readVal(r);
        return out;
      }
      case V.OBJ: {
        const n = r.varUint(); const out = {};
        for (let i = 0; i < n; i++) { const k = readKey(r); out[k] = readVal(r); }
        return out;
      }
      default: throw new Error('bad value tag ' + tag);
    }
  }

  /** 点云打包（裸内容，不含值标签）：mask 位 1=x 2=y 4=p 8=t 16=w 32=tx 64=ty */
  const PMSK = { X: 1, Y: 2, P: 4, T: 8, W: 16, TX: 32, TY: 64 };
  function writePointsRaw(w, pts) {
    const pick = (name) => {
      for (let i = 0; i < pts.length; i++) if (Number.isFinite(pts[i][name])) return true;
      return false;
    };
    let mask = 0;
    if (pick('x')) mask |= PMSK.X;
    if (pick('y')) mask |= PMSK.Y;
    if (pick('p')) mask |= PMSK.P;
    if (pick('t')) mask |= PMSK.T;
    if (pick('w')) mask |= PMSK.W;
    if (pick('tx')) mask |= PMSK.TX;
    if (pick('ty')) mask |= PMSK.TY;
    w.varUint(pts.length);
    w.u8(mask);
    const f32 = (key, bit) => { if (mask & bit) for (const p of pts) w.f32(p[key]); };
    f32('x', PMSK.X); f32('y', PMSK.Y); f32('p', PMSK.P);
    if (mask & PMSK.T) for (const p of pts) w.u32(p.t | 0);
    f32('w', PMSK.W); f32('tx', PMSK.TX); f32('ty', PMSK.TY);
  }
  function readPoints(r) {
    const n = r.varUint(), mask = r.u8();
    const cols = {};
    if (mask & PMSK.X) cols.x = new Array(n);
    if (mask & PMSK.Y) cols.y = new Array(n);
    if (mask & PMSK.P) cols.p = new Array(n);
    if (mask & PMSK.T) cols.t = new Array(n);
    if (mask & PMSK.W) cols.w = new Array(n);
    if (mask & PMSK.TX) cols.tx = new Array(n);
    if (mask & PMSK.TY) cols.ty = new Array(n);
    const take = (key) => { for (let i = 0; i < n; i++) cols[key][i] = r.f32(); };
    if (cols.x) take('x');
    if (cols.y) take('y');
    if (cols.p) take('p');
    if (cols.t) for (let i = 0; i < n; i++) cols.t[i] = r.u32();
    if (cols.w) take('w');
    if (cols.tx) take('tx');
    if (cols.ty) take('ty');
    const out = new Array(n);
    for (let i = 0; i < n; i++) {
      const p = {};
      for (const k of Object.keys(cols)) p[k] = cols[k][i];
      out[i] = p;
    }
    return out;
  }

  /** 橡皮分块打包（裸内容，不含值标签）：每块 oid + zigzag(tx,ty) + 每单元 1 字节 (cx<<4|cy) */
  function writeChunksRaw(w, chunks) {
    w.varUint(chunks.length);
    for (const ch of chunks) {
      w.str(ch.oid);
      w.varInt(ch.tx | 0);
      w.varInt(ch.ty | 0);
      const cells = ch.cells || [];
      w.varUint(cells.length);
      for (const c of cells) w.u8(((c[0] & 15) << 4) | (c[1] & 15));
    }
  }
  function readChunks(r) {
    const n = r.varUint(); const out = new Array(n);
    for (let i = 0; i < n; i++) {
      const oid = r.str(), tx = r.varInt(), ty = r.varInt();
      const cn = r.varUint();
      const cells = new Array(cn);
      for (let j = 0; j < cn; j++) { const b = r.u8(); cells[j] = [b >> 4, b & 15]; }
      out[i] = { oid, tx, ty, cells };
    }
    return out;
  }

  /* ============================== 信封编解码 ============================== */

  const ENV_FLAG_TXN = 1, ENV_FLAG_SQUASH = 2, ENV_FLAG_INV = 4;
  /** op.kind → 1 字节标签（新增 kind 追加，不复用，保证向前兼容） */
  const KINDS = ['create', 'set', 'delete', 'restore', 'group', 'ungroup', 'layer', 'erase'];
  const KIND_ID = new Map(KINDS.map((k, i) => [k, i + 1]));

  function writeVC(w, vc) {
    const keys = Object.keys(vc || {});
    w.u16(keys.length);
    for (const k of keys) { w.str(k); w.u32(vc[k] | 0); }
  }
  function readVC(r) {
    const n = r.u16(); const vc = {};
    for (let i = 0; i < n; i++) vc[r.str()] = r.u32();
    return vc;
  }

  function writeInv(w, inv) {
    w.str(inv.originId || '');
    w.u32(inv.originLamport | 0);
    w.u8((inv.polarity | 0) & 1);
    w.u8(inv.wide ? 1 : 0);
  }
  function readInv(r) {
    const inv = { originId: r.str(), originLamport: r.u32(), polarity: r.u8() };
    if (r.u8()) inv.wide = true;
    return inv;
  }

  function writeOp(w, op) {
    const kid = KIND_ID.get(op.kind) || 0;
    w.u8(kid);
    switch (op.kind) {
      case 'create': {
        const objs = op.objects || [];
        w.u16(objs.length);
        for (const o of objs) {
          w.str(o.oid); w.str(o.type);
          writeVal(w, o.fields || {});
        }
        break;
      }
      case 'set':
        w.str(op.oid);
        writeVal(w, op.fields || {});
        writeVal(w, op.prev == null ? null : op.prev);
        break;
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
      case 'layer':
        w.str(op.oid); w.str(op.z == null ? '' : op.z);
        break;
      case 'erase':
        writeChunksRaw(w, op.chunks || []);
        w.u8(op.unerase ? 1 : 0);
        break;
      default:
        // 未知 kind（新版本引入）：整体用通用值形态携带，旧版本读到 tag=0 应忽略
        w.u8(0);
        writeVal(w, op);
        return;
    }
    if (op.inv) { w.u8(1); writeInv(w, op.inv); } else w.u8(0);
  }

  function readOp(r) {
    const kid = r.u8();
    const kind = KINDS[kid - 1] || null;
    let op;
    switch (kind) {
      case 'create': {
        const n = r.u16(); const objects = new Array(n);
        for (let i = 0; i < n; i++) objects[i] = { oid: r.str(), type: r.str(), fields: readVal(r) };
        op = { kind, objects };
        break;
      }
      case 'set': {
        const oid = r.str();
        const fields = readVal(r);
        const prev = readVal(r);
        op = { kind, oid, fields };
        if (prev) op.prev = prev;
        break;
      }
      case 'delete':
      case 'restore': {
        const n = r.u16(); const oids = new Array(n);
        for (let i = 0; i < n; i++) oids[i] = r.str();
        op = { kind, oids };
        break;
      }
      case 'group':
      case 'ungroup': {
        const gid = r.str(); const n = r.u16(); const oids = new Array(n);
        for (let i = 0; i < n; i++) oids[i] = r.str();
        op = { kind, gid, oids };
        break;
      }
      case 'layer': {
        const oid = r.str(), z = r.str();
        op = { kind, oid, z: z || null };
        break;
      }
      case 'erase': {
        const chunks = readChunks(r);
        const unerase = !!r.u8();
        op = { kind, chunks };
        if (unerase) op.unerase = true;
        break;
      }
      default:
        // 未知 kind：读取通用值（向前兼容跳过）
        r.u8(); // 内部 tag=0
        return readVal(r);
    }
    if (r.u8()) op.inv = readInv(r);
    return op;
  }

  function writeEnvelope(w, env) {
    let flags = 0;
    if (env.txnId) flags |= ENV_FLAG_TXN;
    if (env.squashKey) flags |= ENV_FLAG_SQUASH;
    if (env.op && env.op.inv) flags |= ENV_FLAG_INV;
    w.u8(flags);
    w.str(env.id);
    w.str(env.clientId);
    w.u32(env.lamport | 0);
    writeVC(w, env.clock);
    if (flags & ENV_FLAG_TXN) w.str(env.txnId);
    if (flags & ENV_FLAG_SQUASH) w.str(env.squashKey);
    writeOp(w, env.op || {});
  }

  function readEnvelope(r) {
    const flags = r.u8();
    const env = {
      id: r.str(),
      clientId: r.str(),
      lamport: r.u32(),
      clock: readVC(r)
    };
    if (flags & ENV_FLAG_TXN) env.txnId = r.str(); else env.txnId = null;
    if (flags & ENV_FLAG_SQUASH) env.squashKey = r.str(); else env.squashKey = null;
    env.op = readOp(r);
    return env;
  }

  function writeEnvelopes(w, list) {
    w.u16(list.length);
    for (const e of list) writeEnvelope(w, e);
  }
  function readEnvelopes(r) {
    const n = r.u16(); const out = new Array(n);
    for (let i = 0; i < n; i++) out[i] = readEnvelope(r);
    return out;
  }

  /* ============================== 帧编解码 ============================== */

  const FRAME_HAS_FSEQ = 1;

  function head(w, type, flags) { w.u8(MAGIC0); w.u8(MAGIC1); w.u8(PROTO.MAJOR); w.u8(type); w.u8(flags || 0); }

  function encode(msg) {
    const w = new Writer(512);
    switch (msg.type) {
      case 'hello':
        head(w, T.HELLO);
        w.u8(msg.major || PROTO.MAJOR); w.u8(msg.minor || PROTO.MINOR);
        w.u16(msg.caps || 0); w.str(msg.userId || '');
        break;
      case 'helloAck':
        head(w, T.HELLO_ACK);
        w.u8(msg.major); w.u8(msg.minor); w.u16(msg.caps || 0);
        w.u32(msg.sessionId | 0); w.u32(msg.serverTime | 0);
        break;
      case 'join':
        head(w, T.JOIN);
        w.str(msg.roomId || ''); w.str(msg.userId || '');
        w.u32(msg.lastSeq | 0); writeVC(w, msg.vc);
        break;
      case 'joined':
        head(w, T.JOINED);
        w.u8(msg.major); w.u8(msg.minor); w.u16(msg.caps || 0);
        w.u32(msg.sessionId | 0);
        w.str(msg.roomId || ''); w.str(msg.userId || '');
        w.u32(msg.lastSeq | 0); w.u16(msg.snapEvery | 0);
        writeVC(w, msg.vc);
        break;
      case 'ops':
        head(w, T.OPS, msg.fseq != null ? FRAME_HAS_FSEQ : 0);
        if (msg.fseq != null) w.u32(msg.fseq | 0);
        writeEnvelopes(w, msg.envelopes || []);
        break;
      case 'ack':
        head(w, T.ACK);
        w.u16((msg.ids || []).length);
        for (const id of (msg.ids || [])) w.str(id);
        w.u32(msg.lastSeq | 0);
        w.u32(msg.fseq | 0);
        w.u16((msg.sacks || []).length);
        for (const s of (msg.sacks || [])) w.u32(s | 0);
        break;
      case 'snapshot':
        head(w, T.SNAPSHOT, msg.fseq != null ? FRAME_HAS_FSEQ : 0);
        if (msg.fseq != null) w.u32(msg.fseq | 0);
        w.u32(msg.watermark | 0);
        w.u32(msg.lastSeq | 0);
        writeVal(w, msg.snapshot || null);
        writeEnvelopes(w, msg.envelopes || []);
        writeVal(w, msg.blobs || []);
        break;
      case 'delta':
        head(w, T.DELTA, msg.fseq != null ? FRAME_HAS_FSEQ : 0);
        if (msg.fseq != null) w.u32(msg.fseq | 0);
        w.u32(msg.fromSeq | 0);
        writeEnvelopes(w, msg.envelopes || []);
        break;
      case 'ping': head(w, T.PING); w.u32(msg.t | 0); break;
      case 'pong': head(w, T.PONG); w.u32(msg.t | 0); break;
      case 'signal':
        head(w, T.SIGNAL);
        w.str(msg.from || ''); w.str(msg.to || '');
        w.bytes(toU8(msg.payload));
        break;
      case 'peers':
        head(w, T.PEERS);
        w.u16((msg.peers || []).length);
        for (const p of (msg.peers || [])) w.str(p);
        break;
      case 'bigAnnounce':
        head(w, T.BIG_ANNOUNCE);
        writeVal(w, msg.refs || []);
        break;
      case 'bigReq':
        head(w, T.BIG_REQ);
        w.str(msg.blobId || ''); w.u32(msg.fromChunk | 0);
        break;
      case 'bigChunk':
        head(w, T.BIG_CHUNK);
        w.str(msg.blobId || '');
        w.u32(msg.index | 0); w.u32(msg.total | 0);
        w.bytes(toU8(msg.bytes));
        break;
      case 'bigAck':
        head(w, T.BIG_ACK);
        w.str(msg.blobId || ''); w.u32(msg.contiguous | 0);
        break;
      case 'degrade':
        head(w, T.DEGRADE);
        w.u8(msg.reason | 0); w.u32(msg.seq | 0);
        break;
      case 'resume':
        head(w, T.RESUME);
        w.u32(msg.lastSeq | 0); writeVC(w, msg.vc);
        break;
      case 'rate':
        head(w, T.RATE);
        w.u32(msg.windowMs | 0); w.u32(msg.bytes | 0); w.u32(msg.msgs | 0);
        break;
      case 'error':
        head(w, T.ERROR);
        w.str(msg.code || ''); w.str(msg.message || '');
        w.u16((msg.supported || []).length);
        for (const s of (msg.supported || [])) w.str(s);
        break;
      case 'peerHello':
        head(w, T.PEER_HELLO);
        w.u8(msg.major || PROTO.MAJOR); w.u8(msg.minor || PROTO.MINOR);
        w.u16(msg.caps || 0); w.str(msg.userId || '');
        writeVC(w, msg.vc);
        break;
      case 'data':
        head(w, T.DATA, msg.rexmit ? 1 : 0);
        w.u32(msg.seq | 0);
        w.bytes(toU8(msg.payload));
        break;
      case 'dataAck':
        head(w, T.DATA_ACK);
        w.u32(msg.next | 0);
        w.u16((msg.sacks || []).length);
        for (const s of (msg.sacks || [])) w.u32(s | 0);
        break;
      case 'dataNack':
        head(w, T.DATA_NACK);
        w.u16((msg.seqs || []).length);
        for (const s of (msg.seqs || [])) w.u32(s | 0);
        break;
      default:
        throw new Error('unknown frame type: ' + msg.type);
    }
    return w.finish();
  }

  function toU8(v) {
    if (v == null) return new Uint8Array(0);
    if (v instanceof Uint8Array) return v;
    if (v && v.buffer) return new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(v)) return new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
    if (v instanceof ArrayBuffer) return new Uint8Array(v);
    throw new Error('binary payload required');
  }

  /** 兼容 Node Buffer 与浏览器 ArrayBuffer */
  function frameFrom(raw) {
    const u8 = toU8(raw);
    if (u8.length < 5 || u8[0] !== MAGIC0 || u8[1] !== MAGIC1) {
      return { badMagic: true };
    }
    if (u8[2] !== PROTO.MAJOR) return { badVersion: true, major: u8[2] };
    const type = u8[3], flags = u8[4], r = new Reader(u8.subarray(5));
    const fseq = () => (flags & FRAME_HAS_FSEQ) ? r.u32() : null;
    switch (type) {
      case T.HELLO:
        return { type: 'hello', major: r.u8(), minor: r.u8(), caps: r.u16(), userId: r.str() };
      case T.HELLO_ACK:
        return { type: 'helloAck', major: r.u8(), minor: r.u8(), caps: r.u16(),
          sessionId: r.u32(), serverTime: r.u32() };
      case T.JOIN:
        return { type: 'join', roomId: r.str(), userId: r.str(), lastSeq: r.u32(), vc: readVC(r) };
      case T.JOINED:
        return { type: 'joined', major: r.u8(), minor: r.u8(), caps: r.u16(),
          sessionId: r.u32(), roomId: r.str(), userId: r.str(),
          lastSeq: r.u32(), snapEvery: r.u16(), vc: readVC(r) };
      case T.OPS:
        return { type: 'ops', fseq: fseq(), envelopes: readEnvelopes(r) };
      case T.ACK: {
        const n = r.u16(); const ids = new Array(n);
        for (let i = 0; i < n; i++) ids[i] = r.str();
        const lastSeq = r.u32(), f = r.u32();
        const sn = r.u16(); const sacks = new Array(sn);
        for (let i = 0; i < sn; i++) sacks[i] = r.u32();
        return { type: 'ack', ids, lastSeq, fseq: f, sacks };
      }
      case T.SNAPSHOT:
        return { type: 'snapshot', fseq: fseq(), watermark: r.u32(), lastSeq: r.u32(),
          snapshot: readVal(r), envelopes: readEnvelopes(r), blobs: readVal(r) };
      case T.DELTA:
        return { type: 'delta', fseq: fseq(), fromSeq: r.u32(), envelopes: readEnvelopes(r) };
      case T.PING: return { type: 'ping', t: r.u32() };
      case T.PONG: return { type: 'pong', t: r.u32() };
      case T.SIGNAL:
        return { type: 'signal', from: r.str(), to: r.str(), payload: r.bytes() };
      case T.PEERS: {
        const n = r.u16(); const peers = new Array(n);
        for (let i = 0; i < n; i++) peers[i] = r.str();
        return { type: 'peers', peers };
      }
      case T.BIG_ANNOUNCE:
        return { type: 'bigAnnounce', refs: readVal(r) };
      case T.BIG_REQ:
        return { type: 'bigReq', blobId: r.str(), fromChunk: r.u32() };
      case T.BIG_CHUNK:
        return { type: 'bigChunk', blobId: r.str(), index: r.u32(), total: r.u32(), bytes: r.bytes() };
      case T.BIG_ACK:
        return { type: 'bigAck', blobId: r.str(), contiguous: r.u32() };
      case T.DEGRADE:
        return { type: 'degrade', reason: r.u8(), seq: r.u32() };
      case T.RESUME:
        return { type: 'resume', lastSeq: r.u32(), vc: readVC(r) };
      case T.RATE:
        return { type: 'rate', windowMs: r.u32(), bytes: r.u32(), msgs: r.u32() };
      case T.ERROR: {
        const code = r.str(), message = r.str();
        const n = r.u16(); const supported = new Array(n);
        for (let i = 0; i < n; i++) supported[i] = r.str();
        return { type: 'error', code, message, supported };
      }
      case T.PEER_HELLO:
        return { type: 'peerHello', major: r.u8(), minor: r.u8(), caps: r.u16(),
          userId: r.str(), vc: readVC(r) };
      case T.DATA:
        return { type: 'data', rexmit: !!(flags & 1), seq: r.u32(), payload: r.bytes() };
      case T.DATA_ACK: {
        const next = r.u32();
        const n = r.u16(); const sacks = new Array(n);
        for (let i = 0; i < n; i++) sacks[i] = r.u32();
        return { type: 'dataAck', next, sacks };
      }
      case T.DATA_NACK: {
        const n = r.u16(); const seqs = new Array(n);
        for (let i = 0; i < n; i++) seqs[i] = r.u32();
        return { type: 'dataNack', seqs };
      }
      default:
        return { unknown: true, typeCode: type };
    }
  }

  /* ========================== 可靠序列层（ARQ） ========================== */

  /**
   * 在无序、可能丢包/重复的传输（WebRTC unordered DataChannel / VirtualNetwork）
   * 之上提供“有序、不丢、不重”的消息流：
   *  - 发送端：每条消息分配单调 seq，DATA 帧发出后等 DATA_ACK；
   *    RTO 超时选择性重传；SACK 直接确认乱序到达的消息。
   *  - 接收端：重复 seq 丢弃；seq < 已投递水位 → stale 丢弃；
   *    seq > expected 进入乱序窗口并回 DATA_NACK 索要缺口；
   *    连续就绪后按序冲刷，旧消息永远不可能覆盖新状态。
   */
  class ReliableLink {
    /**
     * @param {object} opts
     * @param {(bytes:Uint8Array)=>void} opts.send 实际底层发送
     * @param {()=>number} [opts.now]
     * @param {number} [opts.rto] 初始重传超时 ms
     * @param {number} [opts.maxPending] 发送窗口上限（背压）
     */
    constructor(opts) {
      this._send = opts.send;
      this._now = opts.now || (() => Date.now());
      this.rto = opts.rto || 200;
      this.maxPending = opts.maxPending || 1024;

      this.nextSeq = 1;          // 发送：下一个待分配 seq
      this.sent = new Map();    // seq -> {bytes, at, sends}
      this.lastAckAt = 0;

      this.deliveredSeq = 0;     // 接收：已连续投递水位
      this.got = new Set();     // 窗口内已收到（含乱序）的 seq
      this.reorder = new Map(); // seq -> payload
      this.maxSeqSeen = 0;

      this.stats = { sent: 0, rexmited: 0, dupDropped: 0, staleDropped: 0,
        reordered: 0, nacks: 0, lost: 0 };
    }

    /** 是否还能再发（背压窗口） */
    get available() { return this.sent.size < this.maxPending; }
    get pendingBytes() {
      let n = 0;
      for (const v of this.sent.values()) n += v.bytes.length;
      return n;
    }

    /** 发送一条上层消息（自动包 DATA 帧）。窗口满返回 false（调用方背压） */
    send(payload) {
      const bytes = toU8(payload);
      if (!this.available) return false;
      const seq = this.nextSeq++;
      this.sent.set(seq, { bytes, at: this._now(), sends: 0 });
      this.stats.sent++;
      this._xmit(seq, bytes, false);
      return true;
    }

    _xmit(seq, bytes, rexmit) {
      const f = encode({ type: 'data', seq, rexmit, payload: bytes });
      this._send(f);
      const rec = this.sent.get(seq);
      if (rec) { rec.at = this._now(); rec.sends += 1; }
      if (rexmit) this.stats.rexmited++;
    }

    /**
     * 处理一个底层收到的帧（已解出的 data/dataAck/dataNack）。
     * @returns {Array<Uint8Array>} 本次按序可投递的上层消息负载
     */
    handle(msg) {
      if (msg.type === 'dataAck') { this._markAck(msg.next, msg.sacks); return []; }
      if (msg.type === 'dataNack') {
        let n = 0;
        for (const seq of msg.seqs) {
          const rec = this.sent.get(seq);
          if (rec) { this._xmit(seq, rec.bytes, true); n++; }
        }
        this.stats.nacks += n;
        return [];
      }
      if (msg.type !== 'data') return [];
      const seq = msg.seq;
      if (seq <= this.deliveredSeq) { this.stats.dupDropped++; return []; } // 旧消息/重复
      if (this.got.has(seq)) { this.stats.dupDropped++; return []; }
      this.got.add(seq);
      this.reorder.set(seq, toU8(msg.payload));
      if (seq > this.maxSeqSeen) this.maxSeqSeen = seq; else this.stats.reordered++;

      const out = [];
      let expected = this.deliveredSeq + 1;
      while (this.reorder.has(expected)) {
        out.push(this.reorder.get(expected));
        this.reorder.delete(expected);
        this.got.delete(expected);
        this.deliveredSeq = expected;
        expected += 1;
      }
      // 滑动接收窗口集合（只保留水位以上的有限集合）
      if (this.got.size > 4096) {
        for (const s of this.got) if (s <= this.deliveredSeq) this.got.delete(s);
      }
      return out;
    }

    _markAck(next, sacks) {
      for (let seq = this._minSent(); seq < next; seq++) {
        if (this.sent.delete(seq)) this.lastAckAt = this._now();
      }
      for (const s of (sacks || [])) this.sent.delete(s);
    }
    _minSent() {
      let min = this.nextSeq;
      for (const s of this.sent.keys()) if (s < min) min = s;
      return min;
    }

    /** 接收端：当前缺口列表（供 DATA_NACK） */
    gaps(limit) {
      limit = limit || 64;
      const out = [];
      for (let s = this.deliveredSeq + 1; s < this.maxSeqSeen && out.length < limit; s++) {
        if (!this.got.has(s)) out.push(s);
      }
      return out;
    }

    /** 接收端：生成 ACK 帧（连续水位 + SACK 乱序集合），无新信息返回 null */
    buildAck() {
      if (this.deliveredSeq === 0 && this.got.size === 0) return null;
      const sacks = [];
      for (const s of this.got) {
        if (s > this.deliveredSeq) sacks.push(s);
        if (sacks.length >= 64) break;
      }
      return encode({ type: 'dataAck', next: this.deliveredSeq + 1, sacks });
    }

    /** 发送端定时维护：超时重传；接收端缺口 NACK。返回需要实际发出的帧列表 */
    tick(now) {
      const out = [];
      for (const [seq, rec] of this.sent) {
        const backoff = Math.min(this.rto * Math.pow(1.6, Math.min(rec.sends, 6)), 2000);
        if (now - rec.at >= backoff) {
          this._xmit(seq, rec.bytes, true);
          this.stats.lost++;
          out.push(seq);
        }
      }
      const gaps = this.gaps();
      if (gaps.length) {
        out.push(true); // 标记：调用方应发 NACK
        this._nackFrame = encode({ type: 'dataNack', seqs: gaps });
      }
      return out;
    }
    takeNack() { const f = this._nackFrame; this._nackFrame = null; return f || null; }
  }

  /* ============================== 大对象分块 ============================== */

  /** 把一个 blob 切成 BIG_CHUNK 帧（帧本身再交给 ReliableLink 保序） */
  function chunkBlob(blobId, data, chunkSize) {
    const bytes = toU8(data);
    const size = chunkSize || CHUNK_BYTES;
    const total = Math.max(1, Math.ceil(bytes.length / size));
    const frames = [];
    for (let i = 0; i < total; i++) {
      frames.push(encode({
        type: 'bigChunk', blobId, index: i, total,
        bytes: bytes.subarray(i * size, Math.min(bytes.length, (i + 1) * size))
      }));
    }
    return { total, frames };
  }

  /**
   * 块重组器：乱序收块、按连续区间完成；重复块丢弃。
   * 完成（或已收齐）时返回 {blobId,total,data}，否则 null。
   */
  class BlobAssembler {
    constructor() { this.jobs = new Map(); }
    add(msg) {
      let job = this.jobs.get(msg.blobId);
      if (!job) { job = { total: msg.total, chunks: new Map(), contiguous: 0 }; this.jobs.set(msg.blobId, job); }
      if (msg.index >= job.total) return null;
      if (job.chunks.has(msg.index)) return null; // 重复块丢弃
      job.chunks.set(msg.index, toU8(msg.bytes));
      while (job.chunks.has(job.contiguous)) job.contiguous++;
      if (job.contiguous < job.total) return null;
      let size = 0;
      for (const b of job.chunks.values()) size += b.length;
      const data = new Uint8Array(size);
      for (let i = 0; i < job.total; i++) {
        const b = job.chunks.get(i);
        data.set(b, i === 0 ? 0 : jobOffset(job, i));
      }
      this.jobs.delete(msg.blobId);
      return { blobId: msg.blobId, total: job.total, data };
    }
  }
  function jobOffset(job, index) {
    let off = 0;
    for (let i = 0; i < index; i++) off += job.chunks.get(i).length;
    return off;
  }

  /** 简单内容存储（blobId -> Uint8Array），浏览器端可由 IndexedDB 支撑 */
  class BlobStore {
    constructor() { this.map = new Map(); this.meta = new Map(); }
    put(id, data, meta) { this.map.set(id, toU8(data)); if (meta) this.meta.set(id, meta); return id; }
    has(id) { return this.map.has(id); }
    get(id) { return this.map.get(id); }
    size(id) { const v = this.map.get(id); return v ? v.length : 0; }
    delete(id) { this.map.delete(id); this.meta.delete(id); }
  }

  /* ============================ 增量同步（VC） ============================ */

  function mergeVC(vc, other) {
    vc = vc || {};
    for (const k of Object.keys(other || {})) {
      const v = other[k] | 0;
      if (v > (vc[k] | 0)) vc[k] = v;
    }
    return vc;
  }

  function vcDominates(va, vb) {
    for (const k of Object.keys(vb || {})) {
      if ((va[k] | 0) < (vb[k] | 0)) return false;
    }
    return true;
  }

  /**
   * 从本地保留日志中挑出对端缺失的信封：
   * 信封 e 的发送者计数 e.clock[e.clientId] 大于对端已知该发送者计数即为缺失。
   */
  function missingForPeer(log, theirVC) {
    const out = [];
    for (const e of log) {
      const sender = e.clientId;
      const need = (e.clock && e.clock[sender]) | 0;
      if (need > ((theirVC && theirVC[sender]) | 0)) out.push(e);
    }
    return out;
  }

  /* ============================== 背压队列 ============================== */

  /**
   * 客户端发送合批队列：
   *  - 连续移动/缩放帧（带相同 squashKey）在真正发出前可被新帧替换（coalesce）；
   *  - 高水位时先丢弃可合并帧，仍满则拒绝入队（调用方降级：降采样/转快照）；
   *  - 低水位恢复。字节级计量，对接 DataChannel.bufferedAmount / WS.bufferedAmount。
   */
  class CoalescingQueue {
    constructor(opts) {
      this.high = (opts && opts.highBytes) || 256 * 1024;
      this.low = (opts && opts.lowBytes) || Math.floor(this.high / 2);
      this.jobs = [];
      this.squashIndex = new Map(); // squashKey -> job
      this.bytes = 0;
      this.coalesced = 0;
      this.shed = 0;
      this.blocked = false;
    }
    /**
     * @param {object} job {squashKey?, size, run}
     * @returns {'accepted'|'coalesced'|'shed'|'blocked'}
     */
    push(job) {
      const size = job.size || 0;
      if (job.squashKey && this.squashIndex.has(job.squashKey)) {
        const old = this.squashIndex.get(job.squashKey);
        this.bytes -= old.size || 0;
        old.size = size;
        old.run = job.run;
        this.bytes += size;
        this.coalesced++;
        return 'coalesced';
      }
      if (this.bytes + size > this.high) {
        // 先尝试淘汰其它可合并帧（高频手势中间帧最没价值）
        for (let i = 0; i < this.jobs.length; i++) {
          const j = this.jobs[i];
          if (j.squashKey && j.squashKey !== job.squashKey) {
            this.bytes -= j.size || 0;
            this.squashIndex.delete(j.squashKey);
            this.jobs.splice(i, 1);
            this.shed++;
            i--;
            if (this.bytes + size <= this.high) break;
          }
        }
      }
      if (this.bytes + size > this.high) { this.blocked = true; return 'blocked'; }
      this.jobs.push(job);
      if (job.squashKey) this.squashIndex.set(job.squashKey, job);
      this.bytes += size;
      this.blocked = false;
      return 'accepted';
    }
    /** 取出并执行全部任务（FIFO），返回执行数 */
    drain(run) {
      let n = 0;
      while (this.jobs.length) {
        const job = this.jobs.shift();
        if (job.squashKey) this.squashIndex.delete(job.squashKey);
        this.bytes -= job.size || 0;
        const fn = run || job.run;
        if (typeof fn === 'function') fn(job);
        n++;
      }
      this.blocked = this.bytes > this.low;
      return n;
    }
    get length() { return this.jobs.length; }
  }

  /**
   * 服务端/对端的出站计量：按扇出序号 fseq 记账，接收端 ACK 到水位后释放。
   * 超过 highBytes 即判定慢客户端 → 上层切换快照同步。
   */
  class OutboundMeter {
    constructor(highBytes) {
      this.high = highBytes || 512 * 1024;
      this.nextFseq = 1;
      this.ackedFseq = 0;
      this.queued = new Map(); // fseq -> bytes
      this.bytes = 0;
    }
    reserve(len) {
      const fseq = this.nextFseq++;
      this.queued.set(fseq, len);
      this.bytes += len;
      return fseq;
    }
    ack(fseq) {
      for (let s = this.ackedFseq + 1; s <= fseq; s++) {
        const v = this.queued.get(s);
        if (v != null) { this.bytes -= v; this.queued.delete(s); }
      }
      if (fseq > this.ackedFseq) this.ackedFseq = fseq;
    }
    get slow() { return this.bytes > this.high; }
  }

  /* ====================== 虚拟网络（Node 确定性测试） ====================== */

  /**
   * 内存模拟的无序不可靠数据报网络（等价 unordered, maxRetransmits=0 的 DC）：
   *  - loss：独立丢包率；delay：基础延迟；jitter：随机抖动（造成乱序）；
   *  - dup：重复送达概率。setLoss/setDelay 可在运行中调整（用于背压/慢客户端测试）。
   */
  class VirtualNetwork {
    constructor(opts) {
      opts = opts || {};
      this.loss = opts.loss || 0;
      this.delay = opts.delay || 0;
      this.jitter = opts.jitter || 0;
      this.dup = opts.dup || 0;
      this.now = opts.now || (() => Date.now());
      this.setTimeout = opts.setTimeout || ((fn, ms) => setTimeout(fn, ms));
      this.edges = new Map(); // id -> transport
      this.sentBytes = 0; this.deliveredBytes = 0; this.dropped = 0;
    }
    makeEndpoint(id) {
      const t = new VirtualTransport(this, id);
      this.edges.set(id, t);
      return t;
    }
    _transmit(from, to, bytes) {
      this.sentBytes += bytes.length;
      const deliver = () => {
        const dst = this.edges.get(to);
        if (dst && dst.onmessage) { this.deliveredBytes += bytes.length; dst.onmessage(bytes, from); }
      };
      if (Math.random() < this.loss) { this.dropped++; return; }
      const ms = this.delay + (this.jitter ? Math.random() * this.jitter : 0);
      this.setTimeout(deliver, ms);
      if (Math.random() < this.dup) this.setTimeout(deliver, ms + 2);
    }
  }

  class VirtualTransport {
    constructor(net, id) { this.net = net; this.id = id; this.onmessage = null; this.onopen = null; }
    send(bytes, to) { this.net._transmit(this.id, to, toU8(bytes)); }
    open() { if (this.onopen) this.onopen(); }
  }

  /**
   * 用 VirtualNetwork 连接两个 ReliableLink（A 发往 B 的 id，反之亦然）。
   * 返回 {a, b, links:{a,b}, net}。
   */
  function virtualPair(opts) {
    const net = new VirtualNetwork(opts);
    const ta = net.makeEndpoint('A'), tb = net.makeEndpoint('B');
    const la = new ReliableLink({ send: (b) => ta.send(b, 'B'), now: net.now });
    const lb = new ReliableLink({ send: (b) => tb.send(b, 'A'), now: net.now });
    ta.onmessage = (b) => lb.handle(frameFrom(b));
    tb.onmessage = (b) => la.handle(frameFrom(b));
    return { net, ta, tb, links: { A: la, B: lb } };
  }

  /* ============================== 工具 ============================== */

  /** 一批信封编码后的字节大小（用于大/小操作路由与背压计量） */
  function encodedBytes(envelopes) {
    return encode({ type: 'ops', envelopes: Array.isArray(envelopes) ? envelopes : [envelopes] }).length;
  }

  function sha256Hex(bytes) {
    const u8 = toU8(bytes);
    if (typeof crypto !== 'undefined' && crypto.createHash) {
      return crypto.createHash('sha256').update(Buffer.from(u8)).digest('hex');
    }
    return null; // 浏览器端用 crypto.subtle.digest 异步完成
  }

  return {
    PROTO, T, LARGE_BYTES, CHUNK_BYTES, protoString, negotiate,
    Writer, Reader,
    writeVal, readVal, writeEnvelope, readEnvelope, writeEnvelopes, readEnvelopes,
    encode, frameFrom, toU8, encodedBytes,
    ReliableLink, BlobAssembler, BlobStore, chunkBlob,
    mergeVC, vcDominates, missingForPeer,
    CoalescingQueue, OutboundMeter,
    VirtualNetwork, VirtualTransport, virtualPair,
    sha256Hex
  };
});
