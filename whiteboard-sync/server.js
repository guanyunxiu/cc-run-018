'use strict';

/* ===========================================================================
 * 协作白板 v3 - 服务端（CRDT 协作内核 + 二进制/可靠传输/信令）
 *
 * v3 在 v2 基础上的迭代（同一 /ws 端口、v2 JSON 协议完全保留）：
 *  - 二进制自定义协议（public/net.js）：首字节 >= 0x80 标识二进制帧，
 *    经 WBNet.Link 提供 分片 / 严格序号 / 累积ACK / 选择性重传 / 心跳 / 背压；
 *  - HELLO 版本协商：major 不一致拒绝，minor 更高则降级运行；
 *  - 快照链：每 SNAPSHOT_EVERY 个已物化操作生成一份二进制快照（保留最近若干份）；
 *  - 增量同步：JOIN / REQ_SYNC 携带 lastSeq + 版本向量，planSync 只返回缺失部分；
 *  - Outbox 慢客户端背压：广播队列超水位自动降级为快照同步（MODE=1），
 *    客户端排空后 REQ_SYNC，服务端补一份快照并恢复流式（MODE=0）；
 *  - WebSocket 仅做信令/控制：RTC_SDP / RTC_ICE 在房间成员间转发（不做 SDP 解析），
 *    大操作与媒体在浏览器端走 WebRTC DataChannel（mesh.js）；媒体可经 /api/media 回退中继；
 *  - OPS 处理仍走 v2 的 CRDT 因果管线（CausalBuffer + LWW），seq 只排序不仲裁。
 *
 * 消息（JSON v2，保持不变）：
 *  C→S join/ops/ping   S→C joined/snapshot/delta/ack/error/pong/ops
 * 消息（二进制 v3，见 net.js MT）：
 *  HELLO/JOIN/OPS/REQ_SYNC/MEDIA_REQ/RTC_SDP/RTC_ICE/PING
 *  WELCOME/JOINED/SNAPSHOT/OPS/ACK/MODE/MEDIA/MEDIA_DATA/MEMBERS/RTC_SDP-ICE/PONG/ERROR
 * =========================================================================== */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const WB = require('./public/kernel.js');
const N = require('./public/net.js');
const MT = N.MT;

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');

const HEARTBEAT_INTERVAL_MS = 15000;
const CLIENT_TIMEOUT_MS = 45000;
const MAX_ENVELOPES = 20000;
const COMPACT_AT = 300;
const SNAPSHOT_EVERY = 100;             // 每 100 个物化操作生成一份快照
const SNAPSHOT_KEEP = 8;                // 快照链保留份数（供不同 lastSeq 的重连者选基线）
const OUTBOX_HIGH_WATER = 768 * 1024;   // 单客户端广播队列水位，超出降级快照
const MAX_POINTS_PER_OP = 20000;
const MAX_MSG_BYTES = 2 * 1024 * 1024;
const MAX_MEDIA_BYTES = 8 * 1024 * 1024;

const VALID_KINDS = new Set([
  'create', 'set', 'delete', 'restore', 'group', 'ungroup', 'layer', 'erase'
]);

/**
 * rooms: Map<roomId, {
 *   clients: Set<client>, log: env[], seq, doc: WB.Doc, buf: CausalBuffer,
 *   applied: Set<envId>, watermark, knownVC,
 *   snapshots: [{seq, data}]            // v3 二进制快照链
 *   media: Map<mediaId, {oid,mime,bytes}>  // 媒体中继回退存储
 * }>
 */
const rooms = new Map();
const mediaStore = new Map();           // mediaId -> {roomId, oid, mime, bytes}（轻量内存中继）

function getOrCreateRoom(roomId) {
  let room = rooms.get(roomId);
  if (!room) {
    room = {
      clients: new Set(),
      log: [],
      seq: 0,
      doc: new WB.Doc(),
      buf: new WB.CausalBuffer(),
      applied: new Set(),
      watermark: null,
      knownVC: Object.create(null),
      snapshots: [],
      media: new Map()
    };
    rooms.set(roomId, room);
    console.log(`[room] created: ${roomId}`);
  }
  return room;
}

function sendJSON(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (_) { /* noop */ }
  }
}
const nowTs = () => Date.now();

/* --------------------------- 信封校验（v2/v3 共用） --------------------------- */

function isVC(v) {
  if (!v || typeof v !== 'object') return false;
  for (const k of Object.keys(v)) {
    if (typeof k !== 'string' || k.length > 64) return false;
    if (!Number.isInteger(v[k]) || v[k] < 0 || v[k] > 1e9) return false;
  }
  return true;
}

function validateEnvelope(env) {
  if (!env || typeof env !== 'object') return 'bad envelope';
  if (typeof env.id !== 'string' || !/^[\w.:-]{1,100}$/.test(env.id)) return 'bad id';
  if (typeof env.clientId !== 'string' || !env.clientId || env.clientId.length > 64) return 'bad clientId';
  if (!Number.isInteger(env.lamport) || env.lamport < 0) return 'bad lamport';
  if (!isVC(env.clock)) return 'bad clock';
  if (!Number.isInteger(env.clock[env.clientId]) || env.clock[env.clientId] <= 0) return 'clock missing self';
  const op = env.op;
  if (!op || typeof op !== 'object' || !VALID_KINDS.has(op.kind)) return 'bad op kind';
  if (op.kind === 'create') {
    if (!Array.isArray(op.objects) || op.objects.length === 0 || op.objects.length > 500) return 'bad objects';
    for (const o of op.objects) {
      if (!o || typeof o.oid !== 'string' || !o.oid || typeof o.type !== 'string') return 'bad object';
      if (o.fields && typeof o.fields !== 'object') return 'bad fields';
    }
  }
  if (op.kind === 'set') {
    if (typeof op.oid !== 'string' || !op.oid) return 'bad oid';
    if (!op.fields || typeof op.fields !== 'object') return 'bad set fields';
  }
  if (op.kind === 'delete' || op.kind === 'restore') {
    if (!Array.isArray(op.oids) || op.oids.length === 0 || op.oids.length > 1000) return 'bad oids';
  }
  if (op.kind === 'group' || op.kind === 'ungroup') {
    if (typeof op.gid !== 'string' || !Array.isArray(op.oids) || op.oids.length === 0) return 'bad group';
  }
  if (op.kind === 'layer') {
    if (typeof op.oid !== 'string' || typeof op.z !== 'string') return 'bad layer';
  }
  if (op.kind === 'erase') {
    if (!Array.isArray(op.chunks) || op.chunks.length === 0 || op.chunks.length > 256) return 'bad chunks';
    for (const ch of op.chunks) {
      if (!ch || typeof ch.oid !== 'string' || !Number.isInteger(ch.tx) || !Number.isInteger(ch.ty)) return 'bad chunk';
      if (!Array.isArray(ch.cells) || ch.cells.some((c) => !Array.isArray(c) || c.length !== 2 ||
        !Number.isInteger(c[0]) || !Number.isInteger(c[1]))) return 'bad cells';
    }
  }
  if (env.txnId != null && (typeof env.txnId !== 'string' || env.txnId.length > 80)) return 'bad txnId';
  if (env.squashKey != null && (typeof env.squashKey !== 'string' || env.squashKey.length > 120)) return 'bad squashKey';
  return null;
}

/* --------------------------- 房间逻辑 --------------------------- */

function mergeVCInto(room, clock) {
  for (const k of Object.keys(clock || {})) {
    const v = clock[k] | 0;
    if (v > (room.knownVC[k] | 0)) room.knownVC[k] = v;
  }
}

/**
 * v3 快照链：每 SNAPSHOT_EVERY 个物化操作拍一份快照（快照本身是 CRDT 物化结果，
 * 内容与 JSON snapshot 相同，但用 net.js 二进制布局承载，新客户端可直接增量解码）。
 * 快照带 seq 与 knownVC；重连者按 lastSeq 选最近的 seq<=lastSeq 快照作为基线。
 */
function maybeSnapshot(room) {
  if (room.seq === 0 || room.seq % SNAPSHOT_EVERY !== 0) return;
  const data = room.doc.snapshot(room.knownVC);
  room.snapshots.push({ seq: room.seq, data });
  if (room.snapshots.length > SNAPSHOT_KEEP) room.snapshots.shift();
  console.log(`[snapshot] room seq=${room.seq} objs=${data.objects.length} chain=${room.snapshots.length}`);
}

/**
 * 应用一批信封并广播。v2(JSON) 与 v3(Link) 客户端在同一房间共存：
 *  - 对 v2 客户端发 JSON ops；
 *  - 对 v3 客户端经 Outbox 发二进制 OPS，慢客户端自动降级快照。
 */
function ingestBatch(room, client, envelopes) {
  const ids = [];
  for (const env of envelopes) {
    ids.push(env.id);
    if (room.applied.has(env.id)) continue;
    room.applied.add(env.id);
    room.buf.enqueue(env);
  }

  const fresh = drainReady(room);

  if (fresh.length) {
    maybeCompact(room);
    maybeSnapshot(room);
    let jsonN = 0, binN = 0, degraded = 0;
    for (const other of room.clients) {
      if (other === client) continue;
      if (other.mode === 'v3' && other.link && !other.link.closed) {
        const ok = other.outbox.enqueue(MT.OPS, { envelopes: fresh });
        if (ok) binN++; else degraded++;
      } else {
        sendJSON(other.ws, { type: 'ops', envelopes: fresh });
        jsonN++;
      }
    }
    const txnIds = new Set(fresh.map((e) => e.txnId).filter(Boolean));
    console.log(`[ops] seq~${room.seq} room=${client.roomId} user=${client.userId} ` +
      `n=${fresh.length}${txnIds.size ? ` txns=${txnIds.size}` : ''} log=${room.log.length} ` +
      `cast json=${jsonN} bin=${binN}${degraded ? ` degraded=${degraded}` : ''}`);
  }
  return ids;
}

function drainReady(room) {
  return room.buf.drain((env) => {
    room.seq += 1;
    env.seq = room.seq;
    env.serverTs = nowTs();
    room.log.push(env);
    room.doc.apply(env);
    mergeVCInto(room, env.clock);
  });
}

function maybeCompact(room) {
  if (room.log.length < COMPACT_AT && room.log.length < MAX_ENVELOPES) return;
  const before = room.log.length;
  const compact = WB.squash(room.log);
  if (compact.length < before) room.log = compact;
  if (room.log.length >= MAX_ENVELOPES) {
    const cut = Math.floor(room.log.length / 2);
    room.watermark = (room.watermark || 0) + cut;
    room.log.splice(0, cut);
  }
}

/** 成员变更通知（v3 二进制通道；首个非 v3 客户端为主机候选仅由浏览器侧自行选举） */
function broadcastMembers(room) {
  const ids = [];
  for (const c of room.clients) ids.push({ id: c.userId, mode: c.mode });
  for (const c of room.clients) {
    if (c.mode === 'v3' && c.link && !c.link.closed) {
      try {
        c.link.send(MT.MEMBERS, {
          ids: room.clients.size ? ids.map((x) => x.id) : [],
          host: pickHost(room)
        });
      } catch (_) { /* noop */ }
    }
  }
}
function pickHost(room) {
  // 主机（mesh 引导者）取房间内最早加入、连接正常的 v3 客户端；无则空串（回退服务端中继）
  for (const c of room.clients) {
    if (c.mode === 'v3' && c.link && !c.link.closed) return c.userId;
  }
  return '';
}

/* --------------------------- v2 JSON 加入 --------------------------- */

function handleJoin(client, msg) {
  const roomId = String(msg.roomId || '').trim();
  if (!roomId || roomId.length > 64) {
    sendJSON(client.ws, { type: 'error', message: 'invalid roomId' });
    return;
  }
  leaveRoom(client);

  const userId = String(msg.userId || '').slice(0, 64) || 'anon-' + client.id.slice(0, 4);
  client.userId = userId;
  client.roomId = roomId;
  client.mode = 'v2';

  const room = getOrCreateRoom(roomId);
  room.clients.add(client);

  const snapshot = room.doc.snapshot(room.knownVC);
  const lastSeq = room.seq;
  sendJSON(client.ws, { type: 'joined', roomId, userId, lastSeq });
  sendJSON(client.ws, {
    type: 'snapshot',
    watermark: room.watermark || 0,
    lastSeq,
    snapshot,
    envelopes: room.log.slice()
  });

  console.log(`[join:json] room=${roomId} user=${userId} members=${room.clients.size} ` +
    `objs=${snapshot.objects.length} log=${room.log.length}`);
}

/* --------------------------- v3 二进制加入 / 同步 --------------------------- */

function handleJoinV3(client, msg) {
  const roomId = String(msg.roomId || '').trim();
  if (!roomId || roomId.length > 64) {
    sendBin(client, MT.ERROR, { code: N.ERROR.BAD_MESSAGE, message: 'invalid roomId' });
    return;
  }
  leaveRoom(client);
  const userId = String(msg.userId || '').slice(0, 64) || 'anon-' + client.id.slice(0, 4);
  client.userId = userId;
  client.roomId = roomId;
  client.mode = 'v3';
  client.lastSeq = msg.lastSeq | 0;

  const room = getOrCreateRoom(roomId);
  room.clients.add(client);
  client.outbox = new N.Outbox(client.link, { highWater: OUTBOX_HIGH_WATER });

  client.link.send(MT.JOINED, { roomId, userId, lastSeq: room.seq, host: pickHost(room) });
  sendSyncPayload(client, msg.lastSeq | 0, msg.vc || {});
  broadcastMembers(room);

  console.log(`[join:bin] room=${roomId} user=${userId} members=${room.clients.size} ` +
    `clientSeq=${msg.lastSeq | 0} serverSeq=${room.seq} snaps=${room.snapshots.length}`);
}

/**
 * 增量同步核心：根据客户端 lastSeq + 版本向量，决定
 *  全量快照 / 基线快照+少量 delta / 纯 delta。
 * 慢客户端降级恢复（REQ_SYNC）也走这里；发完即恢复流式 MODE=0。
 */
function sendSyncPayload(client, lastSeq, vc) {
  const room = rooms.get(client.roomId);
  if (!room) return;
  const plan = N.planSync({
    snapshots: room.snapshots,
    log: room.log,
    currentSnapshot: room.doc.snapshot(room.knownVC),
    currentVC: room.knownVC,
    currentSeq: room.seq,
    watermark: room.watermark || 0,
    lastSeq,
    vc
  });
  // 绕过 Outbox 的降级判定（同步消息必须送达），直接走 Link
  client.link.send(MT.SNAPSHOT, plan);
  if (client.outbox) client.outbox.resume();
  client.lastSeq = room.seq;
  console.log(`[sync] user=${client.userId} base=${plan.watermark} ` +
    `${plan.hasSnapshot ? 'snapshot' : 'delta'} envelopes=${plan.envelopes.length} -> seq=${room.seq}`);
}

function leaveRoom(client) {
  if (!client.roomId) return;
  const room = rooms.get(client.roomId);
  if (room) {
    room.clients.delete(client);
    if (room.clients.size === 0) {
      // 房间清空后保留短时间供重连？这里直接释放（媒体与快照随之回收）
      rooms.delete(client.roomId);
      for (const [mid, m] of mediaStore) if (m.roomId === client.roomId) mediaStore.delete(mid);
      console.log(`[room] destroyed (empty): ${client.roomId}`);
    } else if (client.mode === 'v3') {
      broadcastMembers(room);
    }
  }
  client.roomId = null;
}

/* --------------------------- OPS 校验 / 入库 --------------------------- */

function acceptOps(client, list) {
  const txnIds = new Set(list.map((e) => e && e.txnId).filter(Boolean));
  if (txnIds.size > 1) return 'mixed txnId in one batch';
  for (const env of list) {
    const err = validateEnvelope(env);
    if (err) return err;
    if (env.clientId !== client.userId) return 'clientId mismatch';
    const pts = env.op && env.op.objects && env.op.objects[0] &&
      env.op.objects[0].fields && env.op.objects[0].fields.stroke &&
      env.op.objects[0].fields.stroke.points;
    if (pts && pts.length > MAX_POINTS_PER_OP) return 'too many points';
  }
  return null;
}

function handleOpsJSON(client, msg, rawSize) {
  if (!client.roomId) { sendJSON(client.ws, { type: 'error', message: 'join a room first' }); return; }
  if (rawSize > MAX_MSG_BYTES) { sendJSON(client.ws, { type: 'error', message: 'message too large' }); return; }
  const list = Array.isArray(msg.envelopes) ? msg.envelopes : null;
  if (!list || list.length === 0 || list.length > 1000) {
    sendJSON(client.ws, { type: 'error', message: 'envelopes required' });
    return;
  }
  const err = acceptOps(client, list);
  if (err) { sendJSON(client.ws, { type: 'error', message: err, envId: list[0] && list[0].id }); return; }
  const ids = ingestBatch(rooms.get(client.roomId), client, list);
  sendJSON(client.ws, { type: 'ack', ids, lastSeq: rooms.get(client.roomId).seq });
}

function handleOpsV3(client, msg) {
  if (!client.roomId) { sendBin(client, MT.ERROR, { code: N.ERROR.NOT_JOINED, message: 'join a room first' }); return; }
  const list = Array.isArray(msg.envelopes) ? msg.envelopes : null;
  if (!list || list.length === 0 || list.length > 1000) {
    sendBin(client, MT.ERROR, { code: N.ERROR.BAD_MESSAGE, message: 'envelopes required' });
    return;
  }
  const err = acceptOps(client, list);
  if (err) { sendBin(client, MT.ERROR, { code: N.ERROR.BAD_MESSAGE, message: err, envId: list[0] && list[0].id }); return; }
  const room = rooms.get(client.roomId);
  const ids = ingestBatch(room, client, list);
  client.link.send(MT.ACK, { ids, lastSeq: room.seq });
}

function sendBin(client, type, msg) {
  if (client.link && !client.link.closed) {
    try { client.link.send(type, msg); } catch (_) { /* noop */ }
  }
}

/* --------------------------- RTC 信令转发（WS 控制面） --------------------------- */

function forwardRTC(room, from, type, msg) {
  if (!msg.to || typeof msg.to !== 'string') return;
  const target = [...room.clients].find((c) => c.userId === msg.to && c.mode === 'v3');
  if (!target) return;
  const payload = type === MT.RTC_SDP
    ? { from: from.userId, to: msg.to, sdp: msg.sdp }
    : { from: from.userId, to: msg.to, candidate: msg.candidate };
  try { target.link.send(type, payload); } catch (_) { /* noop */ }
}

/* --------------------------- 媒体中继（DataChannel 不可用时回退） --------------------------- */

function handleMediaReq(client, msg) {
  if (!client.roomId) return;
  const m = mediaStore.get(msg.mediaId);
  if (!m || m.roomId !== client.roomId) {
    sendBin(client, MT.ERROR, { code: N.ERROR.BAD_MESSAGE, message: 'media not found' });
    return;
  }
  const chunkSize = 16 * 1024;
  let offset = Math.max(0, msg.offset | 0);
  if (offset > m.bytes.length) offset = 0;
  // 仅通过 WS 中继首段做引导；完整大文件建议走 P2P DataChannel / HTTP
  const end = Math.min(offset + chunkSize, m.bytes.length);
  client.link.send(MT.MEDIA_DATA, {
    mediaId: msg.mediaId, offset, last: end === m.bytes.length,
    bytes: m.bytes.subarray(offset, end)
  });
}

/* ----------------------------- HTTP ----------------------------- */

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml'
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

  if (url.pathname === '/api/room') {
    const room = rooms.get(url.searchParams.get('roomId'));
    if (!room) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
    const snap = room.doc.snapshot(room.knownVC);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      roomId: url.searchParams.get('roomId'),
      members: room.clients.size,
      seq: room.seq,
      logLen: room.log.length,
      watermark: room.watermark,
      snapshots: room.snapshots.map((s) => ({ seq: s.seq, objs: s.data.objects.length })),
      objectCount: snap.objects.length,
      liveCount: room.doc.liveObjects().length,
      knownVC: room.knownVC,
      log: room.log.map((e) => ({ seq: e.seq, id: e.id, clientId: e.clientId,
        lamport: e.lamport, kind: e.op && e.op.kind, txnId: e.txnId || null,
        squashKey: e.squashKey || null }))
    }));
    return;
  }

  if (url.pathname === '/api/compact') {
    const room = rooms.get(url.searchParams.get('roomId'));
    if (!room) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
    const before = room.log.length;
    room.log = WB.squash(room.log);
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({
      before, after: room.log.length
    }));
    return;
  }

  // 手动快照（测试/运维）：立刻在当前 seq 生成一份快照链成员
  if (url.pathname === '/api/snapshot') {
    const room = rooms.get(url.searchParams.get('roomId'));
    if (!room) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
    const data = room.doc.snapshot(room.knownVC);
    room.snapshots.push({ seq: room.seq, data });
    while (room.snapshots.length > SNAPSHOT_KEEP) room.snapshots.shift();
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({
      seq: room.seq, objects: data.objects.length, chain: room.snapshots.map((s) => s.seq)
    }));
    return;
  }

  // 媒体中继上传（P2P 不可用时回退；也供 curl / 测试验证断点续传）
  if (url.pathname === '/api/media' && req.method === 'PUT') {
    const roomId = url.searchParams.get('roomId');
    const oid = url.searchParams.get('oid') || '';
    const mime = (req.headers['content-type'] || 'application/octet-stream').slice(0, 80);
    const chunks = [];
    let size = 0;
    req.on('data', (d) => {
      size += d.length;
      if (size > MAX_MEDIA_BYTES) { req.destroy(); return; }
      chunks.push(d);
    });
    req.on('end', () => {
      if (!rooms.has(roomId)) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
      const bytes = Buffer.concat(chunks);
      const mediaId = crypto.randomBytes(8).toString('hex');
      mediaStore.set(mediaId, { roomId, oid, mime, bytes });
      const meta = { mediaId, oid, mime, totalBytes: bytes.length, chunkSize: 16 * 1024, nChunks: Math.ceil(bytes.length / (16 * 1024)) };
      const room = rooms.get(roomId);
      for (const c of room.clients) {
        if (c.mode === 'v3' && c.link && !c.link.closed) {
          try { c.link.send(MT.MEDIA, Object.assign({ url: '', from: '' }, meta)); } catch (_) {}
        }
      }
      res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(meta));
    });
    return;
  }

  if (url.pathname === '/api/rooms') {
    const summary = [];
    for (const [roomId, room] of rooms) {
      summary.push({
        roomId, members: room.clients.size, seq: room.seq,
        logLen: room.log.length, watermark: room.watermark,
        snapshots: room.snapshots.map((s) => s.seq),
        liveCount: room.doc.liveObjects().length
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(summary));
    return;
  }

  let pathname = decodeURIComponent(url.pathname);
  if (pathname === '/') pathname = '/index.html';
  const filePath = path.normalize(path.join(PUBLIC_DIR, pathname));
  if (!filePath.startsWith(PUBLIC_DIR)) { res.writeHead(403).end('Forbidden'); return; }
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404).end('Not Found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
    res.end(data);
  });
});

/* --------------------------- WebSocket（v2 JSON + v3 二进制） --------------------------- */

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  const client = {
    id: crypto.randomBytes(6).toString('hex'),
    ws, userId: null, roomId: null,
    lastSeen: nowTs(), alive: true,
    mode: null,               // null（握手前）| 'v2' | 'v3'
    link: null,
    outbox: null,
    helloDone: false,
    session: 0
  };

  ws.on('pong', () => { client.lastSeen = nowTs(); client.alive = true; });

  /* ----- v3：二进制帧经 StreamTransport + Link 可靠层 ----- */
  let transport = null;
  function attachBinary() {
    if (transport) return;
    // socket 写缓冲水位：慢客户端不读时停止继续写入（让 Link 的在途窗口/重传自然背压），
    // 避免向一个停摆的 socket 无限缓冲 + RTO 放大重传拖垮整个服务端事件循环。
    const SOCK_HIGH = 4 * 1024 * 1024;
    transport = new N.StreamTransport({
      send(u8) {
        if (ws.readyState !== ws.OPEN) return false;
        const sock = ws._socket;
        if (sock && sock.writableLength > SOCK_HIGH) return false; // 背压：Link 保帧稍后重发
        ws.send(Buffer.from(u8.buffer, u8.byteOffset, u8.byteLength));
        return true;
      }
    });
    // socket 缓冲排空时通知 Link 续发
    const onDrain = () => transport.emit('drain');
    const wireDrain = () => { const s = ws._socket; if (s && s.writableLength <= SOCK_HIGH) transport.emit('drain'); };
    // ws 在内部 socket 可写时触发；保险起见用底层 socket 的 drain 事件
    setTimeout(() => { const s = ws._socket; if (s) s.on('drain', wireDrain); }, 0);
    void onDrain;
    client.link = new N.Link(transport, {
      pingInterval: 15000, timeout: CLIENT_TIMEOUT_MS
    });
    client.link.on('message', (type, msg) => onV3Message(client, type, msg));
    client.link.on('reset', (reason) => {
      console.log(`[link:reset] user=${client.userId || '-'} reason=${reason}`);
      try { ws.terminate(); } catch (_) { /* noop */ }
    });
    client.link.on('error', (err) => console.error('[link error]', client.userId || '-', err.message));
  }

  ws.on('message', (raw, isBinary) => {
    client.lastSeen = nowTs(); client.alive = true;

    // v3 二进制：ws 库在 binaryType=nodebuffer 时以 Buffer 交付；首字节 >=0x80
    const first = raw.length ? raw[0] : 0;
    if (isBinary || first >= 0x80) {
      attachBinary();
      transport.feed(new Uint8Array(raw.buffer, raw.byteOffset, raw.length));
      return;
    }

    // v2 / v3 握手前 JSON（v3 客户端也必须先发二进制 HELLO，这里只处理 v2）
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch (_) { sendJSON(ws, { type: 'error', message: 'invalid json' }); return; }

    switch (msg.type) {
      case 'join': handleJoin(client, msg); break;
      case 'ops': handleOpsJSON(client, msg, raw.length); break;
      case 'ping': sendJSON(ws, { type: 'pong', ts: nowTs() }); break;
      default: sendJSON(ws, { type: 'error', message: `unknown type: ${msg.type}` });
    }
  });

  ws.on('close', () => { leaveRoom(client); });
  ws.on('error', (err) => {
    console.error('[ws error]', err.message);
    try { ws.terminate(); } catch (_) { /* noop */ }
    leaveRoom(client);
  });
});

/** v3 应用消息路由（Link 已完成重组/去重/重排） */
function onV3Message(client, type, msg) {
  client.lastSeen = nowTs();
  switch (type) {
    case MT.HELLO: {
      if (client.helloDone) return;
      // 协议版本协商：name 校验 + major 必须一致；minor 更新则告知降级
      if (msg.name !== N.PROTO.name) {
        client.link.send(MT.WELCOME, { ok: false, action: 2, major: N.PROTO.major, minor: N.PROTO.minor, reason: 'bad protocol', session: 0 });
        return;
      }
      const neg = N.negotiate(msg.major | 0, msg.minor | 0);
      client.session = (msg.session | 0) >>> 0;
      client.link.send(MT.WELCOME, {
        ok: neg.ok,
        action: neg.action === 'reject' ? 2 : neg.action === 'degrade' ? 1 : 0,
        major: N.PROTO.major, minor: N.PROTO.minor,
        reason: neg.reason || '', session: client.session
      });
      if (neg.action === 'reject') {
        console.log(`[hello] reject major=${msg.major} from ${msg.session}`);
        setTimeout(() => { try { client.ws.terminate(); } catch (_) {} }, 50);
        return;
      }
      client.helloDone = true;
      break;
    }
    case MT.JOIN:
      if (!client.helloDone) { sendBin(client, MT.ERROR, { code: N.ERROR.BAD_MESSAGE, message: 'hello first' }); return; }
      handleJoinV3(client, msg);
      break;
    case MT.OPS:
      handleOpsV3(client, msg);
      break;
    case MT.REQ_SYNC:
      // 慢客户端排空 / 断线续传：按 lastSeq+VC 补发增量或快照
      if (client.roomId) sendSyncPayload(client, msg.lastSeq | 0, msg.vc || {});
      break;
    case MT.RTC_SDP:
    case MT.RTC_ICE:
      if (client.roomId) forwardRTC(rooms.get(client.roomId), client, type, msg);
      break;
    case MT.MEDIA_REQ:
      handleMediaReq(client, msg);
      break;
    case MT.MEDIA:
      // 客户端经 P2P 发布的媒体元数据：仅向房间其余 v3 成员转发元数据，
      // 字节由 DataChannel 直传；服务端不在 WS 上承载媒体字节。
      if (client.roomId) {
        const room = rooms.get(client.roomId);
        for (const other of room.clients) {
          if (other === client || other.mode !== 'v3' || !other.link || other.link.closed) continue;
          try { other.link.send(MT.MEDIA, Object.assign({ from: client.userId }, msg)); } catch (_) {}
        }
      }
      break;
    case MT.PING:
      sendBin(client, MT.PONG, { ts: msg.ts | 0 });
      break;
    case MT.BYE:
      try { client.ws.close(1000, 'bye'); } catch (_) { /* noop */ }
      break;
    default:
      sendBin(client, MT.ERROR, { code: N.ERROR.BAD_MESSAGE, message: 'unknown v3 type ' + type });
  }
}

const heartbeatTimer = setInterval(() => {
  const now = nowTs();
  for (const room of rooms.values()) {
    for (const client of room.clients) {
      // v3 由 Link 自身 PING/超时管理；v2 走 ws ping
      if (client.mode === 'v3') {
        if (client.link && client.link.closed) { try { client.ws.terminate(); } catch (_) {} }
        continue;
      }
      if (client.ws.readyState !== client.ws.OPEN || now - client.lastSeen > CLIENT_TIMEOUT_MS) {
        try { client.ws.terminate(); } catch (_) { /* noop */ }
      } else {
        try { client.ws.ping(); } catch (_) { /* noop */ }
      }
    }
  }
}, HEARTBEAT_INTERVAL_MS);
wss.on('close', () => clearInterval(heartbeatTimer));

server.listen(PORT, HOST, () => {
  const actualPort = server.address().port;
  // 便于测试/守护进程解析实际端口（PORT=0 时由系统分配）
  console.log('LISTENING_PORT=' + actualPort);
  console.log('==============================================================');
  console.log('  Collaborative Whiteboard v3 (CRDT + binary Link + RTC mesh)');
  console.log(`  HTTP : http://localhost:${PORT}/`);
  console.log(`  WS   : ws://<host>:${PORT}/ws  (v2 JSON + v3 binary 共存)`);
  console.log(`  API  : /api/rooms /api/room /api/snapshot /api/media`);
  console.log('==============================================================');
});

server.on('error', (err) => {
  // 端口占用等启动期致命错误必须退出（否则进程空转，测试/守护进程无法感知失败）
  console.error('[server fatal]', err);
  process.exit(1);
});

process.on('uncaughtException', (err) => console.error('[uncaughtException]', err));
