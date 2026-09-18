'use strict';

/* ===========================================================================
 * 协作白板 v2 - 服务端（CRDT 协作内核）
 *
 * 与 v1 的本质区别：
 *  - 服务端 seq 只用于日志排序 / 观测，不再承担冲突仲裁；
 *  - 冲突由 LWW-Register CRDT（lamport + clientId）在各端确定性折叠；
 *  - 每条信封携带 clientId / lamport / clock（依赖向量）；
 *  - CausalBuffer 做 happens-before 因果投递，整事务（txnId）原子广播；
 *  - 支持选择性撤销（逆操作信封）、操作压缩（squashKey + 快照水位）。
 *
 * 消息：
 *  C→S  join {roomId,userId,lastSeq,since:knownVC?}
 *       ops  {envelopes:[...]}            // 单条或事务原子组
 *       ping
 *  S→C  joined / snapshot / delta / ack / error / pong
 *       ops  {envelopes:[...], fromClientId}
 * =========================================================================== */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const WB = require('./public/kernel.js');
const WBT = require('./public/transport.js');

const PORT = process.env.PORT || 8080;
const HOST = process.env.HOST || '0.0.0.0';
const PUBLIC_DIR = path.join(__dirname, 'public');

const HEARTBEAT_INTERVAL_MS = 15000;
const CLIENT_TIMEOUT_MS = 45000;
const MAX_ENVELOPES = 20000;          // 每房间信封硬上限（超出触发压缩 + 快照水位）
const COMPACT_AT = 300;               // 超过该条数触发一次压缩（移动/缩放类日志占大头）
const MAX_POINTS_PER_OP = 20000;
const MAX_MSG_BYTES = 2 * 1024 * 1024;
const MAX_BIN_BYTES = 4 * 1024 * 1024;

/* v3：快照节奏 / 慢客户端降级阈值 */
const SNAPSHOT_EVERY = 100;           // 每 N 个物化操作落一次快照（新客户端快照+少量重放）
const SNAPSHOT_KEEP = 3;              // 每房间保留最近几个快照
const SLOW_CLIENT_HIGH = 512 * 1024;  // 扇出在途字节高水位：判定慢客户端
const SLOW_CLIENT_LOW = 256 * 1024;
const BROADCAST_QUEUE_CAP = 1024;     // 每客户端广播消息条数硬上限（超出直接降级）

const VALID_KINDS = new Set([
  'create', 'set', 'delete', 'restore', 'group', 'ungroup', 'layer', 'erase'
]);

/**
 * rooms: Map<roomId, {
 *   clients: Set<client>,
 *   log: env[],                 // 全量信封（按到达 seq 升序，seq 仅排序用）
 *   seq: number,
 *   doc: WB.Doc,                // 服务端权威物化（供快照/观测）
 *   applied: Set<envId>,        // 幂等
 *   watermark: number|null,     // 已压缩进快照的信封数（log 中该下标之前已被折叠）
 *   knownVC: Object,            // 跨所有已应用信封合并的版本向量（快照基线）
 *   snapshots: [{seq, watermark, snapshot, known, blobs}],  // v3 周期快照（最近 SNAPSHOT_KEEP）
 *   blobRefs: Map<blobId, ref>, // v3 大对象（P2P blob）登记：只存引用，字节不过服务器
 *   opsSinceSnapshot: number
 * }>
 */
const rooms = new Map();

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
      blobRefs: new Map(),
      opsSinceSnapshot: 0
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
/** 发送二进制 v3 帧（ws 会自动按 Buffer 走 binary opcode） */
function sendBin(client, msg) {
  if (client.ws.readyState !== client.ws.OPEN) return;
  try { client.ws.send(WBT.encode(msg)); } catch (_) { /* noop */ }
}
const nowTs = () => Date.now();

/* --------------------------- 信封校验 --------------------------- */

function isFiniteNum(v) { return typeof v === 'number' && Number.isFinite(v); }
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
  // 信封自洽：clock 中必须声明自己的本地计数
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
      if (!Array.isArray(ch.cells) || ch.cells.some((c) => !Array.isArray(c) || c.length !== 2)) return 'bad cells';
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
 * 应用一批信封（同一发送者、可能是事务原子组）。
 * 服务端同样是一个 CRDT 副本：信封先过 CausalBuffer（保证物化时 create 先于 set，
 * 快照永远建立在因果一致的状态上），就绪信封按因果顺序 apply、分配仅用于日志排序的
 * seq，再在同一帧广播给其他成员；事务成员在就绪序列中相邻，随同一条 ops 消息原子下发。
 * 冲突仲裁仍完全由 LWW CRDT 完成，seq 不参与。
 *
 * v3：
 *  - JSON(v2) 客户端：立即文本广播（保持 v2 行为不变）；
 *  - 二进制(v3) 客户端：进入每客户端扇出队列（OutboundMeter 记账 fseq），
 *    慢客户端在途超水位自动降级为快照同步。
 *  - 每 SNAPSHOT_EVERY 个物化操作落一份周期快照，供晚加入/断线续传加速。
 */
function ingestBatch(room, client, envelopes) {
  const ids = [];
  for (const env of envelopes) {
    ids.push(env.id);
    if (room.applied.has(env.id)) continue;
    room.applied.add(env.id);
    room.buf.enqueue(env); // 整批先入队（事务成员收齐），再一次性冲刷，保证原子广播
  }

  // 冲刷所有满足因果（可能跨多个发送者）的就绪信封
  const fresh = drainReady(room);

  if (fresh.length) {
    maybeCompact(room);
    // v2 JSON 扇出（立即，不经过 v3 背压队列）
    const payload = { type: 'ops', envelopes: fresh };
    for (const other of room.clients) {
      if (other === client || other.bin) continue;
      sendJSON(other.ws, payload);
    }
    // v3 二进制扇出（有序号 + 背压 + 降级；发送者自己也不回收自己发的）
    for (const other of room.clients) {
      if (other === client || !other.bin) continue;
      fanoutBin(other, { type: 'ops', envelopes: fresh });
    }
    maybeSnapshot(room);
    const txnIds = new Set(fresh.map((e) => e.txnId).filter(Boolean));
    console.log(`[ops] seq~${room.seq} room=${client.roomId} user=${client.userId} ` +
      `n=${fresh.length}${txnIds.size ? ` txns=${txnIds.size}` : ''} log=${room.log.length} ` +
      `pending=${room.buf.pendingCount} broadcast=${room.clients.size - 1}`);
  }
  return ids;
}

/** 冲刷因果缓冲：就绪信封物化到权威 Doc、分配仅用于日志排序的 seq */
function drainReady(room) {
  return room.buf.drain((env) => {
    room.seq += 1;
    env.seq = room.seq;
    env.serverTs = nowTs();
    room.log.push(env);
    room.doc.apply(env);
    mergeVCInto(room, env.clock);
    room.opsSinceSnapshot += 1;
  });
}

/* ---------------------- v3：周期快照（快照加速） ---------------------- */

/**
 * 每 SNAPSHOT_EVERY 个物化操作生成一份快照基线：
 * 新客户端先加载快照，只需重放快照 seq 之后的少量操作（O(差距) 而非全量日志）。
 */
function maybeSnapshot(room) {
  if (room.opsSinceSnapshot < SNAPSHOT_EVERY) return;
  room.opsSinceSnapshot = 0;
  const snap = {
    seq: room.seq,
    watermark: room.watermark || 0,
    snapshot: room.doc.snapshot(room.knownVC),
    known: Object.assign(Object.create(null), room.knownVC),
    blobs: blobRefsForClient(room, null),
    at: nowTs()
  };
  room.snapshots.push(snap);
  if (room.snapshots.length > SNAPSHOT_KEEP) room.snapshots.shift();
  console.log(`[snapshot] room snapshot @seq=${snap.seq} objs=${snap.snapshot.objects.length} kept=${room.snapshots.length}`);
}

/** 为某客户端挑选最合适的快照基线（<= 其 lastSeq 的最新一份；否则用最新） */
function pickSnapshot(room) {
  if (!room.snapshots.length) return null;
  return room.snapshots[room.snapshots.length - 1];
}

/**
 * 日志压缩：squashKey 相同的信封只留最终一条；
 * 被压缩掉的信封折叠进“快照水位”，新加入/水位之后的重连者走 snapshot，不再需要它们。
 * 水位之前的信封从 log 移除，但 applied 集合保留 id，防止重发重复入库。
 */
function maybeCompact(room) {
  if (room.log.length < COMPACT_AT && room.log.length < MAX_ENVELOPES) return;
  const before = room.log.length;
  const compact = WB.squash(room.log);
  // squash 只去掉被更高 lamport 同 squashKey 覆盖的信封；
  // 被去掉的信封不再需要下发给任何人（它们的最终效果已在保留信封里），
  // 但为了让“晚加入者”仍能重建，把压缩时刻的物化结果作为水位快照基线。
  if (compact.length < before) {
    room.log = compact;
  }
  // 无论 squash 是否减少，超过硬上限都做快照水位裁剪：折叠最旧信封
  if (room.log.length >= MAX_ENVELOPES) {
    const cut = Math.floor(room.log.length / 2);
    room.watermark = (room.watermark || 0) + cut;
    room.log.splice(0, cut);
  }
}

function handleJoin(client, msg) {
  const roomId = String(msg.roomId || '').trim();
  if (!roomId || roomId.length > 64) {
    sendJSON(client.ws, { type: 'error', code: 'bad-room', message: 'invalid roomId' });
    return;
  }
  leaveRoom(client);

  const userId = String(msg.userId || '').slice(0, 64) || 'anon-' + client.id.slice(0, 4);
  client.userId = userId;
  client.roomId = roomId;

  const room = getOrCreateRoom(roomId);
  room.clients.add(client);

  // ---- v3 协议协商：JSON join 携带 proto 块 ----
  const reqProto = msg.proto || null;
  if (reqProto && reqProto.name !== WBT.PROTO.NAME) {
    sendJSON(client.ws, { type: 'error', code: 'proto-name',
      message: 'unsupported protocol family', supported: [WBT.protoString(WBT.PROTO.MAJOR, WBT.PROTO.MINOR)] });
    room.clients.delete(client);
    client.roomId = null;
    return;
  }
  if (reqProto && reqProto.major !== WBT.PROTO.MAJOR) {
    // 大版本不一致：拒绝（带 close code）
    sendJSON(client.ws, { type: 'error', code: 'proto-major',
      message: `incompatible protocol major: client=${reqProto.major} server=${WBT.PROTO.MAJOR}`,
      supported: [WBT.protoString(WBT.PROTO.MAJOR, WBT.PROTO.MINOR)] });
    try { client.ws.close(WBT.PROTO.CLOSE_INCOMPATIBLE, 'incompatible protocol major'); } catch (_) {}
    room.clients.delete(client);
    client.roomId = null;
    return;
  }
  if (reqProto) {
    const neg = WBT.negotiate(reqProto.name || WBT.PROTO.NAME, reqProto.major, reqProto.minor);
    if (!neg.ok) { sendJSON(client.ws, { type: 'error', code: 'proto', message: neg.reason }); return; }
    initBinClient(client, room, { minor: neg.minor, caps: reqProto.caps | 0,
      lastSeq: Number(msg.lastSeq) || 0, vc: msg.vc || null });
    // 服务端只“协商”，真正的二进制 join 会带 lastSeq/VC 再走一次 handleBinJoin
    return;
  }

  // ---- v2 JSON 路径（保持旧协议行为） ----
  // 发送当前快照 + 水位之后的增量信封。
  const snapshot = room.doc.snapshot(room.knownVC);
  const lastSeq = room.seq;

  sendJSON(client.ws, { type: 'joined', roomId, userId, lastSeq });
  sendJSON(client.ws, {
    type: 'snapshot',
    watermark: room.watermark || 0,
    lastSeq,
    snapshot,
    envelopes: room.log.slice() // 快照之后仍在日志中的信封（客户端按 id 幂等折叠）
  });

  console.log(`[join] room=${roomId} user=${userId} members=${room.clients.size} ` +
    `objs=${snapshot.objects.length} log=${room.log.length}${room.watermark != null ? ` wm=${room.watermark}` : ''}`);
}

/* ---------------------- v3：二进制客户端生命周期 ---------------------- */

function initBinClient(client, room, info) {
  client.bin = true;
  client.protoMinor = info.minor;
  client.caps = info.caps;
  client.sessionId = makeSessionId();
  client.meter = new WBT.OutboundMeter(SLOW_CLIENT_HIGH);
  client.degraded = false;
  client.pendingJoin = { lastSeq: info.lastSeq | 0, vc: info.vc || null };
  // 文本 joined 里携带 proto 协商结果；客户端随后发二进制 join
  sendJSON(client.ws, {
    type: 'joined',
    roomId: client.roomId, userId: client.userId, lastSeq: room.seq,
    sessionId: client.sessionId,
    snapEvery: SNAPSHOT_EVERY,
    proto: { name: WBT.PROTO.NAME, major: WBT.PROTO.MAJOR,
      minor: info.minor, caps: WBT.PROTO.CAPS.BIN | WBT.PROTO.CAPS.SNAP3 | WBT.PROTO.CAPS.BLOB |
        ((client.caps & WBT.PROTO.CAPS.DC) ? WBT.PROTO.CAPS.DC : 0) },
    vc: Object.assign(Object.create(null), room.knownVC)
  });
}

function handleBinJoin(client, msg) {
  const roomId = String(msg.roomId || '').trim();
  if (!roomId) { sendBin(client, { type: 'error', code: 'bad-room', message: 'invalid roomId' }); return; }
  if (client.roomId && client.roomId !== roomId) leaveRoom(client);
  const room = getOrCreateRoom(roomId);
  if (!room.clients.has(client)) room.clients.add(client);
  client.roomId = roomId;
  client.userId = String(msg.userId || client.userId || '').slice(0, 64) || ('anon-' + client.id.slice(0, 4));

  const lastSeq = msg.lastSeq | 0;
  const theirVC = msg.vc || {};

  sendBin(client, {
    type: 'joined',
    major: WBT.PROTO.MAJOR, minor: client.protoMinor || WBT.PROTO.MINOR,
    caps: client.caps, sessionId: client.sessionId | 0,
    roomId, userId: client.userId, lastSeq: room.seq,
    snapEvery: SNAPSHOT_EVERY, vc: room.knownVC
  });

  sendSyncState(client, room, lastSeq, theirVC);
  // 推送当前 P2P roster（sessionId 列表）
  sendRoster(room);

  console.log(`[join3] room=${roomId} user=${client.userId} sid=${client.sessionId} ` +
    `lastSeq=${lastSeq} members=${room.clients.size}`);
}

/**
 * 断线续传 / 增量同步决策：
 *  1. 客户端 lastSeq >= room.seq：只需 joined，无增量；
 *  2. 有周期快照且 lastSeq < 快照 seq：先快照，再重放快照之后的少量操作；
 *  3. lastSeq 仍在日志窗口内：只发 delta（缺失信封，服务端 seq 连续）；
 *  4. lastSeq 太旧（已被日志裁剪）：全量快照 + 剩余日志。
 * 另按客户端 VC 过滤（同 seq 窗口也可能因扇出降级漏收部分发送者）。
 */
function sendSyncState(client, room, lastSeq, theirVC) {
  const snap = pickSnapshot(room);
  const log = room.log; // 已按 seq 升序
  const earliestSeq = log.length ? (log[0].seq || 0) : room.seq + 1;

  if (lastSeq >= room.seq) {
    if (room.seq === 0) {
      // 空房间新加入：也要给一份空基线，客户端 NetClient 以此为“已同步”起点
      fanoutBin(client, {
        type: 'snapshot', watermark: 0, lastSeq: 0,
        snapshot: room.doc.snapshot(room.knownVC), envelopes: [], blobs: []
      });
    }
    return;
  }

  if (snap && lastSeq < snap.seq) {
    // 快照加速：重放快照之后的信封（按 VC 进一步剔除客户端已有的）
    const after = log.filter((e) => e.seq > snap.seq && needByVC(e, theirVC));
    fanoutBin(client, {
      type: 'snapshot',
      watermark: snap.watermark,
      lastSeq: room.seq,
      snapshot: snap.snapshot,
      envelopes: after,
      blobs: snap.blobs
    });
    return;
  }

  if (lastSeq >= earliestSeq - 1) {
    // 纯增量：只发 seq 窗口内缺失、且 VC 判定客户端没有的操作
    const missing = log.filter((e) => e.seq > lastSeq && needByVC(e, theirVC));
    if (missing.length) {
      fanoutBin(client, { type: 'delta', fromSeq: missing[0].seq - 1, envelopes: missing });
    }
    return;
  }

  // 太旧：全量基线快照（当前物化）+ 剩余日志
  fanoutBin(client, {
    type: 'snapshot',
    watermark: room.watermark || 0,
    lastSeq: room.seq,
    snapshot: room.doc.snapshot(room.knownVC),
    envelopes: log.slice().filter((e) => needByVC(e, theirVC)),
    blobs: blobRefsForClient(room, null)
  });
}

/** 信封是否为该 VC 尚未覆盖（发送者维度） */
function needByVC(env, theirVC) {
  if (!theirVC || !Object.keys(theirVC).length) return true;
  const sender = env.clientId;
  const need = (env.clock && env.clock[sender]) | 0;
  return need > (theirVC[sender] | 0);
}

/* ---------------------- v3：扇出队列 + 慢客户端降级 ---------------------- */

/**
 * 给一个 v3 客户端排队一条扇出帧，分配单调 fseq。
 * - 正常：直接写 socket（ws 自身缓冲做 TCP 背压）；
 * - 在途（已发未 ACK）字节持续超水位：判定为慢客户端，发 degrade 后
 *   用一份最新快照重置它，清空积压 —— 旧帧不再追发，杜绝旧状态覆盖。
 */
function fanoutBin(client, msg) {
  const room = rooms.get(client.roomId);
  if (!room || client.ws.readyState !== client.ws.OPEN) return;

  // 降级中的客户端不收增量（快照已是最新），避免快照/增量竞态
  if (client.degraded) return;

  const outstanding = client.meter.nextFseq - client.meter.ackedFseq - 1;
  if (client.meter.slow || outstanding >= BROADCAST_QUEUE_CAP) {
    degradeClient(client, room, 1);
    return; // 降级路径已经把最新快照发出，本条积压帧丢弃
  }

  // 先分配 fseq 再编码：接收端据此做扇出帧序号校验（旧帧/重复帧丢弃）
  const probe = WBT.encode(msg);
  msg.fseq = client.meter.reserve(probe.length);
  const bytes = WBT.encode(msg);
  try {
    client.ws.send(bytes);
    client.qlen = (client.qlen || 0) + 1;
  } catch (_) { /* socket 异常，心跳会清理 */ }
}

/**
 * 慢客户端降级：丢弃积压 → 发 degrade 通知 → 发全量快照 → 快照 ACK 后恢复正常。
 * 降级期间收到的 ops 不入队（避免快照与增量竞态），快照里已经是最新状态。
 */
function degradeClient(client, room, reason) {
  if (client.degraded) return;
  client.degraded = true;
  client.meter = new WBT.OutboundMeter(SLOW_CLIENT_HIGH);
  try { client.ws.send(WBT.encode({ type: 'degrade', reason, seq: room.seq })); } catch (_) {}
  const snap = pickSnapshot(room);
  const base = snap && snap.seq >= room.seq - 500 ? snap : null;
  const payload = base ? {
    type: 'snapshot', watermark: base.watermark, lastSeq: room.seq,
    snapshot: base.snapshot,
    envelopes: room.log.filter((e) => e.seq > base.seq),
    blobs: base.blobs
  } : {
    type: 'snapshot', watermark: room.watermark || 0, lastSeq: room.seq,
    snapshot: room.doc.snapshot(room.knownVC),
    envelopes: [],
    blobs: blobRefsForClient(room, null)
  };
  // 重置后的第一条帧由新 meter 计 fseq，客户端 ACK 后恢复正常扇出
  const bytes = WBT.encode(payload);
  payload.fseq = client.meter.reserve(bytes.length);
  try {
    client.ws.send(WBT.encode(payload));
  } catch (_) {}
  client.qlen = 1;
  console.log(`[degrade] sid=${client.sessionId} user=${client.userId} reason=${reason} -> full snapshot @seq=${room.seq}`);
}

function sendRoster(room) {
  const sids = [];
  for (const c of room.clients) if (c.bin && c.sessionId) sids.push(String(c.sessionId));
  for (const c of room.clients) {
    if (c.bin && c.sessionId) {
      sendBin(c, { type: 'peers', peers: sids.filter((s) => s !== String(c.sessionId)) });
    }
  }
}

/* ---------------------- v3：大对象 blob 引用登记 ---------------------- */

/** 大信封只在 WS 控制面登记引用（hash/size/持有者），字节走 P2P */
function handleBigAnnounce(client, room, refs) {
  if (!Array.isArray(refs)) return;
  const fresh = [];
  for (const ref of refs) {
    if (!ref || !/^[a-f0-9]{8,128}$/.test(ref.blobId || '')) continue;
    const exist = room.blobRefs.get(ref.blobId);
    if (exist) {
      for (const h of (ref.holders || [])) if (!exist.holders.includes(h)) exist.holders.push(h);
      continue;
    }
    const stored = {
      blobId: ref.blobId,
      size: Math.min(ref.size | 0, 64 * 1024 * 1024),
      chunksTotal: ref.chunksTotal | 0,
      hash: ref.hash || ref.blobId,
      holders: Array.from(new Set([client.sessionId | 0, ...((ref.holders || []).map((h) => h | 0))])),
      id: String(ref.id || '').slice(0, 100),
      clientId: String(ref.clientId || '').slice(0, 64),
      lamport: ref.lamport | 0,
      oids: Array.isArray(ref.oids) ? ref.oids.slice(0, 500).map(String) : [],
      kind: String(ref.kind || '')
    };
    room.blobRefs.set(ref.blobId, stored);
    // 只把新引用转发给其他成员（他们据此向持有者 P2P 请求分块）
    fresh.push({
      blobId: stored.blobId, size: stored.size, chunksTotal: stored.chunksTotal, hash: stored.hash,
      holders: stored.holders, id: stored.id, clientId: stored.clientId,
      lamport: stored.lamport, oids: stored.oids, kind: stored.kind
    });
  }
  if (fresh.length) {
    for (const other of room.clients) {
      if (other === client || !other.bin) continue;
      sendBin(other, { type: 'bigAnnounce', refs: fresh });
    }
  }
}
function rosterOf(room, self) {
  const out = [];
  for (const c of room.clients) if (c.bin && c.sessionId && c !== self) out.push(String(c.sessionId));
  return out;
}
function blobRefsForClient(room, self) {
  const out = [];
  for (const r of room.blobRefs.values()) {
    out.push({
      blobId: r.blobId, size: r.size, chunksTotal: r.chunksTotal, hash: r.hash,
      holders: r.holders.filter((h) => !self || String(h) !== String(self.sessionId)),
      id: r.id, clientId: r.clientId, lamport: r.lamport, oids: r.oids, kind: r.kind
    });
  }
  return out;
}

function makeSessionId() { return (crypto.randomBytes(4).readUInt32BE(0) & 0x3fffffff) + 1; }

function leaveRoom(client) {
  if (!client.roomId) return;
  const room = rooms.get(client.roomId);
  if (room) {
    room.clients.delete(client);
    // 通知其他成员该持有者离开
    for (const c of room.clients) {
      if (c.bin && c.sessionId) {
        sendBin(c, { type: 'peers', peers: rosterOf(room, c) });
      }
    }
  }
  client.roomId = null;
}

function handleOps(client, msg, rawSize) {
  if (!client.roomId) { sendJSON(client.ws, { type: 'error', message: 'join a room first' }); return; }
  if (rawSize > MAX_MSG_BYTES) { sendJSON(client.ws, { type: 'error', message: 'message too large' }); return; }
  const list = Array.isArray(msg.envelopes) ? msg.envelopes : null;
  if (!list || list.length === 0 || list.length > 1000) {
    sendJSON(client.ws, { type: 'error', message: 'envelopes required' });
    return;
  }
  if (!validateOpsList(client, list, (e) => sendJSON(client.ws, e))) return;

  const ids = ingestBatch(rooms.get(client.roomId), client, list);
  sendJSON(client.ws, { type: 'ack', ids, lastSeq: room_lastSeq(client.roomId) });
}

/** 信封列表的统一校验（v2 JSON 与 v3 二进制共用） */
function validateOpsList(client, list, errFn) {
  // 事务原子性：一批信封若声明同一 txnId，必须整组一起被接受/拒绝/广播
  const txnIds = new Set(list.map((e) => e && e.txnId).filter(Boolean));
  if (txnIds.size > 1) { errFn({ type: 'error', message: 'mixed txnId in one batch' }); return false; }
  for (const env of list) {
    const err = validateEnvelope(env);
    if (err) { errFn({ type: 'error', message: err, envId: env && env.id }); return false; }
    if (env.clientId !== client.userId) {
      errFn({ type: 'error', message: 'clientId mismatch', envId: env.id });
      return false;
    }
    // 粗粒度体积护栏：单笔点数上限
    const pts = env.op && env.op.objects && env.op.objects[0] &&
      env.op.objects[0].fields && env.op.objects[0].fields.stroke &&
      env.op.objects[0].fields.stroke.points;
    if (pts && pts.length > MAX_POINTS_PER_OP) {
      errFn({ type: 'error', message: 'too many points', envId: env.id });
      return false;
    }
  }
  return true;
}

/* --------------------------- v3 二进制消息分发 --------------------------- */

function handleBinMessage(client, raw) {
  if (!client.bin) {
    // 未协商成功就发二进制：拒绝
    try { client.ws.send(WBT.encode({ type: 'error', code: 'proto', message: 'binary before handshake' })); } catch (_) {}
    return;
  }
  if (raw.length > MAX_BIN_BYTES) {
    sendBin(client, { type: 'error', code: 'too-large', message: 'binary message too large' });
    return;
  }
  let msg;
  try { msg = WBT.frameFrom(raw); } catch (e) {
    sendBin(client, { type: 'error', code: 'bad-frame', message: String(e.message || e) });
    return;
  }
  if (msg.badMagic) { sendBin(client, { type: 'error', code: 'magic', message: 'bad magic' }); return; }
  if (msg.badVersion) {
    sendBin(client, { type: 'error', code: 'proto-major',
      message: 'incompatible major ' + msg.major, supported: [WBT.protoString()] });
    try { client.ws.close(WBT.PROTO.CLOSE_INCOMPATIBLE, 'incompatible major'); } catch (_) {}
    return;
  }
  if (!msg.type) return;

  switch (msg.type) {
    case 'join':
      handleBinJoin(client, msg);
      break;
    case 'ops': {
      if (!client.roomId) { sendBin(client, { type: 'error', code: 'no-room', message: 'join a room first' }); break; }
      const list = msg.envelopes || [];
      if (!list.length || list.length > 1000) {
        sendBin(client, { type: 'error', code: 'bad-ops', message: 'envelopes required' });
        break;
      }
      if (!validateOpsList(client, list, (e) => sendBin(client, { type: 'error', code: 'bad-env', message: e.message,
        supported: [WBT.protoString()] }))) break;
      const ids = ingestBatch(rooms.get(client.roomId), client, list);
      sendBin(client, { type: 'ack', ids, lastSeq: room_lastSeq(client.roomId), fseq: 0, sacks: [] });
      break;
    }
    case 'ack': {
      const wasDegraded = !!client.degraded;
      // fseq 扇出确认：释放在途字节；旧 ACK（fseq=0 或水位之前）忽略，防止序号倒退
      if (client.meter && (msg.fseq | 0) > client.meter.ackedFseq) {
        client.meter.ack(msg.fseq | 0);
      }
      if (wasDegraded && (msg.fseq | 0) >= client.meter.nextFseq - 1) {
        // 客户端确认收到降级快照 → 恢复正常广播
        client.degraded = false;
        client.qlen = 0;
        console.log(`[recover] sid=${client.sessionId} user=${client.userId} resumed normal fanout`);
      }
      break;
    }
    case 'signal': {
      // WebRTC 信令中继：只在同房间内转发（to=sessionId），不做任何解析
      const room = rooms.get(client.roomId);
      if (!room) break;
      const target = findBySession(room, msg.to);
      if (target && target.bin) {
        sendBin(target, { type: 'signal', from: String(client.sessionId), payload: msg.payload });
      }
      break;
    }
    case 'bigAnnounce': {
      const room = rooms.get(client.roomId);
      if (room) handleBigAnnounce(client, room, msg.refs);
      break;
    }
    case 'bigReq':
    case 'bigChunk':
    case 'bigAck':
      // 大对象字节永远走 P2P DataChannel；经 WS 到达一律忽略（控制/数据分离）
      break;
    case 'resume': {
      const room = rooms.get(client.roomId);
      if (room) sendSyncState(client, room, msg.lastSeq | 0, msg.vc || {});
      break;
    }
    case 'ping':
      sendBin(client, { type: 'pong', t: msg.t });
      break;
    case 'rate':
      // 客户端上报的窗口带宽（可用于更智能的降级阈值），仅记录
      client.lastRate = { windowMs: msg.windowMs, bytes: msg.bytes, msgs: msg.msgs, at: nowTs() };
      break;
    default:
      sendBin(client, { type: 'error', code: 'unknown', message: 'unknown frame' });
  }
}

function findBySession(room, sid) {
  const key = String(sid);
  for (const c of room.clients) if (c.bin && String(c.sessionId) === key) return c;
  return null;
}
function room_lastSeq(roomId) { const r = rooms.get(roomId); return r ? r.seq : 0; }

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
      binMembers: [...room.clients].filter((c) => c.bin).length,
      seq: room.seq,
      logLen: room.log.length,
      watermark: room.watermark,
      snapshots: room.snapshots.map((s) => s.seq),
      blobRefs: room.blobRefs.size,
      objectCount: snap.objects.length,
      liveCount: room.doc.liveObjects().length,
      knownVC: room.knownVC,
      degraded: [...room.clients].filter((c) => c.bin && c.degraded).map((c) => c.sessionId),
      log: room.log.map((e) => ({ seq: e.seq, id: e.id, clientId: e.clientId,
        lamport: e.lamport, kind: e.op && e.op.kind, txnId: e.txnId || null,
        squashKey: e.squashKey || null }))
    }));
    return;
  }

  // 手动触发一次日志压缩（测试/运维用）：squashKey 相同的连续操作折叠为最终状态
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

  if (url.pathname === '/api/rooms') {
    const summary = [];
    for (const [roomId, room] of rooms) {
      summary.push({
        roomId, members: room.clients.size, seq: room.seq,
        binMembers: [...room.clients].filter((c) => c.bin).length,
        logLen: room.log.length, watermark: room.watermark,
        snapshots: room.snapshots.length, blobRefs: room.blobRefs.size,
        liveCount: room.doc.liveObjects().length
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify(summary));
    return;
  }

  // v3：手动触发一次周期快照（测试/运维）
  if (url.pathname === '/api/snapshot') {
    const room = rooms.get(url.searchParams.get('roomId'));
    if (!room) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
    room.opsSinceSnapshot = SNAPSHOT_EVERY;
    maybeSnapshot(room);
    const last = room.snapshots[room.snapshots.length - 1];
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({
      seq: last.seq, objs: last.snapshot.objects.length, blobs: last.blobs.length
    }));
    return;
  }

  // v3：把指定二进制客户端标记为慢客户端（测试用：验证降级→快照同步）
  if (url.pathname === '/api/degrade') {
    const room = rooms.get(url.searchParams.get('roomId'));
    if (!room) { res.writeHead(404).end(JSON.stringify({ error: 'room not found' })); return; }
    const sid = parseInt(url.searchParams.get('sid') || '0', 10);
    let target = null;
    for (const c of room.clients) if (c.bin && (!sid || c.sessionId === sid)) { target = c; break; }
    if (!target) { res.writeHead(404).end(JSON.stringify({ error: 'bin client not found' })); return; }
    degradeClient(target, room, 2);
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({
      degraded: target.sessionId
    }));
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

/* --------------------------- WebSocket --------------------------- */

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws) => {
  const client = {
    id: crypto.randomBytes(6).toString('hex'),
    ws, userId: null, roomId: null, lastSeen: nowTs(), alive: true,
    bin: false, protoMinor: WBT.PROTO.MINOR, caps: 0, sessionId: 0,
    meter: null, degraded: false, qlen: 0
  };

  ws.on('pong', () => { client.lastSeen = nowTs(); client.alive = true; });

  ws.on('message', (raw, isBinary) => {
    client.lastSeen = nowTs(); client.alive = true;

    // v3 二进制帧（Buffer / ArrayBuffer）走独立分发；文本帧保持 v2 JSON
    const frameIsBin = typeof isBinary === 'boolean'
      ? isBinary
      : (raw instanceof ArrayBuffer || (Buffer.isBuffer(raw) && looksBinary(raw)));
    if (frameIsBin) {
      handleBinMessage(client, WBT.toU8(raw));
      return;
    }

    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch (_) { sendJSON(ws, { type: 'error', message: 'invalid json' }); return; }

    switch (msg.type) {
      case 'join': handleJoin(client, msg); break;
      case 'ops': handleOps(client, msg, raw.length); break;
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

/** ws 8.x 不保证 isBinary 时的兜底：以二进制魔数区分帧 */
function looksBinary(buf) {
  return buf.length >= 2 && buf[0] === 0x57 && buf[1] === 0x42; // 'W''B'
}

const heartbeatTimer = setInterval(() => {
  const now = nowTs();
  for (const room of rooms.values()) {
    for (const client of room.clients) {
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
  console.log('==============================================');
  console.log('  Collaborative Whiteboard v2 (CRDT kernel)');
  console.log(`  HTTP : http://localhost:${PORT}/`);
  console.log(`  WS   : ws://<host>:${PORT}/ws`);
  console.log(`  API  : http://localhost:${PORT}/api/rooms`);
  console.log('==============================================');
});

process.on('uncaughtException', (err) => console.error('[uncaughtException]', err));
