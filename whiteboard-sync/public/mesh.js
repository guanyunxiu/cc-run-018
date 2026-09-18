'use strict';

/* ===========================================================================
 * 协作白板 v3 - P2P 网状网络 + 网络客户端（浏览器 / Node 测试共享）
 *
 * 数据面分工：
 *  - WebSocket 只跑信令/控制：join/snapshot/delta/ack/signal/peers/announce，
 *    帧小、数量少。
 *  - 大操作与媒体（笔迹/图片等编码后超过 LARGE_BYTES 的信封）走 WebRTC
 *    DataChannel 直连 P2P：服务端只登记 blob 引用（hash/size/持有者），
 *    字节流不经过服务器。
 *
 * 可靠性：DataChannel 使用 unordered + 0 重传（SCTP 不做额外保证），
 * 上层 ReliableLink 负责序号/ACK/SACK/NACK/重传/乱序重排/重复丢弃，
 * 大对象再由 BlobAssembler 分块重组（块号去重）。
 *
 * PeerSession：链路建立后互发 peerHello(版本+VC)，双方按版本向量补齐
 * 对方缺失的信封（增量 gossip）；晚加入者还能按 bigAnnounce 的引用
 * 向持有者请求 blob 分块。
 *
 * Mesh（浏览器）：RTCPeerConnection 全互联，offer/answer/candidate
 * 全部经 WS signal 帧中继；按 sessionId 字典序由小端发起，避免 glare。
 * Node 无 WebRTC，测试用 VirtualNetwork + PeerSession 直连同一套逻辑。
 *
 * NetClient：应用唯一入口。二进制 WS 控制面 + P2P 数据面、发送 FIFO
 * 背压（高水位时 app 切换“手势预览”降级，只在松手时提交一帧）、
 * 离线重放、fseq 接收序号校验。
 *
 * UMD：Node(require) / 浏览器(<script>) 均可加载。依赖 transport.js(WBT)。
 * ========================================================================= */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory(require('./transport.js'));
  } else root.WBN = factory(root.WBT);
})(typeof self !== 'undefined' ? self : this, function (WBT) {
  'use strict';

  const RETAINED_OPS_LIMIT = 4000;        // 每客户端内存保留的信封（供 P2P 补缺）
  const BLOB_CACHE_LIMIT = 64;            // 本地缓存的 blob 个数（LRU）
  const DC_CHUNK = 8 * 1024;              // DataChannel 内块大小（SCTP MTU 安全值）
  const SESSION_TICK_MS = 100;
  const LINK_INFLIGHT_CAP = 384 * 1024;   // 单 peer 在途字节上限（背压）

  const nowMs = () => Date.now();

  function _g() { return typeof globalThis !== 'undefined' ? globalThis
    : (typeof self !== 'undefined' ? self : (typeof global !== 'undefined' ? global : {})); }

  /* ============================== 工具 ============================== */

  function hex(buf) {
    const u8 = WBT.toU8(buf);
    let s = '';
    for (let i = 0; i < u8.length; i++) s += (0x100 | u8[i]).toString(16).slice(1);
    return s;
  }

  async function sha256(bytes) {
    const u8 = WBT.toU8(bytes);
    try {
      const g = (typeof globalThis !== 'undefined' && globalThis.crypto) || null;
      if (g && g.subtle && g.subtle.digest) {
        const d = await g.subtle.digest('SHA-256', u8);
        return hex(d);
      }
    } catch (_) { /* fall through */ }
    return WBT.sha256Hex(u8); // Node crypto
  }

  /* ============================== P2P 会话 ============================== */

  /**
   * 一条与对端的可靠数据通道（底层可以是 WebRTC DataChannel 或
   * VirtualTransport）。DATA 帧保序投递后，payload 是内部控制帧：
   *   peerHello / ops / bigChunk / bigReq / bigAck
   */
  class PeerSession {
    /**
     * @param {object} o
     * @param {string} o.me        本端 sessionId
     * @param {string} o.peer      对端 sessionId
     * @param {(bytes:Uint8Array)=>void} o.send  底层不可靠/无序发送
     * @param {object} o.handlers
     *   getVC():object                        本端当前版本向量
     *   getRetainedLog():env[]                可用于补缺的信封
     *   ingestEnvelopes(envs, peerId):void
     *   getBlob(blobId):Uint8Array|null
     *   saveBlob(blobId, data, meta):void
     *   needBlobRefs?():ref[]                 本端缺失 blob 的引用（供对端主动推/告知）
     */
    constructor(o) {
      this.me = o.me;
      this.peer = o.peer;
      this._rawSend = o.send;
      this.h = o.handlers;
      this.link = new WBT.ReliableLink({
        send: (b) => this._rawSend(b),
        now: o.now || nowMs,
        maxPending: 2048,
        rto: o.rto || 150
      });
      this.helloSent = false;
      this.helloReceived = false;
      this.peerVC = null;
      this._blobQueues = new Map();   // blobId -> {frames:Uint8Array[], inflight:number}
      this.bytesSent = 0;
      this.bytesReceived = 0;
      this.lastActive = nowMs();
    }

    start() { this._sendHello(); }

    _sendHello() {
      if (this.helloSent) return;
      this.helloSent = true;
      this._ctrl(WBT.encode({
        type: 'peerHello',
        major: WBT.PROTO.MAJOR, minor: WBT.PROTO.MINOR,
        caps: WBT.PROTO.CAPS.BIN | WBT.PROTO.CAPS.DC,
        userId: this.h.userId || this.me,
        vc: this.h.getVC()
      }));
    }

    /** 底层收到一个完整（可能乱序的）二进制帧（data/dataAck/dataNack） */
    onFrame(bytes) {
      const msgs = Array.isArray(bytes) ? bytes : [bytes];
      for (const b of msgs) {
        const msg = WBT.frameFrom(b);
        if (msg.badMagic || msg.badVersion) continue;
        const delivered = this.link.handle(msg);
        for (const payload of delivered) this._onPayload(payload);
        const ack = this.link.buildAck();
        if (ack) this._rawSend(ack);
        const nack = this.link.takeNack();
        if (nack) this._rawSend(nack);
      }
    }

    _ctrl(frameBytes) {
      // 控制帧也走可靠序列层：有序、不丢、不重
      if (!this.link.send(frameBytes)) {
        // 窗口满（背压极端情况）：调用方 tick 后重试 —— 这里同步重排队
        this._pendingCtrl = this._pendingCtrl || [];
        this._pendingCtrl.push(frameBytes);
      }
    }

    _onPayload(payload) {
      this.lastActive = nowMs();
      this.bytesReceived += payload.length;
      const msg = WBT.frameFrom(payload);
      if (msg.badMagic || msg.badVersion) return;
      switch (msg.type) {
        case 'peerHello': this._onHello(msg); break;
        case 'ops':
          if (Array.isArray(msg.envelopes) && msg.envelopes.length) {
            this.h.ingestEnvelopes(msg.envelopes, this.peer);
          }
          break;
        case 'bigChunk': this._onChunk(msg); break;
        case 'bigReq': this._onBigReq(msg); break;
        case 'bigAck': {
          const q = this._blobQueues.get(msg.blobId);
          if (q && msg.contiguous >= q.total) this._blobQueues.delete(msg.blobId);
          break;
        }
        default: break; // 未知类型忽略（向前兼容）
      }
    }

    _onHello(msg) {
      const neg = WBT.negotiate(WBT.PROTO.NAME, msg.major, msg.minor);
      if (!neg.ok) return; // 大版本不一致：不建立数据同步
      this.helloReceived = true;
      this.peerVC = msg.vc || {};
      if (!this.helloSent) this._sendHello();
      this._syncMissingToPeer();
    }

    /** 按对端 VC 增量推送其缺失信封（大信封若本地已无字节则跳过） */
    _syncMissingToPeer() {
      if (!this.helloReceived) return;
      const log = this.h.getRetainedLog() || [];
      const missing = WBT.missingForPeer(log, this.peerVC);
      const BATCH = 64;
      for (let i = 0; i < missing.length; i += BATCH) {
        const batch = missing.slice(i, i + BATCH).filter((e) => !e.__blobOnly || this.h.getBlob(e.__blobId));
        if (batch.length) this.sendOps(batch);
      }
    }

    /** 本端 VC 推进后可周期性调用，把新信封增量推给对端 */
    pump() {
      if (!this.helloReceived) { this._sendHello(); return; }
      this._syncMissingToPeer();
    }

    sendOps(envelopes) {
      this._ctrl(WBT.encode({ type: 'ops', envelopes }));
    }

    /** 发送一个 blob（分块，窗口背压；tick 续传） */
    sendBlob(blobId, data, opts) {
      opts = opts || {};
      const fromChunk = opts.fromChunk || 0;
      const { total, frames } = WBT.chunkBlob(blobId, data, DC_CHUNK);
      const q = this._blobQueues.get(blobId) || { frames: [], total, inflight: 0 };
      q.total = total;
      q.frames = frames.slice(fromChunk);
      this._blobQueues.set(blobId, q);
      this._flushBlobs();
    }

    _onBigReq(msg) {
      const data = this.h.getBlob(msg.blobId);
      if (data) this.sendBlob(msg.blobId, data, { fromChunk: msg.fromChunk | 0 });
    }

    _onChunk(msg) {
      if (!this.assembler) this.assembler = new WBT.BlobAssembler();
      const done = this.assembler.add(msg);
      if (done) {
        this._ctrl(WBT.encode({ type: 'bigAck', blobId: done.blobId, contiguous: done.total }));
        this.h.saveBlob(done.blobId, done.data, null);
        // 重组出的就是自描述 ops 帧
        const f = WBT.frameFrom(done.data);
        if (f.type === 'ops' && Array.isArray(f.envelopes)) {
          for (const e of f.envelopes) e.__blobId = done.blobId;
          this.h.ingestEnvelopes(f.envelopes, this.peer);
        }
      }
    }

    requestBlob(blobId, fromChunk) {
      this._ctrl(WBT.encode({ type: 'bigReq', blobId, fromChunk: fromChunk | 0 }));
    }

    _flushBlobs() {
      for (const [blobId, q] of this._blobQueues) {
        while (q.frames.length && this.link.available &&
               this.link.pendingBytes < LINK_INFLIGHT_CAP) {
          const frame = q.frames.shift();
          if (!this.link.send(frame)) { q.frames.unshift(frame); break; }
          this.bytesSent += frame.length;
        }
      }
    }

    /** 周期维护：DATA 重传、NACK、blob 续推 */
    tick(now) {
      this.link.tick(now || nowMs());
      const nack = this.link.takeNack();
      if (nack) this._rawSend(nack);
      // 控制帧窗口恢复后续发
      if (this._pendingCtrl && this._pendingCtrl.length) {
        const rest = [];
        for (const f of this._pendingCtrl) if (!this.link.send(f)) rest.push(f);
        this._pendingCtrl = rest;
      }
      this._flushBlobs();
    }
  }

  /* ============================== 浏览器 WebRTC Mesh ============================== */

  /**
   * 全互联 Mesh：
   *  - roster 来自服务端 peers 帧（成员是 sessionId）；
   *  - 字典序小端发起 offer（双方同时收到 roster 也只有一侧发，无 glare）；
   *  - SDP / ICE candidate 全部经 WS signal 中继；
   *  - 每条 DC 一个 PeerSession。
   */
  class Mesh {
    /**
     * @param {object} o
     * @param {string} o.sessionId 本端 sessionId
     * @param {(to:string,payload:Uint8Array)=>void} o.sendSignal
     * @param {(session:PeerSession, peerId:string)=>void} o.onSession
     * @param {object} o.handlers PeerSession handlers
     * @param {object} [o.RTC] 注入 RTCPeerConnection（测试）
     */
    constructor(o) {
      this.sessionId = String(o.sessionId);
      this.sendSignal = o.sendSignal;
      this.onSession = o.onSession || function () {};
      this.handlers = o.handlers;
      this.RTC = o.RTC || (typeof globalThis !== 'undefined' ? globalThis.RTCPeerConnection : null);
      this.peers = new Map();   // sessionId -> {pc, dc, session}
      this.roster = new Set();
      this._ticks = [];
      this._timer = setInterval(() => this.tick(), SESSION_TICK_MS);
    }

    get size() { return this.peers.size; }

    /** 服务端 roster 更新 */
    setRoster(list) {
      this.roster = new Set(list.map(String));
      for (const id of this.roster) {
        if (id === this.sessionId || this.peers.has(id)) continue;
        this._connect(id);
      }
      // 已离房成员清理
      for (const [id, p] of this.peers) {
        if (!this.roster.has(id)) this._close(id, p);
      }
    }

    _shouldInitiate(peerId) { return this.sessionId < String(peerId); }

    _connect(peerId) {
      const entry = { pc: null, dc: null, session: null, initiated: this._shouldInitiate(peerId) };
      this.peers.set(peerId, entry);
      if (!this.RTC) {
        // 无 WebRTC（Node 测试）：只占位，测试自行注入 VirtualTransport 会话
        return;
      }
      const pc = new this.RTC({ iceServers: [] });
      entry.pc = pc;
      pc.onicecandidate = (ev) => {
        if (ev.candidate) this._signal(peerId, { type: 'candidate', candidate: ev.candidate });
      };
      pc.ondatachannel = (ev) => this._bindDC(peerId, entry, ev.channel);
      if (entry.initiated) {
        const dc = pc.createDataChannel('wb3', { ordered: false, maxRetransmits: 0 });
        this._bindDC(peerId, entry, dc);
        pc.createOffer().then((offer) => pc.setLocalDescription(offer))
          .then(() => this._signal(peerId, { type: 'offer', sdp: pc.localDescription }))
          .catch((e) => console.warn('[mesh offer]', e));
      }
    }

    _bindDC(peerId, entry, dc) {
      dc.binaryType = 'arraybuffer';
      entry.dc = dc;
      const session = new PeerSession(Object.assign({
        me: this.sessionId, peer: peerId,
        send: (bytes) => { if (dc.readyState === 'open') dc.send(bytes); }
      }, { handlers: this.handlers }));
      entry.session = session;
      dc.onopen = () => {
        session.start();
        this.onSession(session, peerId);
      };
      dc.onmessage = (ev) => session.onFrame(WBT.toU8(ev.data));
      dc.onclose = () => { /* roster 更新时清理 */ };
      dc.onerror = () => { try { dc.close(); } catch (_) {} };
      // bufferedAmount 低水位：配合发送队列背压
      try { dc.bufferedAmountLowThreshold = 256 * 1024; } catch (_) {}
    }

    _signal(peerId, obj) {
      this.sendSignal(String(peerId), new TextEncoder().encode(JSON.stringify(obj)));
    }

    /** 收到 WS 中继来的信令 */
    onSignal(from, payload) {
      const peerId = String(from);
      let msg;
      try { msg = JSON.parse(new TextDecoder().decode(WBT.toU8(payload))); } catch (_) { return; }
      let entry = this.peers.get(peerId);
      if (!entry) {
        // 对端（字典序更小）先发起：被动建链
        this.roster.add(peerId);
        this._connect(peerId);
        entry = this.peers.get(peerId);
      }
      const pc = entry.pc;
      if (!pc || !this.RTC) return;
      if (msg.type === 'offer') {
        pc.setRemoteDescription(new this.RTC.SessionDescription(msg.sdp))
          .then(() => pc.createAnswer())
          .then((answer) => pc.setLocalDescription(answer))
          .then(() => this._signal(peerId, { type: 'answer', sdp: pc.localDescription }))
          .catch((e) => console.warn('[mesh answer]', e));
      } else if (msg.type === 'answer') {
        pc.setRemoteDescription(new this.RTC.SessionDescription(msg.sdp)).catch(() => {});
      } else if (msg.type === 'candidate') {
        pc.addIceCandidate(new this.RTC.IceCandidate(msg.candidate)).catch(() => {});
      }
    }

    /** 测试注入：用外部 PeerSession（VirtualTransport）替换占位 */
    attachSession(peerId, session) {
      let entry = this.peers.get(peerId);
      if (!entry) { entry = { pc: null, dc: null, session: null }; this.peers.set(peerId, entry); }
      entry.session = session;
      this.onSession(session, peerId);
    }

    getSession(peerId) {
      const e = this.peers.get(peerId);
      return e ? e.session : null;
    }
    sessions() {
      const out = [];
      for (const e of this.peers.values()) if (e.session) out.push(e.session);
      return out;
    }

    tick() {
      const now = nowMs();
      for (const e of this.peers.values()) if (e.session) e.session.tick(now);
    }

    _close(peerId, entry) {
      this.peers.delete(peerId);
      try { entry.dc && entry.dc.close(); } catch (_) {}
      try { entry.pc && entry.pc.close(); } catch (_) {}
    }
    close() { clearInterval(this._timer); for (const [id, e] of this.peers) this._close(id, e); }
  }

  /* ============================== 网络客户端 ============================== */

  /**
   * 应用直接使用的门面：
   *   const net = new NetClient({userId, handlers:{...}});
   *   await net.connect(wsUrl); net.join(roomId, {lastSeq, vc});
   *   net.sendEnvelopes([...]);   // 自动小→WS / 大→P2P
   *
   * 接收回调（handlers）：
   *   onSnapshot(msg) / onDelta(msg) / onOps(envelopes, from) /
   *   onAck(ids, lastSeq) / onDegraded(reason) / onSession(peerCount) /
   *   onState(state) / onError(code, message)
   */
  class NetClient {
    constructor(o) {
      this.userId = o.userId;
      this.handlers = o.handlers || {};
      this.WS = o.WS || (typeof globalThis !== 'undefined' ? globalThis.WebSocket : null);
      this.offline = o.offline || null;
      this.now = o.now || nowMs;
      this.enableMesh = o.enableMesh !== false;
      this.allowWsBig = o.allowWsBig !== false;
      this.bigWaitMs = o.bigWaitMs || 1500;

      this.ws = null;
      this.sessionId = 0;
      this.roomId = null;
      this.lastSeq = 0;
      this.proto = null;
      this.connected = false;

      // 发送 FIFO（真实、已发时钟的信封帧，背压时禁止生产而不是丢帧）
      this.sendQueue = [];
      this.sendBytes = 0;
      this.highBytes = (o.highBytes) || 512 * 1024;
      this.lowBytes = Math.floor(this.highBytes / 2);
      this.pressure = false;

      // 服务端扇出帧的接收序号校验（fseq）
      this.recvFseq = 0;
      this.ackFseq = 0;

      // P2P
      this.mesh = null;
      this.blobStore = new WBT.BlobStore();
      this.refs = new Map();          // blobId -> ref（含 holders）
      this.retainedLog = [];
      this.retainedIds = new Set();
      this.clockVC = o.clockVC || (() => ({}));
      this.clockMerge = o.clockMerge || function () {};
      this.blobLru = [];

      this._tickTimer = null;
      this._pumpTimer = null;

      if (o.ws) this.attachSocket(o.ws);
    }

    /* ---------------------------- 连接 / 握手 ---------------------------- */

    connect(url) {
      return new Promise((resolve, reject) => {
        const ws = new this.WS(url);
        ws.binaryType = 'arraybuffer';
        this.attachSocket(ws);
        ws.onopen = () => resolve();
        ws.onerror = (e) => reject(e);
      });
    }

    /** 绑定一个已打开（可能已完成文本握手）的 socket（浏览器 app 复用协商连接） */
    attachSocket(ws) {
      this.ws = ws;
      ws.binaryType = 'arraybuffer';
      ws.onmessage = (ev) => this._onWsMessage(ev.data);
      const prevOnClose = ws.onclose;
      ws.onclose = () => {
        this.connected = false;
        this._stopTimers();
        if (this.handlers.onState) this.handlers.onState('closed');
        if (typeof prevOnClose === 'function') try { prevOnClose(); } catch (_) {}
      };
    }

    /** 发送 JSON join（带协议协商块）；服务端 joined 决定是否切二进制 */
    requestJoin(roomId) {
      this.roomId = roomId;
      this._sendJSON({
        type: 'join', roomId, userId: this.userId, lastSeq: this.lastSeq,
        proto: { name: WBT.PROTO.NAME, major: WBT.PROTO.MAJOR, minor: WBT.PROTO.MINOR,
          caps: this._caps() }
      });
    }

    /** 由应用在收到文本 joined 后调用：proto 存在即升级到 v3 二进制模式 */
    upgrade(joined) {
      const p = joined.proto;
      if (!p || !WBT.negotiate(p.name || WBT.PROTO.NAME, p.major, p.minor).ok) return false;
      this.proto = p;
      this.sessionId = joined.sessionId | 0;
      this.lastSeq = joined.lastSeq | 0;
      // 二进制 join：携带 lastSeq + VC，服务端决定 snapshot / delta
      this.sendFrame({ type: 'join', roomId: this.roomId, userId: this.userId,
        lastSeq: this.lastSeq, vc: this.clockVC() });
      this.connected = true;
      this._startTimers();
      if (this.enableMesh) this._initMesh();
      if (this.handlers.onState) this.handlers.onState('binary');
      return true;
    }

    _caps() {
      let caps = WBT.PROTO.CAPS.BIN | WBT.PROTO.CAPS.SNAP3 | WBT.PROTO.CAPS.BLOB;
      if (this.enableMesh && (typeof globalThis !== 'undefined' && globalThis.RTCPeerConnection)) {
        caps |= WBT.PROTO.CAPS.DC;
      }
      return caps;
    }

    _initMesh() {
      this.mesh = new Mesh({
        sessionId: this.sessionId,
        sendSignal: (to, payload) => this.sendFrame({ type: 'signal', to, payload }),
        handlers: this._peerHandlers(),
        onSession: () => {
          if (this.handlers.onSession) this.handlers.onSession(this.mesh.size);
          // 有 P2P 对端了：把等待大对象的任务发出去
          this._flushBigWaiting();
        }
      });
    }

    /* ---------------------------- WS 收发 ---------------------------- */

    _sendJSON(obj) {
      if (this.ws && this.ws.readyState === 1) this.ws.send(JSON.stringify(obj));
    }

    sendFrame(msg) {
      const bytes = WBT.encode(msg);
      this._wsSend(bytes);
      return bytes.length;
    }

    _wsSend(bytes) {
      if (!this.ws || this.ws.readyState !== 1) {
        this.sendQueue.push({ bytes });
        this.sendBytes += bytes.length;
        return;
      }
      // 背压：内核缓冲已高 → 入队等 bufferedAmount 回落
      const buffered = this.ws.bufferedAmount | 0;
      if (buffered + bytes.length > this.highBytes || this.sendQueue.length) {
        this.sendQueue.push({ bytes });
        this.sendBytes += bytes.length;
        this._updatePressure();
      } else {
        try { this.ws.send(bytes); } catch (_) { this.sendQueue.push({ bytes }); this.sendBytes += bytes.length; }
      }
      this._drainQueue(false);
    }

    _drainQueue(force) {
      while (this.sendQueue.length && this.ws && this.ws.readyState === 1) {
        const buffered = this.ws.bufferedAmount | 0;
        if (!force && buffered > this.lowBytes) break;
        const job = this.sendQueue.shift();
        this.sendBytes -= job.bytes.length;
        try { this.ws.send(job.bytes); } catch (_) { this.sendQueue.unshift(job); this.sendBytes += job.bytes.length; break; }
      }
      this._updatePressure();
    }

    _updatePressure() {
      const buffered = (this.ws && this.ws.bufferedAmount) | 0;
      const high = this.sendBytes + buffered > this.highBytes;
      if (high && !this.pressure) {
        this.pressure = true;
        if (this.handlers.onPressure) this.handlers.onPressure(true);
      } else if (!high && this.pressure && this.sendBytes + buffered < this.lowBytes) {
        this.pressure = false;
        if (this.handlers.onPressure) this.handlers.onPressure(false);
      }
    }

    _onWsMessage(data) {
      // 文本帧只可能是握手期 joined / error（v3 下）
      if (typeof data === 'string') {
        let msg; try { msg = JSON.parse(data); } catch (_) { return; }
        if (msg.type === 'joined') this.upgrade(msg);
        else if (msg.type === 'error' && this.handlers.onError) this.handlers.onError(msg.code || '', msg.message);
        return;
      }
      const msg = WBT.frameFrom(data);
      if (msg.badMagic) { if (this.handlers.onError) this.handlers.onError('magic', 'bad magic'); return; }
      if (msg.badVersion) {
        if (this.handlers.onError) this.handlers.onError('version', 'unsupported major ' + msg.major);
        return;
      }
      this._onBinary(msg);
    }

    _onBinary(msg) {
      switch (msg.type) {
        case 'joined':
          // 服务端也可能直接二进制 joined（未来路径）
          this.sessionId = msg.sessionId;
          this.connected = true;
          break;
        case 'snapshot':
          this._onSnapshot(msg);
          this._recvFseq(msg);
          break;
        case 'delta':
          if (this._staleFseq(msg)) break;
          this.lastSeq = Math.max(this.lastSeq, msg.fromSeq + (msg.envelopes || []).length);
          if (this.handlers.onDelta) this.handlers.onDelta(msg);
          this._retain(msg.envelopes);
          this._recvFseq(msg);
          break;
        case 'ops':
          if (this._staleFseq(msg)) break;
          this._retain(msg.envelopes);
          if (this.handlers.onOps) this.handlers.onOps(msg.envelopes || [], 'server');
          this._recvFseq(msg);
          break;
        case 'ack':
          if (this.handlers.onAck) this.handlers.onAck(msg.ids || [], msg.lastSeq | 0);
          if (msg.fseq) this.ackFseq = Math.max(this.ackFseq, msg.fseq);
          break;
        case 'pong': break;
        case 'signal':
          if (this.mesh) this.mesh.onSignal(msg.from, msg.payload);
          break;
        case 'peers':
          if (this.mesh) this.mesh.setRoster(msg.peers);
          if (this.handlers.onPeers) this.handlers.onPeers(msg.peers);
          break;
        case 'bigAnnounce':
          this._onBigAnnounce(msg.refs || []);
          break;
        case 'degrade':
          if (this.handlers.onDegraded) this.handlers.onDegraded(msg.reason, msg.seq);
          break;
        case 'error':
          if (this.handlers.onError) this.handlers.onError(msg.code, msg.message);
          break;
        default: break;
      }
    }

    /* ----------------------- 服务端扇出 fseq 序号校验 ----------------------- */

    _staleFseq(msg) {
      if (msg.fseq == null) return false;
      if (msg.fseq <= this.recvFseq) return true; // 旧帧/重复帧：丢弃，绝不用旧状态覆盖
      return false;
    }
    _recvFseq(msg) {
      if (msg.fseq != null && msg.fseq > this.recvFseq) {
        this.recvFseq = msg.fseq;
        // 捎带 ACK：已连续收到的水位 + 最新 envelope ack
        this.sendFrame({ type: 'ack', ids: [], lastSeq: this.lastSeq, fseq: this.recvFseq, sacks: [] });
      }
    }

    /* ---------------------------- 快照 / 增量 ---------------------------- */

    _onSnapshot(msg) {
      this.lastSeq = msg.lastSeq | 0;
      // 基线切换：P2P 保留日志只留快照水位之后的
      this.retainedLog = (msg.envelopes || []).slice();
      this.retainedIds = new Set(this.retainedLog.map((e) => e.id));
      if (this.handlers.onSnapshot) this.handlers.onSnapshot(msg);
      // 快照引用的大对象：向持有者 P2P 拉取
      this._onBigAnnounce(msg.blobs || []);
    }

    _retain(envelopes) {
      if (!envelopes) return;
      for (const e of envelopes) {
        if (this.retainedIds.has(e.id)) continue;
        this.retainedIds.add(e.id);
        this.retainedLog.push(e);
      }
      if (this.retainedLog.length > RETAINED_OPS_LIMIT) {
        const cut = this.retainedLog.length - RETAINED_OPS_LIMIT;
        const removed = this.retainedLog.splice(0, cut);
        for (const e of removed) this.retainedIds.delete(e.id);
      }
    }

    /* ---------------------------- 发送信封 ---------------------------- */

    /**
     * 提交一批信封（同一事务）。
     * 小信封走 WS 二进制 ops；大信封逐个走 P2P blob（WS 只发引用）。
     * @returns {{small:number, big:string[]}} 路由结果（big 为 blobId 列表）
     */
    async sendEnvelopes(envelopes) {
      const list = Array.isArray(envelopes) ? envelopes : [envelopes];
      const small = [], big = [];
      for (const e of list) {
        const n = WBT.encode({ type: 'ops', envelopes: [e] }).length;
        if (n > WBT.LARGE_BYTES) big.push(e); else small.push(e);
      }
      const bigIds = [];
      if (small.length) {
        const bytes = this.sendFrame({ type: 'ops', envelopes: small });
        this._updatePressureBytes(bytes);
        if (this.offline) {
          for (const e of small) this.offline.putPending(e).catch(() => {});
        }
      }
      for (const e of big) bigIds.push(await this._routeBig(e));
      this._retain(list);
      // 推给已连接 P2P 对端（gossip 加速，服务端也会广播小信封，对端幂等去重）
      this._pumpPeers();
      return { small: small.length, big: bigIds };
    }

    _updatePressureBytes() {
      const buffered = (this.ws && this.ws.bufferedAmount) | 0;
      const high = this.sendBytes + buffered > this.highBytes;
      if (high && !this.pressure) { this.pressure = true; this.handlers.onPressure && this.handlers.onPressure(true); }
    }

    /** 大信封：编码 → hash 分块 → WS 登记引用 → P2P 推块；无对端时超时降级走 WS */
    async _routeBig(env) {
      const data = WBT.encode({ type: 'ops', envelopes: [env] });
      const blobId = await sha256(data);
      const total = Math.max(1, Math.ceil(data.length / DC_CHUNK));
      this._cacheBlob(blobId, data);
      const ref = {
        blobId, size: data.length, chunksTotal: total, hash: blobId,
        holders: [this.sessionId],
        id: env.id, clientId: env.clientId, lamport: env.lamport, clock: env.clock,
        oids: _envOids(env), kind: env.op && env.op.kind
      };
      this.refs.set(blobId, ref);
      // 控制面：只登记引用（字节不过服务器）
      this.sendFrame({ type: 'bigAnnounce', refs: [ref] });

      const sessions = this.mesh ? this.mesh.sessions() : [];
      if (sessions.length) {
        for (const s of sessions) s.sendBlob(blobId, data);
      } else if (this.allowWsBig) {
        // 暂无 P2P 对端：短等 mesh；仍无人则整信封降级走 WS（服务器兜底，不丢编辑）
        const ok = await this._waitForPeers(this.bigWaitMs);
        if (ok) for (const s of this.mesh.sessions()) s.sendBlob(blobId, data);
        else {
          env.__wsFallback = true;
          this.sendFrame({ type: 'ops', envelopes: [env] });
        }
      }
      if (this.offline) this.offline.putPending(env).catch(() => {});
      return blobId;
    }

    _waitForPeers(ms) {
      return new Promise((resolve) => {
        const t0 = this.now();
        const iv = setInterval(() => {
          if (this.mesh && this.mesh.sessions().some((s) => s.helloReceived)) {
            clearInterval(iv); resolve(true);
          } else if (this.now() - t0 >= ms) { clearInterval(iv); resolve(false); }
        }, 80);
      });
    }

    _cacheBlob(blobId, data) {
      this.blobStore.put(blobId, data);
      this.blobLru.push(blobId);
      if (this.blobLru.length > BLOB_CACHE_LIMIT) {
        const old = this.blobLru.shift();
        if (old !== blobId) this.blobStore.delete(old);
      }
    }

    _flushBigWaiting() {
      for (const ref of this.refs.values()) {
        if (this.blobStore.has(ref.blobId)) {
          for (const s of this.mesh.sessions()) s.sendBlob(ref.blobId, this.blobStore.get(ref.blobId));
        }
      }
    }

    /* ---------------------------- 大对象引用 ---------------------------- */

    _onBigAnnounce(refs) {
      for (const ref of refs) {
        const exist = this.refs.get(ref.blobId);
        if (exist) {
          for (const h of (ref.holders || [])) if (!exist.holders.includes(h)) exist.holders.push(h);
          continue;
        }
        this.refs.set(ref.blobId, ref);
        // 已经有完整信封（P2P 推送先到）→ 无需请求
        if (this._haveEnvelope(ref.id)) continue;
        this._requestBlob(ref);
      }
    }

    _haveEnvelope(id) {
      return this.retainedIds.has(id) ||
        (this.handlers.hasEnvelope && this.handlers.hasEnvelope(id));
    }

    _requestBlob(ref) {
      if (!this.mesh) return;
      const tryReq = () => {
        if (this.blobStore.has(ref.blobId)) return true;
        const holderSession = (ref.holders || [])
          .map((h) => this.mesh.getSession(String(h))).find(Boolean);
        if (holderSession) { holderSession.requestBlob(ref.blobId, 0); return true; }
        return false;
      };
      if (!tryReq()) {
        let tries = 0;
        const iv = setInterval(() => {
          tries++;
          if (tryReq() || tries > 20) clearInterval(iv);
        }, 150);
      }
    }

    /* ---------------------------- P2P 回调 ---------------------------- */

    _peerHandlers() {
      return {
        userId: this.userId,
        getVC: () => this.clockVC(),
        getRetainedLog: () => this.retainedLog,
        getBlob: (id) => this.blobStore.get(id) || null,
        saveBlob: (id, data) => this._cacheBlob(id, data),
        hasEnvelope: (id) => this._haveEnvelope(id),
        ingestEnvelopes: (envs, peer) => {
          const fresh = envs.filter((e) => !this.retainedIds.has(e.id) &&
            !(this.handlers.hasEnvelope && this.handlers.hasEnvelope(e.id)));
          if (!fresh.length) return;
          this._retain(fresh);
          if (this.handlers.onOps) this.handlers.onOps(fresh, 'p2p:' + peer);
        }
      };
    }

    _pumpPeers() {
      if (!this.mesh) return;
      for (const s of this.mesh.sessions()) {
        s.peerVC = s.helloReceived ? s.peerVC : s.peerVC;
        s.pump();
      }
    }

    /** 应用每收到/发送信封后调用，推进 P2P 增量 gossip */
    notifyVC() { this._pumpPeers(); }

    /** 离线重放：未确认信封按原 id 重发（服务端/对端幂等去重） */
    async replayOffline(list) {
      if (!list || !list.length) return 0;
      let n = 0;
      // 保持时钟顺序分批
      for (const e of list) {
        const size = WBT.encode({ type: 'ops', envelopes: [e] }).length;
        if (size > WBT.LARGE_BYTES) await this._routeBig(e);
        else this.sendFrame({ type: 'ops', envelopes: [e] });
        n++;
      }
      return n;
    }

    /** 收到服务端 ack 后清理离线存储 */
    async confirmAck(ids) {
      if (this.offline && ids && ids.length) {
        try { await this.offline.removePending(ids); } catch (_) {}
      }
    }

    resume(lastSeq, vc) {
      this.lastSeq = lastSeq;
      this.sendFrame({ type: 'resume', lastSeq, vc: vc || this.clockVC() });
    }

    reportRate(windowMs, bytes, msgs) {
      this.sendFrame({ type: 'rate', windowMs, bytes, msgs });
    }

    /* ---------------------------- 心跳 / 维护 ---------------------------- */

    _startTimers() {
      this._stopTimers();
      this._tickTimer = setInterval(() => {
        const t = this.now();
        if (this.sendFrame) try { this.sendFrame({ type: 'ping', t: (t % 0xffffffff) | 0 }); } catch (_) {}
        this._drainQueue(false);
      }, 15000);
    }
    _stopTimers() {
      if (this._tickTimer) clearInterval(this._tickTimer);
      this._tickTimer = null;
    }
    close() {
      this._stopTimers();
      if (this.mesh) this.mesh.close();
      try { this.ws && this.ws.close(); } catch (_) {}
    }
  }

  function _envOids(env) {
    const op = env.op || {};
    if (op.kind === 'create') return (op.objects || []).map((o) => o.oid);
    if (op.oid) return [op.oid];
    return op.oids || [];
  }

  return { PeerSession, Mesh, NetClient, sha256, RETAINED_OPS_LIMIT, DC_CHUNK };
});
