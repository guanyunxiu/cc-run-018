'use strict';

/* ===========================================================================
 * 协作白板 v3 - 同步客户端（浏览器端）
 *
 * 在 kernel.js（CRDT）与 net.js/mesh.js（传输）之上提供一个完整门面：
 *
 *  连接与版本
 *   - 连接后先发二进制 HELLO 协商协议：major 不一致直接拒绝并提示；
 *     minor 高于服务端时按服务端能力降级（WELCOME.action=degrade）。
 *   - JOIN 携带 lastSeq + 本地版本向量；服务端 planSync 决定
 *     全量快照 / 基线快照+delta / 纯 delta（增量同步、快照加速）。
 *
 *  发送侧（背压 + 离线编辑）
 *   - 本地编辑乐观物化 → 信封写入 IndexedDB（未确认持久化）→ SendQueue；
 *     同 squashKey 的中间帧在队列中合并（连续移动只发最终态）。
 *   - Link 发送窗口满时自动 await drain；缓存超水位发 backpressure 事件，
 *     UI 可据此降采样/暂停采集。
 *   - 关页/断线后未确认信封留在 IndexedDB；重连 JOIN 成功后按 lamport 序合并重发，
 *     时钟（Lamport/local/VC）也持久化，跨刷新不会产生旧逻辑时间戳。
 *
 *  接收侧（序号校验 / 去重 / 乱序）
 *   - Link 层保证帧不丢不重不乱序；应用层再做两道校验：
 *       1) env.id 幂等（Doc.delivered / pending）；
 *       2) 服务端 seq 单调，回退 seq 丢弃，缺口触发 REQ_SYNC。
 *   - 同一信封可能从 P2P 与服务端中继各到一次：id 去重，先到者生效。
 *
 *  慢客户端
 *   - 服务端 Outbox 超水位下发 MODE=1（快照模式）：本端停止逐帧积压，
 *     待本地 Link 排空后发 REQ_SYNC{lastSeq,vc}，服务端补快照后 MODE=0 恢复。
 *
 *  大对象/媒体
 *   - 大操作与媒体优先 P2P mesh（fanout）；无在线 peer 时退回服务端链路；
 *   - 图片等媒体走 MediaSender/Receiver 分块，断线按 offset 续传。
 *
 * 事件：status, snapshot, ops, ack, mode, peers, backpressure, drain,
 *       media, protocol, error
 * =========================================================================== */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory(require('./kernel.js'), require('./net.js'), require('./mesh.js'));
  } else root.WBSync = factory(root.WB, root.WBNet, root.WBMesh);
}(typeof self !== 'undefined' ? self : this, function (WB, N, MeshMod) {
  'use strict';

  const MT = N.MT;

  class SyncClient extends N.EE {
    /**
     * @param {object} cfg
     *   userId, roomId,
     *   connectWS(): WebSocket        由调用方创建原生 WS（便于 URL/鉴权注入）
     *   doc: WB.Doc, buf: WB.CausalBuffer, clock: WB.Clock
     */
    constructor(cfg) {
      super();
      this.userId = cfg.userId;
      this.roomId = cfg.roomId;
      this.connectWS = cfg.connectWS;
      this.doc = cfg.doc || new WB.Doc();
      this.buf = cfg.buf || new WB.CausalBuffer();
      this.clock = cfg.clock || new WB.Clock(this.userId);
      this.history = cfg.history || new Map();

      this.ws = null;
      this.wsTransport = null;
      this.link = null;
      this.sendQueue = null;
      this.mesh = null;
      this.store = null;

      this.lastSeq = 0;
      this.state = 'offline';          // offline | hello | joining | online
      this.protocol = { major: N.PROTO.major, minor: N.PROTO.minor, degraded: false };
      this.snapshotMode = false;       // 服务端降级标记
      this.pending = new Map();        // envId -> env（内存镜像，IndexedDB 为准）
      this.seenServerSeq = 0;          // 已应用的最大服务端 seq（旧包不覆盖新状态）
      this._snapshotLoaded = false;
      this._syncPending = false;
      this._mediaReceivers = new Map();
      this._closing = false;
    }

    /* ------------------------- 启动 / 持久化 ------------------------- */

    async start() {
      this.store = await N.OfflineStore.create(this.userId);
      // 恢复时钟（Lamport/local/VC 跨刷新单调）
      const savedClock = await this.store.loadClock();
      if (savedClock) {
        this.clock.lamport = Math.max(this.clock.lamport, savedClock.lamport | 0);
        this.clock.local = Math.max(this.clock.local, savedClock.local | 0);
        Object.assign(this.clock.vc, savedClock.vc || {});
      }
      // 离线期间未确认信封载入内存
      const pending = await this.store.allPending();
      for (const e of pending) this.pending.set(e.id, e);

      // 尝试秒开：先用本地缓存快照渲染，再与服务端对账
      const cached = await this.store.getSnapshot(this.roomId);
      if (cached && cached.snapshot) {
        this._applySnapshotPayload(cached, true);
        this.emit('status', 'cached');
      }

      this._openWS();
      return this;
    }

    _openWS() {
      if (this._closing) return;
      this.setState('hello');
      const ws = this.connectWS();
      this.ws = ws;
      const transport = N.wrapWS(ws);
      this.wsTransport = transport;
      const link = new N.Link(transport, { pingInterval: 20000, timeout: 45000 });
      this.link = link;
      this.sendQueue = new N.SendQueue(link, { highWater: 1024 * 1024 });

      this.sendQueue.on('backpressure', (n) => this.emit('backpressure', n));
      this.sendQueue.on('drain', () => this.emit('drain'));
      link.on('message', (type, msg) => this._onMessage(type, msg));
      link.on('reset', (reason) => {
        if (this._closing) return;
        console.warn('[sync] link reset:', reason);
        this._handleDisconnect();
      });
      link.on('error', (err) => this.emit('error', err));

      // mesh 由本端驱动，信令经 WS link 发送
      this.mesh = new MeshMod.Mesh({
        selfId: this.userId,
        signal: (type, m) => { try { link.send(type, m); } catch (_) {} }
      });
      this.mesh.on('message', (from, type, msg) => this._onPeerMessage(from, type, msg));
      this.mesh.on('peer:open', (id) => { this.emit('peers', this.mesh.onlinePeers()); });
      this.mesh.on('peer:close', (id) => { this.emit('peers', this.mesh.onlinePeers()); });
      this.mesh.on('backpressure', (id, n) => this.emit('backpressure', n, 'p2p:' + id));

      // WS 打开后 Link 就绪，立刻 HELLO
      ws.addEventListener('open', () => {
        link.send(MT.HELLO, {
          name: N.PROTO.name, major: N.PROTO.major, minor: N.PROTO.minor,
          session: (Date.now() & 0xffffffff) >>> 0,
          capabilities: 1
        });
      });
      ws.addEventListener('close', () => { if (!this._closing) this._handleDisconnect(); });
      ws.addEventListener('error', () => { try { ws.close(); } catch (_) {} });
    }

    _handleDisconnect() {
      if (this.state === 'offline') return;
      this.setState('offline');
      this.emit('status', 'offline');
      // pending 已在 IndexedDB；指数退避重连
      const delay = Math.min(500 * Math.pow(2, this._retries() | 0), 8000);
      setTimeout(() => { if (!this._closing && this.state === 'offline') this._openWS(); }, delay);
    }
    _retries() { this._rt = (this._rt || 0) + 1; return this._rt; }

    setState(s) {
      this.state = s;
      this.emit('status', s);
    }

    /* ------------------------- 服务端消息 ------------------------- */

    _onMessage(type, msg) {
      switch (type) {
        case MT.WELCOME:
          if (!msg.ok) { this.emit('protocol', { rejected: true, reason: msg.reason }); this.close(); return; }
          this.protocol = { major: msg.major, minor: msg.minor, degraded: msg.action === 1 };
          this.emit('protocol', this.protocol);
          this.setState('joining');
          // JOIN 带 lastSeq + VC：服务端只发缺失部分
          this.link.send(MT.JOIN, {
            roomId: this.roomId, userId: this.userId,
            lastSeq: this.lastSeq,
            vc: this.clock.vc || {},
            relay: 1
          });
          break;

        case MT.JOINED:
          this._rt = 0;
          this.lastSeq = Math.max(this.lastSeq, msg.lastSeq);
          this.setState('online');
          this.mesh.setMembers([this.userId], msg.host || '');
          // 合并上传离线/未确认信封（服务端按 env.id 幂等）
          this._flushPending();
          break;

        case MT.MEMBERS:
          this.mesh.setMembers(msg.ids, msg.host);
          this.emit('peers', this.mesh.onlinePeers());
          break;

        case MT.SNAPSHOT:
          this._applySnapshotPayload(msg, false);
          this._maybeRequestMissing();
          break;

        case MT.OPS:
          this._ingestServerEnvelopes(msg.envelopes || [], 'server');
          break;

        case MT.ACK:
          this._onAck(msg);
          break;

        case MT.MODE:
          this.snapshotMode = msg.mode === 1;
          this.emit('mode', this.snapshotMode ? 'snapshot' : 'stream');
          if (!this.snapshotMode) this._syncPending = false;
          else this._scheduleSlowSync();
          break;

        case MT.RTC_SDP:
        case MT.RTC_ICE:
          this.mesh.handleSignal(type, msg);
          break;

        case MT.MEDIA:
          this.emit('media', msg);
          break;

        case MT.MEDIA_DATA:
          this._routeMediaData(msg);
          break;

        case MT.PONG:
          break;

        case MT.ERROR:
          console.warn('[sync] server error', msg.code, msg.message);
          this.emit('error', new Error(msg.message));
          break;
      }
    }

    /* ------------------------- P2P 消息 ------------------------- */

    _onPeerMessage(from, type, msg) {
      switch (type) {
        case MT.OPS:
          // P2P 旁路直达：信封已带完整因果向量，入因果缓冲；
          // 与服务端中继重复时 id 幂等丢弃。seq 以服务端广播为准，P2P 包 seq 仅作观测。
          this._ingestServerEnvelopes(msg.envelopes || [], 'p2p:' + from);
          break;
        case MT.MEDIA:
          this.emit('media', Object.assign({ from }, msg));
          break;
        case MT.MEDIA_REQ:
          // 本端持有该媒体（publishMedia 缓存）则自动分块响应，断点按 offset 续传
          if (!this.answerMediaRequest(from, msg)) this.emit('media-request', from, msg);
          break;
        case MT.MEDIA_DATA:
          this._routeMediaData(msg);
          break;
      }
    }

    /* ------------------------- 快照 / 增量 ------------------------- */

    /**
     * 应用同步载荷（全量快照 or 纯 delta）。
     * @param {boolean} cached 是否来自本地 IndexedDB 缓存（只渲染不对账 seq）
     */
    _applySnapshotPayload(payload, cached) {
      if (payload.hasSnapshot && payload.snapshot) {
        this.doc.loadSnapshot(payload.snapshot);
        // 快照基线 VC 并入因果缓冲与本地时钟
        const known = payload.snapshot.known || {};
        for (const k of Object.keys(known)) {
          this.buf.known[k] = known[k];
          this.clock.mergeVC({ [k]: known[k] });
        }
        this.seenServerSeq = Math.max(this.seenServerSeq, payload.lastSeq || payload.watermark || 0);
        if (!cached) {
          this.lastSeq = payload.lastSeq;
          this.store && this.store.saveSnapshot(this.roomId, payload).catch(() => {});
        }
        this._snapshotLoaded = true;
      }
      // 叠加信封（幂等）
      this._ingestServerEnvelopes(payload.envelopes || [], cached ? 'cache' : 'snapshot');
      if (!cached) {
        this.lastSeq = Math.max(this.lastSeq, payload.lastSeq | 0);
        this.seenServerSeq = Math.max(this.seenServerSeq, payload.lastSeq | 0);
      }
      this.emit('snapshot', payload, !!cached);
      this.emit('ops', []); // 触发渲染统一入口
    }

    /**
     * 接收信封统一入口（服务端 / P2P / 快照 delta）。
     * 三道防线：
     *   A. Link 层已保证帧有序不重（发送队列内严格 seq）；
     *   B. 服务端 seq 校验：回退 seq 直接丢弃，防止旧消息覆盖新状态；
     *   C. env.id 幂等 + CausalBuffer 依赖重排。
     */
    _ingestServerEnvelopes(envelopes, source) {
      // B：seq 过滤（P2P 包的 seq 可能滞后，不参与 seq 水位推进）
      const fromServer = source === 'server' || source === 'snapshot';
      const accepted = [];
      let maxSeq = this.seenServerSeq;
      if (fromServer) {
        const sorted = envelopes.slice().sort((a, b) => (a.seq | 0) - (b.seq | 0));
        for (const e of sorted) {
          const s = e.seq | 0;
          if (s === 0) { accepted.push(e); continue; }      // 快照内无 seq 信封
          if (s <= this.seenServerSeq) continue;            // 旧包/重复包丢弃
          accepted.push(e);
          if (s > maxSeq) maxSeq = s;
        }
      } else {
        for (const e of envelopes) accepted.push(e);
      }

      // C：幂等 + 因果重排
      let changed = false;
      this.buf.enqueue(accepted);
      const ready = this.buf.drain();
      for (const e of ready) {
        this.clock.observeLamport(e.lamport);
        this.clock.mergeVC(e.clock);
        if (this.doc.apply(e)) changed = true;
        this.history.set(e.id, e);
        // 别人的操作不会出现在本地 pending；自己操作经 P2P 回环时借此补确认
        if (this.pending.has(e.id) && source !== 'cache') this._confirmLocal(e.id);
      }
      if (fromServer && maxSeq > this.seenServerSeq) {
        this.seenServerSeq = maxSeq;
        this.lastSeq = Math.max(this.lastSeq, maxSeq);
      }
      // P2P 收到的操作继续向其它 peer 转发（mesh relay），不回送来源
      if (source.indexOf('p2p:') === 0 && ready.length) {
        const from = source.slice(4);
        this.mesh.fanout(MT.OPS, { envelopes: ready }, from);
      }
      if (changed) this.emit('ops', ready);

      // 检测缺口：服务端 seq 不连续 → 请求增量（乱序到达时由 CausalBuffer 挂起，
      // 超时仍未满足依赖也走同一路径）
      if (fromServer) this._maybeRequestMissing();
    }

    _gapDetected() {
      // rcvBuf 在 Link 层；这里用信封级启发式：
      // 服务端信封应按 seq 连续到达，若最大 seq 与最近水位差距 > 窗口则请求补齐。
      return false;
    }

    _maybeRequestMissing() {
      if (this._syncPending || this.state !== 'online') return;
      // CausalBuffer 有挂起信封（依赖缺失）说明增量有缺口：请求一次增量同步
      if (this.buf.pendingCount > 0) {
        this._syncPending = true;
        this.link.send(MT.REQ_SYNC, { lastSeq: this.lastSeq, vc: this.clock.vc });
        setTimeout(() => { this._syncPending = false; }, 2000);
      }
    }

    /** 慢客户端降级：等链路排空后发 REQ_SYNC 拉快照 */
    _scheduleSlowSync() {
      const tryReq = () => {
        if (!this.snapshotMode || this.state !== 'online') return;
        if (this.link.blocked) {
          const go = () => { this.link.off('drain', go); setTimeout(tryReq, 30); };
          this.link.on('drain', go);
          return;
        }
        this.link.send(MT.REQ_SYNC, { lastSeq: this.lastSeq, vc: this.clock.vc });
      };
      setTimeout(tryReq, 100);
    }

    /* ------------------------- 本地编辑提交 ------------------------- */

    /**
     * 提交一批已签名信封（SyncClient 负责本地乐观物化 + 持久化 + 发送）。
     */
    commit(envelopes) {
      const list = Array.isArray(envelopes) ? envelopes : [envelopes];
      const ready = this.buf.push(list);
      for (const e of ready) {
        this.clock.observeLamport(e.lamport);
        this.clock.mergeVC(e.clock);
        this.doc.apply(e);
        this.history.set(e.id, e);
      }
      this.publish(list);
      this.emit('local-commit', list);
      return list;
    }

    /**
     * 仅持久化 + 发送（调用方已自行做过本地物化，避免重复进 CausalBuffer）。
     * 浏览器 app.js 沿用既有的“先本地 buf.push 物化再发”流程时走这里。
     *  - 写入 IndexedDB pending（离线编辑、关页不丢）；
     *  - 在线：大操作优先 P2P mesh fanout（DataChannel 低延迟），
     *    同时经服务端 SendQueue 走权威 seq + ACK 兜底；
     *  - 离线：只落盘，重连后 _flushPending 按 lamport 序合并上传。
     */
    publish(envelopes) {
      const list = Array.isArray(envelopes) ? envelopes : [envelopes];
      for (const e of list) {
        this.pending.set(e.id, e);
        if (this.store) this.store.savePending(e).catch(() => {});
      }
      if (this.store) this.store.saveClock(this.clock);

      if (this.state === 'online') {
        if (this._isLargeBatch(list) && this.mesh && this.mesh.onlineCount > 0) {
          this.mesh.fanout(MT.OPS, { envelopes: list });
        }
        if (this.sendQueue) this.sendQueue.submitOps(list);
      }
      this.emit('local-publish', list);
      return list;
    }

    _isLargeBatch(list) {
      let bytes = 0;
      for (const e of list) {
        bytes += N.estimateEnvBytes(e);
        if (bytes > 16 * 1024) return true; // 超过约一帧：优先走 DataChannel
      }
      return false;
    }

    /** 重连后合并上传：按 lamport 升序（本地序号严格连续），分批发送 */
    async _flushPending() {
      const list = [...this.pending.values()]
        .sort((a, b) => (a.lamport - b.lamport) || (a.id < b.id ? -1 : 1));
      if (!list.length) return;
      // 快照若把本地未确认操作折叠掉了（换设备/清库场景），先本地幂等补回
      const missing = list.filter((e) => !this.doc.has(e.id));
      if (missing.length) this._ingestServerEnvelopes(missing, 'cache');
      for (let i = 0; i < list.length; i += 200) {
        this.sendQueue.submitOps(list.slice(i, i + 200));
      }
      this.emit('flush', list.length);
    }

    _onAck(msg) {
      for (const id of msg.ids || []) this._confirmLocal(id);
      this.lastSeq = Math.max(this.lastSeq, msg.lastSeq | 0);
      this.seenServerSeq = Math.max(this.seenServerSeq, msg.lastSeq | 0);
      this.emit('ack', msg);
    }

    _confirmLocal(id) {
      if (!this.pending.has(id)) return;
      this.pending.delete(id);
      this.store.removePendingById(id).catch(() => {});
    }

    /* ----------------------------- 媒体 ----------------------------- */

    /**
     * 发送媒体（图片二进制等）：
     *  有在线 P2P peer → DataChannel 分块（发送端不主动推，等对端 MEDIA_REQ 拉取，
     *    天然支持断点续传）；
     *  无 peer → HTTP PUT 到 /api/media 走服务端中继，服务端再向房间广播 MEDIA 元数据。
     * @returns {Promise<{mediaId,totalBytes,...}>}
     */
    async publishMedia(mediaId, bytes, meta) {
      bytes = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
      const full = Object.assign({
        mediaId, totalBytes: bytes.length, chunkSize: 16 * 1024,
        nChunks: Math.ceil(bytes.length / (16 * 1024)), url: '', from: this.userId
      }, meta || {});
      this._localMedia = this._localMedia || new Map();
      this._localMedia.set(mediaId, { bytes, meta: full });
      // 宣告元数据：服务端一份（用于无 mesh 成员），P2P 一份
      if (this.state === 'online') {
        try { this.link.send(MT.MEDIA, full); } catch (_) {}
        this.mesh.fanout(MT.MEDIA, full);
      }
      return full;
    }

    /** 请求拉取媒体（prefix 为本地已收前缀，支持断线续传） */
    requestMedia(meta, fromPeer, prefix) {
      const receiver = new N.MediaReceiver((t, m) => {
        if (fromPeer && this.mesh.peers.has(fromPeer)) this.mesh.sendTo(fromPeer, t, m);
        else if (this.state === 'online') this.link.send(t, m);
      }, { chunkSize: meta.chunkSize || 16 * 1024 });
      this._mediaReceivers.set(meta.mediaId, receiver);
      receiver.on('complete', (id, bytes, m) => {
        this._mediaReceivers.delete(id);
        this.emit('media-complete', id, bytes, m);
      });
      receiver.on('duplicate', (id, off) => this.emit('media-duplicate', id, off));
      receiver.request(meta, prefix || null);
      return receiver;
    }

    _routeMediaData(msg) {
      const r = this._mediaReceivers.get(msg.mediaId);
      if (r) { r.feed(msg); return; }
      // 未在拉取则忽略（上层可据 MEDIA 元数据稍后再 requestMedia）
    }

    /** 供 P2P 对端请求时（sync 持有发布缓存）创建发送端 */
    answerMediaRequest(from, req) {
      const item = this._localMedia && this._localMedia.get(req.mediaId);
      if (!item) return false;
      const sender = new N.MediaSender((t, m) => this.mesh.sendTo(from, t, m),
        req.mediaId, item.bytes, item.meta, { chunkSize: item.meta.chunkSize });
      sender.feedRequest(req);
      return true;
    }

    /* ----------------------------- 杂项 ----------------------------- */

    /** 主动发起一次增量对账（UI“同步”按钮/定时） */
    requestResync() {
      if (this.state === 'online') {
        this.link.send(MT.REQ_SYNC, { lastSeq: this.lastSeq, vc: this.clock.vc });
      }
    }

    get pendingCount() { return this.pending.size; }
    get onlinePeers() { return this.mesh ? this.mesh.onlinePeers() : []; }
    get backlogBytes() {
      const link = this.link ? this.link.bufferedAmount : 0;
      const mesh = this.mesh ? this.mesh.totalBuffered() : 0;
      return link + mesh;
    }

    close() {
      this._closing = true;
      try { this.mesh && this.mesh.close(); } catch (_) {}
      try { this.link && this.link.close('client close'); } catch (_) {}
      try { this.ws && this.ws.close(); } catch (_) {}
      this.setState('offline');
    }
  }

  return { SyncClient };
}));
