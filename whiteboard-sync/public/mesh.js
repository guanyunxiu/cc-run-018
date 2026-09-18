'use strict';

/* ===========================================================================
 * 协作白板 v3 - P2P Mesh（浏览器端）
 *
 * 职责：
 *  - 维护房间内成员间的 WebRTC RTCPeerConnection 网状连接；
 *  - SDP offer/answer 与 ICE candidate 全部经 WebSocket（WBNet.Link）转发 ——
 *    WS 只做信令/控制，一旦 DataChannel 建立，大操作与媒体走 P2P；
 *  - DataChannel 使用 ordered=false, maxRetransmits=0（不可靠、不保序），
 *    可靠性交给与服务端同一套 WBNet.Link（分片/ACK/SACK/重传/背压），
 *    因此浏览器端所有通道（WS / DataChannel）对上层都是同一个可靠消息接口；
 *  - ICE 失败 / 通道关闭时标记 down 并指数退避重连；
 *  - 提供 fanout(type,msg) 向所有 mesh peer 广播，sendTo 定向发送。
 *
 * 主机（host）：由服务端在 JOINED/MEMBERS 中给出（最早加入的 v3 成员）。
 * 新成员先与 host 建链；再根据 MEMBERS 与其余成员两两建链（mesh）。
 * 任一端均可用「userId 字典序较小者发起 offer」消弭 glare（双向同时邀请）。
 * =========================================================================== */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory(require('./net.js'));
  else root.WBMesh = factory(root.WBNet);
}(typeof self !== 'undefined' ? self : this, function (N) {
  'use strict';

  const MT = N.MT;
  const RTC_CONFIG = {
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]
  };

  class Peer {
    constructor(mesh, userId) {
      this.mesh = mesh;
      this.userId = userId;
      this.pc = null;
      this.dc = null;
      this.transport = null;
      this.link = null;
      this.state = 'new';            // new | connecting | connected | down
      this.retries = 0;
      this.retryTimer = null;
      this.iceQueue = [];
    }

    /** 建立（或重建）PC + DataChannel；由应发起 offer 的一端调用 */
    async connect() {
      if (this.state === 'connecting' || this.state === 'connected') return;
      this.state = 'connecting';
      this._closePc();
      const pc = new RTCPeerConnection(RTC_CONFIG);
      this.pc = pc;

      // 主数据通道：不可靠数据报语义，可靠性由 WBNet.Link 负责；
      // 大消息 Link 自行切成 ≤16KB 帧，匹配 DataChannel 安全阈值。
      const dc = pc.createDataChannel('wb3', { ordered: false, maxRetransmits: 0 });
      this.dc = dc;
      dc.binaryType = 'arraybuffer';
      this._bindChannel(dc);

      pc.onicecandidate = (e) => {
        if (e.candidate) {
          this.mesh.signal(MT.RTC_ICE, {
            from: this.mesh.selfId, to: this.userId,
            candidate: JSON.stringify(e.candidate)
          });
        }
      };
      pc.onconnectionstatechange = () => {
        if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
          this.markDown('pc ' + pc.connectionState);
        }
      };
      pc.oniceconnectionstatechange = () => {
        if (pc.iceConnectionState === 'failed') this.markDown('ice failed');
      };

      try {
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        this.mesh.signal(MT.RTC_SDP, {
          from: this.mesh.selfId, to: this.userId,
          sdp: JSON.stringify(pc.localDescription)
        });
      } catch (err) {
        this.markDown('offer failed: ' + err.message);
      }
    }

    /** 收到对端 offer：回 answer */
    async handleOffer(sdpJson) {
      if (this.state === 'connected') return;
      this.state = 'connecting';
      this._closePc();
      const pc = new RTCPeerConnection(RTC_CONFIG);
      this.pc = pc;
      pc.ondatachannel = (e) => this._bindChannel(e.channel);
      pc.onicecandidate = (e) => {
        if (e.candidate) {
          this.mesh.signal(MT.RTC_ICE, {
            from: this.mesh.selfId, to: this.userId,
            candidate: JSON.stringify(e.candidate)
          });
        }
      };
      pc.onconnectionstatechange = () => {
        if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
          this.markDown('pc ' + pc.connectionState);
        }
      };
      try {
        await pc.setRemoteDescription(JSON.parse(sdpJson));
        for (const c of this.iceQueue) { try { await pc.addIceCandidate(c); } catch (_) {} }
        this.iceQueue.length = 0;
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        this.mesh.signal(MT.RTC_SDP, {
          from: this.mesh.selfId, to: this.userId,
          sdp: JSON.stringify(pc.localDescription)
        });
      } catch (err) {
        this.markDown('answer failed: ' + err.message);
      }
    }

    /** 收到对端 answer（仅发起方） */
    async handleAnswer(sdpJson) {
      if (!this.pc || this.pc.remoteDescription) return;
      try {
        await this.pc.setRemoteDescription(JSON.parse(sdpJson));
        for (const c of this.iceQueue) { try { await this.pc.addIceCandidate(c); } catch (_) {} }
        this.iceQueue.length = 0;
      } catch (err) {
        this.markDown('remote-desc failed: ' + err.message);
      }
    }

    async handleIce(candidateJson) {
      let cand;
      try { cand = JSON.parse(candidateJson); } catch (_) { return; }
      if (this.pc && this.pc.remoteDescription) {
        try { await this.pc.addIceCandidate(cand); } catch (_) {}
      } else {
        this.iceQueue.push(cand); // SDP 尚未到达，缓存后补
      }
    }

    _bindChannel(dc) {
      this.dc = dc;
      dc.binaryType = 'arraybuffer';
      const transport = new N.StreamTransport({
        send(u8) {
          if (dc.readyState !== 'open') return false;
          // 超 bufferedAmount 低水位返回 false，Link 窗口背压会自然降速
          if (dc.bufferedAmount > 1024 * 1024) return false;
          dc.send(u8.buffer.slice(u8.byteOffset, u8.byteOffset + u8.byteLength));
          if (dc.bufferedAmountLowThreshold === 0) dc.bufferedAmountLowThreshold = 256 * 1024;
          return dc.bufferedAmount < 1024 * 1024;
        }
      });
      this.transport = transport;
      dc.onopen = () => {
        this.state = 'connected';
        this.retries = 0;
        // 可靠层挂到 DataChannel 上（与 WS Link 同构：分片/ACK/重传/序号校验）
        this.link = new N.Link(transport, { pingInterval: 10000, timeout: 30000 });
        this.link.on('message', (type, msg) => this.mesh.onPeerMessage(this.userId, type, msg));
        this.link.on('reset', () => this.markDown('link reset'));
        this.link.on('backpressure', (n) => this.mesh.emit('backpressure', this.userId, n));
        dc.onbufferedamountlow = () => transport.emit('drain');
        this.mesh.emit('peer:open', this.userId);
      };
      dc.onmessage = (ev) => transport.feed(new Uint8Array(ev.data));
      dc.onclose = () => this.markDown('dc closed');
      dc.onerror = () => this.markDown('dc error');
    }

    markDown(reason) {
      const wasUp = this.state === 'connected';
      this.state = 'down';
      if (this.link) { try { this.link.close('down'); } catch (_) {} this.link = null; }
      if (wasUp) this.mesh.emit('peer:close', this.userId, reason);
      this.mesh.emit('peer:down', this.userId, reason);
      // 指数退避重连（仍由“较小 id 发起”规则决定谁来 offer）
      clearTimeout(this.retryTimer);
      const delay = Math.min(500 * Math.pow(2, this.retries), 8000);
      this.retries += 1;
      this.retryTimer = setTimeout(() => {
        if (this.mesh.shouldInitiate(this.userId)) this.connect();
      }, delay);
    }

    _closePc() {
      if (this.link) { try { this.link.close('rebuild'); } catch (_) {} this.link = null; }
      if (this.pc) { try { this.pc.close(); } catch (_) {} }
      this.pc = null;
      this.dc = null;
      this.transport = null;
    }

    send(type, msg) {
      if (this.link && this.state === 'connected') {
        this.link.send(type, msg);
        return true;
      }
      return false;
    }

    get up() { return this.state === 'connected' && !!this.link; }
    get bufferedAmount() {
      return this.dc ? (this.dc.bufferedAmount || 0) : 0;
    }
  }

  /**
   * @param {object} opts
   *   selfId   本端 userId
   *   signal(type,msg)  经 WS 发送 RTC_SDP / RTC_ICE 的回调
   */
  class Mesh extends N.EE {
    constructor(opts) {
      super();
      this.selfId = opts.selfId;
      this.signal = opts.signal;
      this.peers = new Map();   // userId -> Peer
      this.members = new Set();
      this.host = '';
    }

    /** 服务端成员列表更新 */
    setMembers(ids, host) {
      this.host = host || '';
      this.members = new Set(ids.filter((id) => id !== this.selfId));
      for (const id of this.members) {
        if (!this.peers.has(id)) {
          const peer = new Peer(this, id);
          this.peers.set(id, peer);
          if (this.shouldInitiate(id)) peer.connect();
        }
      }
      for (const [id, peer] of this.peers) {
        if (!this.members.has(id)) { peer._closePc(); this.peers.delete(id); }
      }
    }

    /**
     * 消弭 glare：userId 字典序较小的一端发起 offer。
     * host 总是对其所有成员发起（新成员加入首个连接能立刻建立）。
     */
    shouldInitiate(remoteId) {
      if (this.host && this.selfId === this.host) return true;
      if (this.host && remoteId === this.host) return false;
      return this.selfId < remoteId;
    }

    /** 处理 WS 转发来的 RTC 信令 */
    async handleSignal(type, msg) {
      if (msg.to !== this.selfId) return;
      let peer = this.peers.get(msg.from);
      if (!peer) { peer = new Peer(this, msg.from); this.peers.set(msg.from, peer); }
      if (type === MT.RTC_SDP) {
        const desc = JSON.parse(msg.sdp);
        if (desc.type === 'offer') await peer.handleOffer(msg.sdp);
        else if (desc.type === 'answer') await peer.handleAnswer(msg.sdp);
      } else if (type === MT.RTC_ICE) {
        await peer.handleIce(msg.candidate);
      }
    }

    onPeerMessage(from, type, msg) {
      this.emit('message', from, type, msg);
    }

    /** 向所有在线 peer 广播；返回至少有一个可达 peer */
    fanout(type, msg, except) {
      let delivered = 0;
      for (const [id, peer] of this.peers) {
        if (except && id === except) continue;
        if (peer.send(type, msg)) delivered++;
      }
      return delivered;
    }

    sendTo(remoteId, type, msg) {
      const peer = this.peers.get(remoteId);
      return peer ? peer.send(type, msg) : false;
    }

    /** 当前在线 P2P peer 数量 */
    get onlineCount() {
      let n = 0;
      for (const p of this.peers.values()) if (p.up) n++;
      return n;
    }
    onlinePeers() {
      const out = [];
      for (const [id, p] of this.peers) if (p.up) out.push(id);
      return out;
    }
    totalBuffered() {
      let n = 0;
      for (const p of this.peers.values()) n += p.bufferedAmount;
      return n;
    }

    close() {
      for (const p of this.peers.values()) p._closePc();
      this.peers.clear();
    }
  }

  return { Mesh, Peer, RTC_CONFIG };
}));
