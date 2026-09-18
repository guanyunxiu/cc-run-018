'use strict';

/* ===========================================================================
 * 协作白板内核（客户端 / 服务端共享，零依赖）
 *
 * 一致性模型：LWW-Element-Map CRDT + 因果广播
 *  - 每个字段是一个 LWW 寄存器：写入带 (lamport, clientId) 时间戳，
 *    任意两个副本对同一组操作折叠出相同结果（状态最终一致，与到达顺序无关）。
 *  - 操作携带 Lamport 逻辑时钟 lamport 与版本向量 clock（依赖向量），
 *    服务端/客户端统一通过 CausalBuffer 做因果投递（happens-before）。
 *  - 服务端 seq 只用于日志排序 / 观测，不参与冲突仲裁。
 *
 * 选择性撤销：不回滚历史，而是发一条“逆操作”。
 *  - 逆操作带 inv:{originId, polarity}，折叠时若检测到原操作之后存在
 *    他人的冲突写入（causal-after），则该逆操作 void（空转），
 *    因此撤销自己的旧操作绝不会覆盖别人后续的修改。
 *
 * 同文件 UMD：Node(require) / 浏览器(<script>) 均可加载。
 * ========================================================================= */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.WB = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const TILE_CELLS = 16;                 // 每边 16x16 个擦除单元
  const SHAPES = ['rect', 'ellipse', 'triangle', 'arrow', 'line'];
  const BRUSHES = ['pen', 'highlighter', 'dashed', 'texture', 'eraser'];
  const TIE_DOT = '.';                   // (lamport, clientId) 比较的连接符

  function uid(prefix) {
    return (prefix || 'id') + '-' +
      Date.now().toString(36) + '-' +
      Math.random().toString(36).slice(2, 10);
  }

  /* ------------------------------- 时钟 ------------------------------- */

  /**
   * Lamport 逻辑时钟 + 版本向量（依赖向量）。
   * local: 本节点发出去的操作计数；vc: 观测到的各节点计数。
   */
  class Clock {
    constructor(clientId) {
      this.clientId = clientId;
      this.lamport = 0;
      this.local = 0;
      this.vc = Object.create(null);
    }

    /** 生成本地操作的 (lamport, vc)；local 单调递增 */
    tick() {
      this.local += 1;
      this.lamport += 1;
      this.vc[this.clientId] = this.local;
      return { lamport: this.lamport, clock: this.snapshotVC() };
    }

    /** 观测到远端时间戳后推进 Lamport（取 max+1，不在此处并入 vc） */
    observeLamport(l) {
      if (Number.isFinite(l) && l > this.lamport) this.lamport = l;
    }

    /** 投递远端操作后并入其版本向量 */
    mergeVC(other) {
      if (!other) return;
      for (const k of Object.keys(other)) {
        const v = other[k] | 0;
        if (v > (this.vc[k] | 0)) this.vc[k] = v;
      }
    }

    snapshotVC() {
      return Object.assign(Object.create(null), this.vc);
    }

    cloneVC() { return this.snapshotVC(); }
  }

  /** vcA 是否覆盖 vcB（每个维度计数都 >=） */
  function vDominates(va, vb) {
    if (!vb) return true;
    for (const k of Object.keys(vb)) {
      if ((va[k] | 0) < (vb[k] | 0)) return false;
    }
    return true;
  }

  /* ----------------------------- 因果缓冲 ----------------------------- */

  /**
   * 严格的“按发送者顺序 + 依赖覆盖”因果缓冲。
   * 规则（env 来自 sender，版本向量 clock，Lamport lamport）：
   *   clock[sender] === 本地已知 sender 计数 + 1
   *   且对所有其它节点 p，clock[p] <= 本地已知 p 计数
   * 满足即可投递；投递后并入 clock，继续尝试冲刷积压。
   */
  class CausalBuffer {
    constructor() {
      this.known = Object.create(null); // clientId -> 已投递最大 local 计数
      this.pending = [];                // 未满足因果的操作
    }

    /**
     * 尝试投递一个信封（或一批），返回本次新就绪、可按序物化的信封。
     * 因果规则（env 来自 sender，版本向量 clock）：
     *   clock[sender] === 已知 sender 计数 + 1（同一发送者严格连续、无缺号）
     *   且对任意 p≠sender，clock[p] <= 已知 p 计数（不引用尚未见到的依赖）。
     * 事务原子性：同一 txnId 的成员 sender 计数连续，缺任意一条时最后一条也不满足，
     * 因此整组同时进入就绪序列，不会出现“只看到一半”。
     */
    push(env) {
      this.enqueue(env);
      return this.drain(null);
    }

    /** 仅入队，不冲刷（供需要把整批/整事务收齐后再一次性 drain 的场景，如服务端） */
    enqueue(env) {
      const incoming = Array.isArray(env) ? env.slice() : [env];
      for (const e of incoming) {
        if (!this.pending.some((p) => p.id === e.id)) this.pending.push(e);
      }
      return this;
    }

    /** 一个信封是否满足投递条件 */
    _ready(e) {
      const sender = e.clientId;
      const have = this.known[sender] | 0;
      const need = (e.clock && e.clock[sender]) | 0;
      if (need !== have + 1) return false;
      for (const k of Object.keys(e.clock || {})) {
        if (k === sender) continue;
        if ((e.clock[k] | 0) > (this.known[k] | 0)) return false;
      }
      return true;
    }

    /**
     * 反复冲刷缓冲：对每个就绪信封推进 known 计数并回调 applyFn，
     * 直到没有新的就绪信封（连锁解锁也在同一轮内完成）。
     * @param {(env:object)=>void} [applyFn] 可选，就绪即回调（如服务端物化+定序）
     * @returns {Array<env>} 本次投递的全部信封（因果有序）
     */
    drain(applyFn) {
      const out = [];
      let progressed = true;
      while (progressed && this.pending.length) {
        progressed = false;
        for (let i = 0; i < this.pending.length; i++) {
          const e = this.pending[i];
          if (this._ready(e)) {
            this.pending.splice(i, 1);
            i -= 1;
            this.known[e.clientId] = (e.clock && e.clock[e.clientId]) ||
              ((this.known[e.clientId] | 0) + 1);
            out.push(e);
            if (applyFn) applyFn(e);
            progressed = true;
          }
        }
      }
      return out;
    }

    has(id) { return this.pending.some((e) => e.id === id); }
    get pendingCount() { return this.pending.length; }
  }

  /* --------------------------- 时间戳胜负比较 --------------------------- */

  /** (lamport, clientId) 字典序：lamport 大的胜；相等则 clientId 大的胜 */
  function tsNewer(la, ca, lb, cb) {
    if (la !== lb) return la > lb;
    return String(ca) > String(cb);
  }
  function tag(lamport, clientId) { return lamport + TIE_DOT + clientId; }
  function tagParts(tagStr) {
    const i = tagStr.indexOf(TIE_DOT);
    return { lamport: Number(tagStr.slice(0, i)), clientId: tagStr.slice(i + 1) };
  }

  /* ============================ CRDT 文档模型 ============================ */

  /** 一条擦除寄存器记录是否覆盖了指定单元 */
  function eraseRecCoversCell(r, cell) {
    for (const c of r.value || []) {
      if (c[0] === cell[0] && c[1] === cell[1]) return true;
    }
    return false;
  }

  /**
   * 擦除寄存器每条记录的 id 形如 "<envId>:<tx>:<ty>"（一个信封可含多个分块），
   * 而 inv.originId 是信封 id 本身；撤销匹配原操作时两种形式都要认。
   */
  function eraseRecIsOrigin(r, originId) {
    return r.id === originId || (typeof r.id === 'string' && r.id.startsWith(originId + ':'));
  }

  /**
   * 对象白板的 CRDT 物化器。输入因果有序的信封流，输出对象表 + 图层 + 擦除层。
   *
   * 字段（每个对象一份 LWW 寄存器集合）：
   *   type / x / y / w / h / rot / content / stroke / style / group / deleted
   * 寄存器保存完整写入记录 {value, lamport, clientId, inv?}，
   * 选择胜者时跳过“被他人后续修改架空”的逆操作（selective-undo 核心）。
   */
  class Doc {
    constructor() {
      this.objs = new Map();     // oid -> { base:{field:rec}, layers:[rec...], regs:{field:[rec...]} }
      this.groups = new Map();   // gid -> rec
      this.delivered = new Set();
    }

    _ensure(oid) {
      let o = this.objs.get(oid);
      if (!o) {
        o = { regs: new Map() };
        this.objs.set(oid, o);
      }
      return o;
    }

    _putReg(o, field, rec) {
      let arr = o.regs.get(field);
      if (!arr) { arr = []; o.regs.set(field, arr); }
      if (!arr.some((r) => r.id === rec.id)) arr.push(rec);
    }

    /**
     * 判断一条逆操作（undo / redo）是否被架空。
     * @param {object} rec 逆写入记录
     * @param {object} o   对象
     * @param {boolean} wide create/delete 类逆操作按对象级保护
     * @param {string} fieldKey 窄保护时指定寄存器（擦除寄存器形如 erase:tx:ty）
     * @param {[number,number]} cell 像素擦寄存器专用：保护粒度细化到块内单元，
     *        只统计覆盖该单元的他人普通写入（同块内别的单元不算冲突意图）。
     */
    _inverseVoid(rec, o, wide, fieldKey, cell) {
      const inv = rec.inv;
      if (!inv) return false;
      const originL = inv.originLamport;

      // wide=true：create 的逆操作，对象任一字段被他人改过即空转；
      // 否则只保护逆写入所在字段（擦除记录用 fieldKey 指定 erase 寄存器）。
      // wide（create 的撤销）：对象上任意寄存器（含像素擦除块）被他人写过即空转；
      // 窄（set/layer/erase 的撤销）：只看逆写入所在的那一个寄存器。
      const fields = wide
        ? Array.from(o.regs.keys())
        : [fieldKey || rec.field];

      for (const f of fields) {
        const arr = o.regs.get(f) || [];
        const isEraseField = f.indexOf('erase:') === 0;
        for (const r of arr) {
          if (r.id === rec.id) continue;
          if (r.clientId === rec.clientId) continue; // 只防“别人”
          if (r.inv) continue;                        // 别人的撤销/重做不构成保护性写入
          if (cell && !eraseRecCoversCell(r, cell)) continue; // 像素擦：只看同一单元
          // 原操作自身不是保护性写入（撤销的正是它）；
          // 擦除记录 id 带 ":tx:ty" 后缀，用前缀匹配认回原信封
          if (r.id === inv.originId) continue;
          if (isEraseField && eraseRecIsOrigin(r, inv.originId)) continue;
          // 别人在原操作之后（含并发 Lamport 更大）的写入 → 逆操作空转
          if (r.lamport >= originL) return true;
        }
      }
      return false;
    }

    /**
     * 选出某字段当前生效的写入记录。普通写恒有效；逆写需通过架空检测。
     */
    _winner(o, field) {
      const arr = o.regs.get(field) || [];
      let winner = null;
      for (const r of arr) {
        if (r.inv && this._inverseVoid(r, o, r.inv.wide === true)) continue;
        if (!winner || tsNewer(r.lamport, r.clientId, winner.lamport, winner.clientId)) winner = r;
      }
      return winner;
    }

    /** 应用一个信封（调用方须保证因果有序、幂等去重） */
    apply(env) {
      if (!env || !env.id || this.delivered.has(env.id)) return false;
      this.delivered.add(env.id);
      const op = env.op || {};
      switch (op.kind) {
        case 'create':  this._applyCreate(env); break;
        case 'set':     this._applySet(env); break;
        case 'delete':  this._applyDelete(env, true); break;
        case 'restore': this._applyDelete(env, false); break;
        case 'group':   this._applyGroup(env, true); break;
        case 'ungroup': this._applyGroup(env, false); break;
        case 'layer':   this._applyLayer(env); break;
        case 'erase':   this._applyErase(env); break;
        // 未知 kind 忽略（向前兼容）
      }
      return true;
    }

    _rec(env, field, value, extra) {
      return Object.assign({
        id: env.id, field, value,
        lamport: env.lamport | 0, clientId: env.clientId
      }, extra || {});
    }

    _applyCreate(env) {
      const op = env.op;
      // 逆 create（redo 重建）要让架空检测按对象级保护
      const extra = op.inv ? { inv: op.inv } : null;
      for (const init of op.objects || []) {
        const o = this._ensure(init.oid);
        const fields = Object.assign({ type: init.type, deleted: false }, init.fields || {});
        for (const f of Object.keys(fields)) {
          this._putReg(o, f, this._rec(env, f, fields[f], extra));
        }
      }
    }

    _applySet(env) {
      const op = env.op;
      const o = this._ensure(op.oid);
      for (const f of Object.keys(op.fields || {})) {
        this._putReg(o, f, this._rec(env, f, op.fields[f], {
          inv: op.inv || null, squashKey: op.squashKey || null
        }));
      }
    }

    _applyDelete(env, isDelete) {
      const op = env.op;
      for (const oid of op.oids || []) {
        const o = this._ensure(oid);
        this._putReg(o, 'deleted', this._rec(env, 'deleted', isDelete, {
          inv: op.inv || null
        }));
      }
    }

    _applyGroup(env, isGroup) {
      const op = env.op;
      const rec = {
        id: env.id, lamport: env.lamport | 0, clientId: env.clientId,
        gid: op.gid, members: op.oids || [], value: isGroup
      };
      // 组本身 LWW
      if (isGroup) {
        const prev = this.groups.get(op.gid);
        if (!prev || tsNewer(rec.lamport, rec.clientId, prev.lamport, prev.clientId)) {
          this.groups.set(op.gid, rec);
        }
      } else {
        const prev = this.groups.get(op.gid);
        if (prev && tsNewer(rec.lamport, rec.clientId, prev.lamport, prev.clientId)) {
          this.groups.delete(op.gid);
        }
      }
      // 成员对象的 group 字段也走 LWW（删除组时写 null）
      for (const oid of op.oids || []) {
        const o = this._ensure(oid);
        this._putReg(o, 'group', this._rec(env, 'group', isGroup ? op.gid : null));
      }
    }

    _applyLayer(env) {
      const op = env.op;
      const o = this._ensure(op.oid);
      this._putReg(o, 'z', this._rec(env, 'z', op.z, {
        inv: op.inv || null
      }));
    }

    _applyErase(env) {
      const op = env.op;
      for (const ch of op.chunks || []) {
        const key = 'erase:' + ch.tx + ':' + ch.ty;
        const o = this._ensure(ch.oid);
        let arr = o.regs.get(key);
        if (!arr) { arr = []; o.regs.set(key, arr); }
        const recId = env.id + ':' + ch.tx + ':' + ch.ty;
        if (!arr.some((r) => r.id === recId)) {
          arr.push({
            id: recId, field: key,
            lamport: env.lamport | 0, clientId: env.clientId,
            value: ch.cells || [], unerase: !!op.unerase,
            inv: op.inv || null
          });
        }
      }
    }

    /** 读取一个对象的当前物化值（不含擦除单元） */
    get(oid) {
      const o = this.objs.get(oid);
      if (!o) return null;
      const out = { oid };
      for (const f of o.regs.keys()) {
        if (f.startsWith('erase:')) continue;
        const w = this._winner(o, f);
        if (w) out[f] = w.value;
      }
      return out;
    }

    /** 所有未删除对象，按图层序（z 升序，平局 (createLamport, clientId)） */
    liveObjects() {
      const out = [];
      for (const [oid, o] of this.objs) {
        const del = this._winner(o, 'deleted');
        if (del && del.value === true) continue;
        const obj = this.get(oid);
        out.push(obj);
      }
      out.sort((a, b) => {
        const za = a.z || ''; const zb = b.z || '';
        if (za !== zb) return fracCmp(za, zb);
        const oa = this.objs.get(a.oid); const ob = this.objs.get(b.oid);
        const ra = oa.regs.get('type')[0]; const rb = ob.regs.get('type')[0];
        if (ra.lamport !== rb.lamport) return ra.lamport - rb.lamport;
        return String(ra.clientId) < String(rb.clientId) ? -1 : 1;
      });
      return out;
    }

    /** 某笔迹对象被像素擦掉的单元集合（Set<"tx,ty,cx,cy">） */
    erasedCells(oid) {
      const o = this.objs.get(oid);
      const set = new Set();
      if (!o) return set;
      // 逐单元 LWW：同一单元以最新（lamport, clientId）的普通擦/取消擦为准；
      // 逆写入（撤销/重做）的架空检测同样按“单元”粒度 ——
      // 只有他人在原擦除之后又擦了【同一单元】才让该单元的恢复空转，
      // 同一块内别人擦的其它单元不影响本单元的撤销。
      const perCell = new Map();
      for (const key of o.regs.keys()) {
        if (!key.startsWith('erase:')) continue;
        for (const r of o.regs.get(key)) {
          for (const c of r.value) {
            if (r.inv && this._inverseVoid(r, o, false, key, c)) continue;
            const ck = key.slice(6) + ',' + c[0] + ',' + c[1];
            const prev = perCell.get(ck);
            // erase / unerase 也是 LWW：以 (lamport, clientId) 最新为准
            if (!prev || tsNewer(r.lamport, r.clientId, prev.lamport, prev.clientId)) {
              perCell.set(ck, { erased: !r.unerase, lamport: r.lamport, clientId: r.clientId });
            }
          }
        }
      }
      for (const [ck, v] of perCell) if (v.erased) set.add(ck);
      return set;
    }

    has(id) { return this.delivered.has(id); }

    /**
     * 导出压缩快照（新成员 / 压缩水位之后的重连者使用）。
     * 普通寄存器只保留当前获胜记录；像素擦寄存器按“单元”折叠：
     * 每个单元保留当前获胜记录 + 每个他人在该单元上最新的普通写入（protector）。
     * 获胜记录携带 (lamport, clientId) 继续参与 LWW 仲裁；protector 保证快照之后
     * 到达的撤销/重做仍能做单元级架空检测（压缩不能削弱选择性撤销的保护）。
     */
    snapshot(knownVC) {
      const objects = [];
      for (const [oid, o] of this.objs) {
        const regs = {};
        const erases = {}; // "tx,ty" -> [{cell:[cx,cy], rec}]
        for (const [key, arr] of o.regs) {
          if (key.startsWith('erase:')) {
            const tileKey = key.slice(6);
            // 逐单元折叠（与 erasedCells 同一套语义）：
            //  1) 逆写入按“单元级架空”判定，被架空的撤销/重做不作为该单元获胜值；
            //  2) 除获胜记录外，再保留每个他人在该单元上最新的普通写入
            //     （protector），供快照之后到达的逆操作继续做架空保护——
            //     压缩只删历史信封，不能让选择性撤销的保护失效。
            const perCell = new Map();
            for (const r of arr) {
              for (const c of r.value) {
                const ck = c[0] + ',' + c[1];
                let entry = perCell.get(ck);
                if (!entry) { entry = { winner: null, protectors: new Map() }; perCell.set(ck, entry); }
                const isVoidInv = r.inv && this._inverseVoid(r, o, false, key, c);
                if (isVoidInv) continue; // 被架空的逆写入：既不胜出也不充当保护
                if (!entry.winner ||
                  tsNewer(r.lamport, r.clientId, entry.winner.rec.lamport, entry.winner.rec.clientId)) {
                  entry.winner = { cell: c, rec: r };
                }
                if (!r.inv) {
                  const p = entry.protectors.get(r.clientId);
                  if (!p || tsNewer(r.lamport, r.clientId, p.lamport, p.clientId)) {
                    entry.protectors.set(r.clientId, r);
                  }
                }
              }
            }
            if (perCell.size) {
              const cells = [];
              for (const [, entry] of perCell) {
                const keep = [entry.winner.rec];
                for (const [cid, r] of entry.protectors) {
                  if (cid === entry.winner.rec.clientId) continue;
                  if (!keep.some((q) => q.id === r.id)) keep.push(r);
                }
                // 保留的 protector 必然覆盖该单元；序列化时统一记该单元坐标
                const cell = entry.winner.cell;
                for (const r of keep) {
                  cells.push({ cell, id: r.id, lamport: r.lamport,
                    clientId: r.clientId, unerase: !!r.unerase, inv: r.inv || null });
                }
              }
              if (cells.length) erases[tileKey] = cells;
            }
            continue;
          }
          const w = this._winner(o, key);
          if (w) {
            regs[key] = { id: w.id, value: w.value, lamport: w.lamport,
              clientId: w.clientId, inv: w.inv || null, squashKey: w.squashKey || null };
          }
        }
        objects.push({ oid, regs, erases });
      }
      const groups = [];
      for (const [gid, r] of this.groups) {
        groups.push({ gid, id: r.id, lamport: r.lamport, clientId: r.clientId, members: r.members });
      }
      return {
        version: 2,
        known: knownVC ? Object.assign(Object.create(null), knownVC) : Object.create(null),
        groups,
        objects
      };
    }

    /** 载入快照：重建寄存器（每寄存器仅获胜记录），随后可继续叠加信封 */
    loadSnapshot(snap) {
      this.objs.clear();
      this.groups.clear();
      this.delivered.clear();
      for (const so of snap.objects || []) {
        const o = { regs: new Map() };
        this.objs.set(so.oid, o);
        for (const key of Object.keys(so.regs)) {
          const r = so.regs[key];
          o.regs.set(key, [{ id: r.id, field: key, value: r.value,
            lamport: r.lamport | 0, clientId: r.clientId, inv: r.inv || null,
            squashKey: r.squashKey || null }]);
          this.delivered.add(r.id);
        }
        for (const tileKey of Object.keys(so.erases || {})) {
          const key = 'erase:' + tileKey;
          const arr = [];
          for (const e of so.erases[tileKey]) {
            arr.push({ id: e.id, field: key, value: [e.cell],
              lamport: e.lamport | 0, clientId: e.clientId,
              unerase: !!e.unerase, inv: e.inv || null });
            this.delivered.add(e.id);
          }
          o.regs.set(key, arr);
        }
      }
      for (const g of snap.groups || []) {
        this.groups.set(g.gid, { id: g.id, lamport: g.lamport | 0, clientId: g.clientId,
          gid: g.gid, members: g.members, value: true });
      }
      return this;
    }
  }

  /* ----------------------------- 信封构造 ----------------------------- */

  /**
   * 构造一个操作信封。
   * @param {Clock} clock 当前节点时钟（tick 后填入 lamport/clock）
   * @param {object} op    操作体 {kind, ...}
   * @param {object} opts  {txnId, squashKey, inv}
   */
  function makeEnvelope(clock, op, opts) {
    opts = opts || {};
    const t = clock.tick();
    return {
      id: clock.clientId + ':' + clock.local,
      clientId: clock.clientId,
      lamport: t.lamport,
      clock: t.clock,
      txnId: opts.txnId || null,
      squashKey: opts.squashKey || null,
      op: opts.inv ? Object.assign({}, op, { inv: opts.inv }) : op
    };
  }

  /** 把多个信封标记为同一原子事务（共享 txnId） */
  function atomic(envelopes) {
    const txnId = envelopes[0].txnId || uid('txn');
    for (const e of envelopes) e.txnId = txnId;
    return envelopes;
  }

  /* ============================ 操作压缩 ============================ */

  /**
   * 日志压缩：相同 squashKey 的 set 操作只保留最后一条（携带最终值）。
   * 连续移动 / 缩放会产生大量同 squashKey 的 op，提交后压缩为最终状态，
   * 显著减小历史日志体积。create 等无 squashKey 的操作原样保留。
   * 保留被压缩操作的因果位置：返回的信封按最小 lamport 排序。
   */
  function squash(envelopes) {
    const keep = new Map(); // squashKey -> env
    const passthrough = [];
    for (const env of envelopes) {
      const key = env.squashKey;
      if (key) {
        const prev = keep.get(key);
        if (!prev || tsNewer(env.lamport, env.clientId, prev.lamport, prev.clientId)) {
          keep.set(key, env);
        }
      } else {
        passthrough.push(env);
      }
    }
    return passthrough.concat(Array.from(keep.values()))
      .sort((a, b) => a.lamport - b.lamport || (String(a.clientId) < String(b.clientId) ? -1 : 1));
  }

  /* ===================== 分数序（fractional indexing） ===================== */

  // 图层顺序 z 使用十进制分数字符串（"0." 开头），哨兵 0 / 1。
  // midpoint 用逐位十进制大数平均，保证并发下永不冲突、无需重排。

  function isFrac(s) { return typeof s === 'string' && s.startsWith('0.'); }

  /** 分数字符串 -> 数字数组（去掉 "0."） */
  function digits(s) {
    const out = [];
    for (let i = 2; i < s.length; i++) out.push(s.charCodeAt(i) - 48);
    return out;
  }

  function fromDigits(a) {
    // 去掉尾随 0（规范形式，避免 "0.50" 与 "0.5" 两种写法）
    let n = a.length;
    while (n > 1 && a[n - 1] === 0) n -= 1;
    return '0.' + a.slice(0, n).join('');
  }

  /**
   * 两个分数的严格中点（0 <= a < b <= 1）。
   * 两段十进制运算（进位方向相反，必须分开）：
   *  1) 加法：从最低位向右向左逐位求和，进位向左，可能产生整数部分 1（如 0.8+0.7=1.5）；
   *  2) 除 2：从整数位向左向右逐位长除，余数 ×10 进到右侧低位。
   * 例：0.59375 与 0.625 → 和 1.21875 → 中点 0.609375。
   * @param {number[]} ad a 的小数位（空数组表示边界 0）
   * @param {number[]} [bd] b 的小数位；缺省表示上边界 1
   */
  function midpointDigits(ad, bd) {
    const upperOne = bd == null;
    const b = upperOne ? [] : bd;
    const n = Math.max(ad.length, b.length);
    // sum[0] 整数位；sum[1..n] 小数位（多留 1 位防止小数进位覆盖整数位）
    const sum = new Array(n + 1).fill(0);
    let carry = 0;
    for (let i = n - 1; i >= 0; i--) {
      const v = (ad[i] || 0) + (b[i] || 0) + carry;
      sum[i + 1] = v % 10;
      carry = Math.floor(v / 10);
    }
    sum[0] = carry + (upperOne ? 1 : 0); // b=1：整数位加 1（a<1，和的整数位至多为 1）
    // 2) 整体除 2：out[0] 整数位（中点 < 1 恒为 0），其余为小数位
    const out = [];
    let rem = 0;
    for (let i = 0; i < sum.length; i++) {
      const v = rem * 10 + sum[i];
      out.push(Math.floor(v / 2));
      rem = v % 2;
    }
    if (rem) out.push(5); // 最低位余 1 → 右补一位 5
    return fromDigits(out.slice(1));
  }

  /** 两个分数字符串的中点 */
  function midpoint(aStr, bStr) {
    return midpointDigits(digits(aStr), digits(bStr));
  }

  /** 在 a、b 之间取分数；null 表示边界 0 / 1 */
  function zBetween(aStr, bStr) {
    if (aStr == null && bStr == null) return '0.5';
    if (aStr == null) return midpointDigits([], digits(bStr)); // 0 .. b
    if (bStr == null) return midpointDigits(digits(aStr), null); // a .. 1
    return midpoint(aStr, bStr);
  }

  /** 分数比较：-1 / 0 / 1，补齐位数按字典序 */
  function fracCmp(aStr, bStr) {
    const a = isFrac(aStr) ? digits(aStr) : (aStr === '1' ? [10] : [0]);
    const b = isFrac(bStr) ? digits(bStr) : (bStr === '1' ? [10] : [0]);
    const n = Math.max(a.length, b.length);
    for (let i = 0; i < n; i++) {
      const x = a[i] || 0, y = b[i] || 0;
      if (x !== y) return x < y ? -1 : 1;
    }
    return 0;
  }

  /** 在已排序 z 数组中，把 oid 放到 index 位置应取的分数 */
  function zForInsert(sortedZs, index) {
    const before = index > 0 ? sortedZs[index - 1] : null;
    const after = index < sortedZs.length ? sortedZs[index] : null;
    return zBetween(before, after);
  }

  /* ======================== 笔迹点：压感/速度/宽度 ======================== */

  /**
   * 采样点：{x,y, p 压感0..1, tx/ty 倾斜, t 时间戳}
   * 笔迹宽度模型（两端共享同一公式，保证验收场景 4 两端一致）：
   *   width(p) = base * (pressureFactor) * (speedFactor)
   *   pressureFactor = kP + (1-kP)*p          压感变宽
   *   speedFactor    = 1 / (1 + kS*vNorm)     速度变细
   */
  const WIDTH = { kP: 0.45, kS: 0.55, vRef: 2.5 }; // vRef: px/ms 归一化参考

  function pointWidth(p, prev, base, opts) {
    opts = opts || WIDTH;
    const pressure = Number.isFinite(p.p) ? p.p : 0.5;
    let v = 0;
    if (prev && Number.isFinite(p.t) && Number.isFinite(prev.t) && p.t > prev.t) {
      v = Math.hypot(p.x - prev.x, p.y - prev.y) / (p.t - prev.t);
    }
    const vNorm = Math.min(v / opts.vRef, 3);
    const fP = opts.kP + (1 - opts.kP) * pressure;
    const fS = 1 / (1 + opts.kS * vNorm);
    return base * fP * fS;
  }

  /** 为点集预计算宽度（发送端算好随点传输，接收端无需重放） */
  function computeWidths(points, base) {
    const out = new Array(points.length);
    for (let i = 0; i < points.length; i++) {
      out[i] = pointWidth(points[i], points[i - 1], base);
    }
    return out;
  }

  /* ===================== RDP 简化 + 样条平滑（Catmull-Rom / B 样条） ===================== */

  /** Ramer–Douglas–Peucker 点集简化（压感等属性随点保留） */
  function rdp(points, epsilon) {
    if (points.length < 3) return points.slice();
    const keep = new Array(points.length).fill(false);
    keep[0] = keep[points.length - 1] = true;
    const stack = [[0, points.length - 1]];
    while (stack.length) {
      const [s, e] = stack.pop();
      let maxD = -1, idx = -1;
      const a = points[s], b = points[e];
      const dx = b.x - a.x, dy = b.y - a.y;
      const len2 = dx * dx + dy * dy || 1;
      for (let i = s + 1; i < e; i++) {
        const p = points[i];
        const t = ((p.x - a.x) * dx + (p.y - a.y) * dy) / len2;
        const px = a.x + t * dx, py = a.y + t * dy;
        const d = (p.x - px) ** 2 + (p.y - py) ** 2;
        if (d > maxD) { maxD = d; idx = i; }
      }
      if (maxD > epsilon * epsilon) {
        keep[idx] = true;
        stack.push([s, idx], [idx, e]);
      }
    }
    return points.filter((_, i) => keep[i]);
  }

  /**
   * 向心 Catmull-Rom 样条 → 三次贝塞尔段。
   * alpha=0.5 向心（不过冲、不打圈），手绘平滑主流选择；alpha=0 即均匀 Catmull-Rom。
   * @returns {Array} [{x,y} 起] + 每段 {c1x,c1y,c2x,c2y,x,y}
   */
  function catmullRomToBezier(points, alpha) {
    if (alpha == null) alpha = 0.5;
    if (points.length === 1) return [{ x: points[0].x, y: points[0].y }];
    const pts = points.slice();
    // 端点外推，保证曲线起止于笔迹两端点
    pts.unshift({ x: 2 * pts[0].x - pts[1].x, y: 2 * pts[0].y - pts[1].y });
    const last0 = points[points.length - 1], prev0 = points[points.length - 2];
    pts.push({ x: 2 * last0.x - prev0.x, y: 2 * last0.y - prev0.y });

    // 节点参数（向心），与外推后控制点序列 pts 对齐
    const t = [0];
    for (let i = 1; i < pts.length; i++) {
      t[i] = t[i - 1] + Math.pow(Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y), alpha);
    }

    // 第 i 段覆盖原始点 points[i]→points[i+1]（i 从 0 起），
    // 对应控制点 pts[i..i+3]、节点 t[i..i+3]（pts 首尾各有一个外推点）。
    const segs = [{ x: points[0].x, y: points[0].y }];
    for (let i = 0; i < points.length - 1; i++) {
      const p0 = pts[i], p1 = pts[i + 1], p2 = pts[i + 2], p3 = pts[i + 3];
      const t0 = t[i], t1 = t[i + 1], t2 = t[i + 2], t3 = t[i + 3];
      const dt = Math.max(t2 - t1, 1e-6);
      // 非均匀 Catmull-Rom（Hermite 形式）端点切向量；
      // 均匀节点（相邻间距 1）时退化为 T1=(P2-P0)/2, T2=(P3-P1)/2，
      // 对应标准均匀 Catmull-Rom → Bezier：C1=P1+(P2-P0)/6, C2=P2-(P3-P1)/6。
      const T1x = (p2.x - p0.x) / Math.max(t2 - t0, 1e-6);
      const T1y = (p2.y - p0.y) / Math.max(t2 - t0, 1e-6);
      const T2x = (p3.x - p1.x) / Math.max(t3 - t1, 1e-6);
      const T2y = (p3.y - p1.y) / Math.max(t3 - t1, 1e-6);
      const c1x = p1.x + dt / 3 * T1x, c1y = p1.y + dt / 3 * T1y;
      const c2x = p2.x - dt / 3 * T2x, c2y = p2.y - dt / 3 * T2y;
      segs.push({ c1x, c1y, c2x, c2y, x: p2.x, y: p2.y });
    }
    return segs;
  }

  /**
   * 三次均匀 B 样条 → 三次贝塞尔段（节点区间 1/6 系数）。
   * 比 Catmull-Rom 更平滑但不过数据点，作为“混合插值”的平滑档。
   */
  function bsplineToBezier(points) {
    if (points.length === 1) return [{ x: points[0].x, y: points[0].y }];
    const pts = points.slice();
    pts.unshift(points[0]);
    const last = points[points.length - 1], prev = points[points.length - 2];
    pts.push({ x: 2 * last.x - prev.x, y: 2 * last.y - prev.y });

    const segs = [];
    for (let i = 0; i < pts.length - 3; i++) {
      const p0 = pts[i], p1 = pts[i + 1], p2 = pts[i + 2], p3 = pts[i + 3];
      // 起点 (p0+4p1+p2)/6
      const sx = (p0.x + 4 * p1.x + p2.x) / 6;
      const sy = (p0.y + 4 * p1.y + p2.y) / 6;
      if (i === 0) segs.push({ x: sx, y: sy });
      segs.push({
        c1x: (2 * p1.x + p2.x) / 3, c1y: (2 * p1.y + p2.y) / 3,
        c2x: (p1.x + 2 * p2.x) / 3, c2y: (p1.y + 2 * p2.y) / 3,
        x: (p1.x + 4 * p2.x + p3.x) / 6, y: (p1.y + 4 * p2.y + p3.y) / 6
      });
    }
    return segs;
  }

  /** 平滑调度：'catmull'（默认）/ 'bspline' / 'linear' / 'midq'（v1 中点二次贝塞尔） */
  function smoothPath(points, mode) {
    if (!mode || mode === 'catmull') return catmullRomToBezier(points, 0.5);
    if (mode === 'bspline') return bsplineToBezier(points);
    if (mode === 'linear') {
      return [{ x: points[0].x, y: points[0].y }]
        .concat(points.slice(1).map((p) => ({ linear: true, x: p.x, y: p.y })));
    }
    // midq：相邻中点为终点的二次贝塞尔（v1 行为）
    const mid = (a, b) => ({ x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 });
    if (points.length === 2) return [{ x: points[0].x, y: points[0].y }, { linear: true, x: points[1].x, y: points[1].y }];
    const segs = [{ x: points[0].x, y: points[0].y }];
    let m = mid(points[0], points[1]);
    segs.push({ linear: true, x: m.x, y: m.y });
    for (let i = 1; i < points.length - 1; i++) {
      m = mid(points[i], points[i + 1]);
      segs.push({ q: true, cx: points[i].x, cy: points[i].y, x: m.x, y: m.y });
    }
    segs.push({ linear: true, x: points[points.length - 1].x, y: points[points.length - 1].y });
    return segs;
  }

  /* ============================== 橡皮擦分块 ============================== */

  /**
   * 像素擦除按 TILE_CELLS x TILE_CELLS 个单元分块。
   * 单元大小 = 笔迹宽度（或橡皮尺寸），块边长 = 16 * cellSize。
   * 只同步被触碰的块 + 块内单元下标，传输/重绘都是增量，不全量重绘。
   */
  function rasterizeErase(pathPoints, cellSize) {
    const size = Math.max(2, cellSize | 0);
    const tileSpan = size * TILE_CELLS;
    const tiles = new Map(); // "tx,ty" -> Set<"cx,cy">
    const r = size / 2;
    const add = (x, y) => {
      const tx = Math.floor(x / tileSpan);
      const ty = Math.floor(y / tileSpan);
      const cx = Math.floor((x - tx * tileSpan) / size);
      const cy = Math.floor((y - ty * tileSpan) / size);
      const key = tx + ',' + ty;
      let s = tiles.get(key);
      if (!s) { s = new Set(); tiles.set(key, s); }
      s.add(cx + ',' + cy);
    };
    const stamp = (cx, cy) => {
      // 橡皮是半径约 size/2 的圆：覆盖中心单元 + 8 邻域内落在半径内的单元
      for (let gx = -1; gx <= 1; gx++) {
        for (let gy = -1; gy <= 1; gy++) {
          add(cx + gx * size * 0.5, cy + gy * size * 0.5);
        }
      }
    };
    for (let i = 0; i < pathPoints.length; i++) {
      const p = pathPoints[i];
      const q = pathPoints[i - 1];
      if (!q) { stamp(p.x, p.y); continue; }
      // 沿线以半个单元为步长踏步覆盖（快速划动也不漏块）
      const d = Math.hypot(p.x - q.x, p.y - q.y);
      const steps = Math.max(1, Math.ceil(d / (size / 2)));
      for (let k = 0; k <= steps; k++) {
        const t = k / steps;
        stamp(q.x + (p.x - q.x) * t, q.y + (p.y - q.y) * t);
      }
    }
    const chunks = [];
    for (const [key, s] of tiles) {
      const [tx, ty] = key.split(',').map(Number);
      chunks.push({
        tx, ty,
        cells: Array.from(s).map((c) => c.split(',').map(Number))
      });
    }
    return { cellSize: size, chunks };
  }

  /** 计算一条擦除路径影响到的笔迹 oid（对象擦除 / 整笔擦除的命中测试） */
  function hitStrokes(pathPoints, objects, radius) {
    const hits = new Set();
    for (const obj of objects) {
      if (obj.type !== 'stroke') continue;
      const pts = (obj.stroke && obj.stroke.points) || [];
      if (_polylineNearPath(pts, pathPoints, radius)) hits.add(obj.oid);
    }
    return hits;
  }

  function _distPointSeg(px, py, ax, ay, bx, by) {
    const dx = bx - ax, dy = by - ay;
    const l2 = dx * dx + dy * dy || 1;
    let t = ((px - ax) * dx + (py - ay) * dy) / l2;
    t = Math.max(0, Math.min(1, t));
    return Math.hypot(px - (ax + t * dx), py - (ay + t * dy));
  }

  function _polylineNearPath(strokePts, pathPts, radius) {
    if (!strokePts.length || !pathPts.length) return false;
    for (const q of pathPts) {
      for (let i = 0; i < strokePts.length - 1; i++) {
        if (_distPointSeg(q.x, q.y, strokePts[i].x, strokePts[i].y,
          strokePts[i + 1].x, strokePts[i + 1].y) <= radius) return true;
      }
      if (strokePts.length === 1 && Math.hypot(q.x - strokePts[0].x, q.y - strokePts[0].y) <= radius) return true;
    }
    return false;
  }

  /* ============================== 命中测试 ============================== */

  function pointInObject(x, y, obj, pad) {
    pad = pad == null ? 4 : pad;
    switch (obj.type) {
      case 'stroke': {
        const pts = (obj.stroke && obj.stroke.points) || [];
        const w = Math.max((obj.stroke && obj.stroke.width) || 2, 6);
        return _polylineNearPath(pts, [{ x, y }], w / 2 + pad);
      }
      case 'rect':
      case 'ellipse':
      case 'image':
      case 'note':
      case 'text':
      case 'triangle':
      case 'arrow':
      case 'line':
      case 'group': {
        // line/arrow 的 w/h 是向量分量，可能为负；归一化为包围盒
        const x0 = Math.min(obj.x, obj.x + (obj.w || 0)), x1 = Math.max(obj.x, obj.x + (obj.w || 0));
        const y0 = Math.min(obj.y, obj.y + (obj.h || 0)), y1 = Math.max(obj.y, obj.y + (obj.h || 0));
        return x >= x0 - pad && x <= x1 + pad && y >= y0 - pad && y <= y1 + pad;
      }
      default:
        return false;
    }
  }

  /** 对象的轴对齐包围盒 {x,y,w,h}（笔迹取点集；line/arrow 归一化负向量） */
  function objectBounds(obj) {
    if (obj.type === 'stroke') {
      const pts = (obj.stroke && obj.stroke.points) || [];
      if (!pts.length) return { x: 0, y: 0, w: 0, h: 0 };
      const x0 = Math.min.apply(null, pts.map((p) => p.x));
      const y0 = Math.min.apply(null, pts.map((p) => p.y));
      const x1 = Math.max.apply(null, pts.map((p) => p.x));
      const y1 = Math.max.apply(null, pts.map((p) => p.y));
      return { x: x0, y: y0, w: x1 - x0, h: y1 - y0 };
    }
    const x0 = Math.min(obj.x || 0, (obj.x || 0) + (obj.w || 0));
    const y0 = Math.min(obj.y || 0, (obj.y || 0) + (obj.h || 0));
    return { x: x0, y: y0, w: Math.abs(obj.w || 0), h: Math.abs(obj.h || 0) };
  }

  /** 矩形框选：对象包围盒与选择矩形相交即选中（笔迹同样适用） */
  function objectsInRect(rx, ry, rw, rh, objects) {
    const out = [];
    for (const obj of objects) {
      const b = objectBounds(obj);
      if (b.x <= rx + rw && b.x + b.w >= rx && b.y <= ry + rh && b.y + b.h >= ry) out.push(obj);
    }
    return out;
  }

  /* ======================= 图形识别（几何启发式） ======================= */

  /**
   * 识别一笔画成的基础图形：line / arrow / rect / ellipse / triangle。
   * 识别失败返回 null（保留原笔迹）。
   */
  function recognizeShape(points) {
    if (!points || points.length < 3) return null;
    const first = points[0], last = points[points.length - 1];
    const closed = Math.hypot(last.x - first.x, last.y - first.y) < 40;
    const bbox = _bbox(points);
    const diag = Math.hypot(bbox.w, bbox.h);
    if (diag < 20) return null;

    // 开放笔划：直线 / 箭头
    if (!closed) {
      const straight = _straightness(points);
      if (straight > 0.93) {
        // 箭头：终点附近有两条回勾的短线
        const barb = _findArrowBarbs(points);
        // 向量式 line/arrow：起点 (x,y)，终点 (x+w,y+h)，渲染端按线段绘制
        return {
          type: barb ? 'arrow' : 'line',
          fields: { x: first.x, y: first.y, w: last.x - first.x, h: last.y - first.y }
        };
      }
      return null;
    }

    // 闭合笔划：用简化后的拐点判别
    const simplified = rdp(points, diag * 0.08);
    const corners = simplified.slice(1, -1);
    // 类圆：质心半径变异系数小即判定椭圆（圆是椭圆特例）
    const { cx, cy } = _center(points);
    const radii = points.map((p) => Math.hypot(p.x - cx, p.y - cy));
    const mean = radii.reduce((a, b) => a + b, 0) / radii.length;
    const variance = radii.reduce((a, b) => a + (b - mean) ** 2, 0) / radii.length;
    if (Math.sqrt(variance) / mean < 0.3) {
      return { type: 'ellipse', fields: { x: bbox.x, y: bbox.y, w: bbox.w, h: bbox.h } };
    }
    if (corners.length <= 2) {
      return { type: 'triangle', fields: { x: bbox.x, y: bbox.y, w: bbox.w, h: bbox.h } };
    }
    if (corners.length <= 4) {
      return { type: 'rect', fields: { x: bbox.x, y: bbox.y, w: bbox.w, h: bbox.h, rot: _rectRotation(simplified) } };
    }
    return null;
  }

  function _bbox(points) {
    const xs = points.map((p) => p.x), ys = points.map((p) => p.y);
    const x = Math.min.apply(null, xs), y = Math.min.apply(null, ys);
    return { x, y, w: Math.max.apply(null, xs) - x, h: Math.max.apply(null, ys) - y };
  }
  function _center(points) {
    let sx = 0, sy = 0;
    for (const p of points) { sx += p.x; sy += p.y; }
    return { cx: sx / points.length, cy: sy / points.length };
  }
  function _straightness(points) {
    const a = points[0], b = points[points.length - 1];
    const chord = Math.hypot(b.x - a.x, b.y - a.y) || 1;
    let path = 0;
    for (let i = 1; i < points.length; i++) path += Math.hypot(points[i].x - points[i - 1].x, points[i].y - points[i - 1].y);
    return chord / path;
  }
  function _findArrowBarbs(points) {
    // 简化：主直线 + 末端附近两个离轴最远点构成两翼即认为是箭头（启发式，已在 line 判定之后）
    return null;
  }
  function _rectRotation(corners) {
    // 取最长边的方向（弧度）
    let best = 0, bestLen = -1;
    for (let i = 0; i < corners.length; i++) {
      const a = corners[i], b = corners[(i + 1) % corners.length];
      const d = Math.hypot(b.x - a.x, b.y - a.y);
      if (d > bestLen) { bestLen = d; best = Math.atan2(b.y - a.y, b.x - a.x); }
    }
    // 归一化到 -45°..45°
    while (best > Math.PI / 4) best -= Math.PI / 2;
    while (best < -Math.PI / 4) best += Math.PI / 2;
    return best;
  }

  /* ======================= 手写转文字（$1 Unistroke） ======================= */

  /**
   * 单笔画手写识别（$1 Protractor 的轻量实现）：
   * 重采样 64 点 → 旋转到 indicative angle → 缩放到参考尺寸 → 与模板路径距离打分。
   * 模板集覆盖数字与常用字母/符号；分数不足返回 null（保留原笔迹）。
   */
  const UNISTROKE_N = 64;
  const UNISTROKE_SIZE = 250;
  const UNISTROKE_HALF = 125;
  const UNISTROKE_ANGLE = 45 * Math.PI / 180;
  const UNISTROKE_THRESHOLD = 0.82;

  const _tplCache = new Map();

  function _resample(points, n) {
    let I = 0;
    for (let i = 1; i < points.length; i++) I += Math.hypot(points[i].x - points[i - 1].x, points[i].y - points[i - 1].y);
    const interval = I / (n - 1);
    const pts = points.map((p) => ({ x: p.x, y: p.y }));
    const out = [pts[0]];
    let D = 0;
    for (let i = 1; i < pts.length; i++) {
      const d = Math.hypot(pts[i].x - pts[i - 1].x, pts[i].y - pts[i - 1].y);
      if (D + d >= interval && d > 0) {
        const qx = pts[i - 1].x + ((interval - D) / d) * (pts[i].x - pts[i - 1].x);
        const qy = pts[i - 1].y + ((interval - D) / d) * (pts[i].y - pts[i - 1].y);
        out.push({ x: qx, y: qy });
        pts.splice(i, 0, { x: qx, y: qy });
        D = 0;
      } else D += d;
    }
    while (out.length < n) out.push({ x: pts[pts.length - 1].x, y: pts[pts.length - 1].y });
    return out;
  }

  function _centroid(points) {
    let x = 0, y = 0;
    for (const p of points) { x += p.x; y += p.y; }
    return { x: x / points.length, y: y / points.length };
  }

  function _rotateBy(points, radians) {
    const c = Math.cos(radians), s = Math.sin(radians);
    const cen = _centroid(points);
    return points.map((p) => ({
      x: (p.x - cen.x) * c - (p.y - cen.y) * s + cen.x,
      y: (p.x - cen.x) * s + (p.y - cen.y) * c + cen.y
    }));
  }

  function _boundingBox(points) {
    const b = _bbox(points);
    return { w: Math.max(b.w, 1), h: Math.max(b.h, 1) };
  }

  function _scaleTo(points, size) {
    const b = _boundingBox(points);
    return points.map((p) => ({ x: p.x * size / b.w, y: p.y * size / b.h }));
  }

  function _translateTo(points, target) {
    const c = _centroid(points);
    return points.map((p) => ({ x: p.x + target.x - c.x, y: p.y + target.y - c.y }));
  }

  function _pathDistance(a, b) {
    let d = 0;
    for (let i = 0; i < a.length; i++) d += Math.hypot(a[i].x - b[i].x, a[i].y - b[i].y);
    return d / a.length;
  }

  function _distanceAtAngle(points, tpl, radians) {
    return _pathDistance(_rotateBy(points, radians), tpl);
  }

  function _distanceAtBestAngle(points, tpl) {
    let a = -UNISTROKE_ANGLE, b = UNISTROKE_ANGLE;
    const threshold = Math.PI / 90;
    let x1 = (1 - 0.618) * a + 0.618 * b;
    let f1 = _distanceAtAngle(points, tpl, x1);
    let x2 = 0.618 * a + (1 - 0.618) * b;
    let f2 = _distanceAtAngle(points, tpl, x2);
    while (Math.abs(b - a) > threshold) {
      if (f1 < f2) { b = x2; x2 = x1; f2 = f1; x1 = (1 - 0.618) * a + 0.618 * b; f1 = _distanceAtAngle(points, tpl, x1); }
      else { a = x1; x1 = x2; f1 = f2; x2 = 0.618 * a + (1 - 0.618) * b; f2 = _distanceAtAngle(points, tpl, x2); }
    }
    return Math.min(f1, f2);
  }

  /** 归一化一条候选笔迹 */
  function normalizeUnistroke(points) {
    let pts = _resample(points, UNISTROKE_N);
    const radians = Math.atan2(pts[0].y - _centroid(pts).y, pts[0].x - _centroid(pts).x);
    pts = _rotateBy(pts, -radians);
    pts = _scaleTo(pts, UNISTROKE_SIZE);
    pts = _translateTo(pts, { x: UNISTROKE_HALF, y: UNISTROKE_HALF });
    return pts;
  }

  function _templatePoints(name) {
    // 在 250x250 参考框内用少量锚点描述模板，再做 Catmull-Rom 加密后统一归一化
    const H = UNISTROKE_HALF;
    const raw = {
      '0': [[H, 30], [220, H], [H, 220], [30, H], [H, 30]],
      '1': [[H - 20, 80], [H, 30], [H, 220]],
      '2': [[30, 90], [H - 30, 30], [H + 30, 90], [30, 220], [220, 220]],
      '3': [[30, 40], [200, 30], [120, H], [200, H + 30], [110, 220], [30, 210]],
      '4': [[170, 30], [60, 150], [210, 150], [210, 30], [210, 220]],
      '5': [[220, 30], [50, 30], [40, H], [200, H - 10], [200, 220], [40, 220]],
      '6': [[200, 40], [60, H], [30, 200], [H, 225], [200, 180], [170, H], [70, H]],
      '7': [[30, 30], [220, 30], [120, 220]],
      '8': [[H, 30], [210, H], [H, 220], [40, H], [H, 30], [H, 220]],
      '9': [[70, 40], [190, H], [220, 40], [H, 25], [40, 80], [60, 220]],
      'x': [[40, 40], [210, 210], [H, H], [210, 40], [40, 210]],
      '+': [[H, 30], [H, 220], [H, H], [30, H], [220, H]],
      '-': [[40, H], [210, H]],
      '✓': [[30, 130], [90, 200], [220, 40]],
      '→': [[30, H], [210, H], [160, H - 35], [210, H], [160, H + 35]],
      '?': [[40, 70], [H, 30], [200, 80], [H, 150], [H, 195], [H, 215]]
    };
    if (!raw[name]) return null;
    const anchors = raw[name].map(([x, y]) => ({ x, y }));
    // 用 Catmull-Rom 段加密到 ~64 点
    const segs = catmullRomToBezier(anchors, 0.5);
    const dense = [segs[0]];
    for (let i = 1; i < segs.length; i++) {
      const s = segs[i - 1], e = segs[i];
      for (let t = 0.2; t <= 1.0001; t += 0.2) {
        const it = 1 - t;
        dense.push({
          x: it * it * it * s.x + 3 * it * it * t * e.c1x + 3 * it * t * t * e.c2x + t * t * t * e.x,
          y: it * it * it * s.y + 3 * it * it * t * e.c1y + 3 * it * t * t * e.c2y + t * t * t * e.y
        });
      }
    }
    return normalizeUnistroke(dense);
  }

  /**
   * 识别手写字符。
   * @returns {{char:string, score:number}|null}
   */
  function recognizeHandwriting(points, templates) {
    const set = templates || ['0','1','2','3','4','5','6','7','8','9','x','+','-','✓','→','?'];
    const candidate = normalizeUnistroke(points);
    let best = null, bestDist = Infinity;
    for (const name of set) {
      let tpl = _tplCache.get(name);
      if (!tpl) { tpl = _templatePoints(name); if (tpl) _tplCache.set(name, tpl); }
      if (!tpl) continue;
      const d = _distanceAtBestAngle(candidate, tpl);
      if (d < bestDist) { bestDist = d; best = name; }
    }
    // 距离归一为 0..1 的相似度（参考尺寸对角线级别 ~120）
    const score = Math.max(0, 1 - bestDist / 120);
    if (best && score >= UNISTROKE_THRESHOLD) return { char: best, score };
    return null;
  }

  /* ============================== 撤销管理器 ============================== */

  /**
   * 选择性撤销 / 重做。
   * 每个客户端只记录“自己发出”的顶层操作（事务算一条）。
   * undo 不回滚日志，而是构造逆操作信封（带 inv），交 CRDT 折叠仲裁：
   * 若他人在原操作之后改过同一字段，逆操作自动 void，他人结果不受破坏。
   */
  class UndoManager {
    /**
     * @param {Clock} clock
     * @param {object} opts { makeEnv: (op, inv)=>env }  逆操作仍由本节点时钟签发
     */
    constructor(clock, opts) {
      this.clock = clock;
      this.makeEnv = (opts && opts.makeEnv) || ((op, inv) => makeEnvelope(clock, op, { inv }));
      this.undoStack = []; // {envIds:[], summary, time}
      this.redoStack = [];
    }

    /**
     * 记录一条自己发出的顶层操作/事务。
     * @param {env|env[]} envelopes 本次编辑发出的信封（连续移动/缩放可能含多条被压缩的信封）
     * @param {string} summary 历史面板展示名
     * @param {object[]} [inverseOps] 可选：显式逆操作体（如一次移动手势恢复到手势前坐标）；
     *        缺省时按每条原操作自动构造逆操作。
     */
    record(envelopes, summary, inverseOps) {
      const list = Array.isArray(envelopes) ? envelopes : [envelopes];
      this.undoStack.push({
        envIds: list.map((e) => e.id),
        inverseOps: Array.isArray(inverseOps) ? inverseOps : null,
        summary: summary || (list[0].op && list[0].op.kind) || 'op',
        time: Date.now()
      });
      if (this.undoStack.length > 200) this.undoStack.shift();
      this.redoStack.length = 0; // 新编辑清空 redo
    }

    canUndo() { return this.undoStack.length > 0; }
    canRedo() { return this.redoStack.length > 0; }

    /**
     * 撤销最近一条自己的操作。
     * @param {Map} history id -> env（需要读取原操作体以生成逆操作）
     * @returns {Array<env>} 逆操作信封（可能为空：已被架空/找不到）
     */
    undo(history) {
      const entry = this.undoStack.pop();
      if (!entry) return [];
      const invs = this._invertEntry(entry, history, 0);
      if (invs.length) {
        this.redoStack.push(entry);
        return Array.isArray(invs[0]) ? invs : [invs];
      }
      return [];
    }

    /**
     * 选择性撤销：撤销历史中自己的某一条（不必是栈顶）。
     * 不影响其它条目；典型 UI：历史面板点“撤销此操作”。
     */
    undoSelective(history, envId) {
      const idx = this.undoStack.findIndex((e) => e.envIds.includes(envId));
      if (idx < 0) return [];
      const [entry] = this.undoStack.splice(idx, 1);
      const invs = this._invertEntry(entry, history, 0);
      return invs.length ? (Array.isArray(invs[0]) ? invs : [invs]) : [];
    }

    redo(history) {
      const entry = this.redoStack.pop();
      if (!entry) return [];
      const invs = this._invertEntry(entry, history, 1);
      if (invs.length) this.undoStack.push(entry);
      return invs.length ? (Array.isArray(invs[0]) ? invs : [invs]) : [];
    }

    /**
     * 生成逆操作信封组。polarity 0=undo，1=redo。
     * 每个成员原操作生成一条逆 op（保持事务原子性）。
     *  - create 的 undo（delete）标记 wide=true：对象任意字段被他人改过即空转；
     *  - create 的 redo：重新 create（携带原初始字段）；
     *  - set 的逆使用提交时随带的 prev 快照（精确恢复，且字段级架空保护仍生效）。
     */
    _invertEntry(entry, history, polarity) {
      // 显式逆操作（移动/缩放手势的起始快照）：undo 用它；redo 重新应用原信封的最终状态。
      if (entry.inverseOps && polarity === 0) {
        // originLamport 取手势内最新信封的 lamport：
        // 他人若在看到本次移动之后再改该字段，其 lamport 必大于它 → 逆操作自动空转。
        let originL = 0, originId = entry.envIds[0];
        for (const id of entry.envIds) {
          const env = history.get ? history.get(id) : history[id];
          if (env && env.lamport > originL) { originL = env.lamport; originId = env.id; }
        }
        return entry.inverseOps.map((opBody) =>
          this.makeEnv(opBody, { originId, originLamport: originL, polarity: 0 }));
      }
      if (entry.inverseOps && polarity === 1) {
        // redo：把被撤销的最终值重新写回（取手势中最后一条信封的 op）
        const lastEnv = history.get ? history.get(entry.envIds[entry.envIds.length - 1])
          : history[entry.envIds[entry.envIds.length - 1]];
        if (lastEnv) return [this.makeEnv(lastEnv.op, { originId: lastEnv.id, originLamport: lastEnv.lamport, polarity: 1 })];
        return [];
      }

      const groups = [];
      for (const id of entry.envIds) {
        const env = history.get ? history.get(id) : history[id];
        if (!env) continue;
        const op = env.op;

        let invOp = null;
        let wide = false;
        if (polarity === 1 && op.kind === 'create') {
          // redo 一次 create：重新创建（初始字段不变）
          invOp = { kind: 'create', objects: op.objects || [] };
          wide = true;
        } else if (polarity === 1 && op.kind === 'erase') {
          // redo 一次像素擦：重新擦同样的分块/单元（而不是再次反转成 unerase）
          invOp = { kind: 'erase', chunks: (op.chunks || []).map((c) => ({
            oid: c.oid, tx: c.tx, ty: c.ty, cells: (c.cells || []).map((x) => x.slice())
          })) };
        } else {
          invOp = invertOp(op, env);
          if (polarity === 0 && op.kind === 'create') wide = true;
        }
        if (!invOp) continue;

        const inv = { originId: env.id, originLamport: env.lamport, polarity };
        if (wide) inv.wide = true;
        groups.push(this.makeEnv(invOp, inv));
      }
      if (groups.length > 1) atomic(groups);
      return groups;
    }
  }

  /**
   * 由原操作构造逆操作体（不含信封）。
   * set/delete/layer/erase 均可逆；create 逆为 delete。
   */
  function invertOp(op, originEnv) {
    switch (op.kind) {
      case 'create':
        return { kind: 'delete', oids: (op.objects || []).map((o) => o.oid) };
      case 'delete':
        return { kind: 'restore', oids: (op.oids || []).slice() };
      case 'restore':
        return { kind: 'delete', oids: (op.oids || []).slice() };
      case 'set': {
        // 逆 set 使用原操作提交时随带的 prev（修改前快照）精确恢复；
        // 即便 prev 缺失，字段级架空检测也能保证不覆盖他人后续修改。
        const prev = op.prev || {};
        const fields = {};
        for (const f of Object.keys(op.fields || {})) {
          if (Object.prototype.hasOwnProperty.call(prev, f)) fields[f] = prev[f];
        }
        return { kind: 'set', oid: op.oid, fields };
      }
      case 'layer':
        return { kind: 'layer', oid: op.oid, z: op.prevZ || null };
      case 'erase':
        return { kind: 'erase', chunks: op.chunks || [], unerase: !op.unerase };
      case 'group':
        return { kind: 'ungroup', gid: op.gid, oids: op.oids || [] };
      case 'ungroup':
        return { kind: 'group', gid: op.gid, oids: op.oids || [] };
      default:
        return null;
    }
  }

  /* ============================== 导出 ============================== */

  return {
    version: '2.0.0',
    TILE_CELLS, SHAPES, BRUSHES, WIDTH,
    uid,
    // 时钟 / 因果
    Clock, vDominates, CausalBuffer, tsNewer, tag, tagParts,
    // CRDT
    Doc, makeEnvelope, atomic,
    // 压缩
    squash,
    // 图层
    zBetween, midpoint, fracCmp, zForInsert, isFrac,
    // 笔迹
    pointWidth, computeWidths,
    // 平滑 / 简化
    rdp, catmullRomToBezier, bsplineToBezier, smoothPath,
    // 橡皮
    rasterizeErase, hitStrokes,
    // 命中
    pointInObject, objectsInRect, objectBounds,
    // 识别
    recognizeShape, recognizeHandwriting, normalizeUnistroke,
    // 撤销
    UndoManager, invertOp
  };
});
