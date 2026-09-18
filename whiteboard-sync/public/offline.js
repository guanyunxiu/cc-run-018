'use strict';

/* ===========================================================================
 * 协作白板 v3 - 离线编辑持久化（IndexedDB）
 *
 *  - 本地操作一旦生成（乐观预提交）即持久化到 IndexedDB（store: pending-ops），
 *    未收到服务端 ACK 前不删除；浏览器崩溃 / 断电 / 断网都不丢编辑。
 *  - 本地时钟（local 计数 / lamport / VC）一并持久化（store: meta），
 *    重开页面后信封 id（clientId:local）继续单调，绝不复用旧 id。
 *  - 重连后 NetClient 读取全部未确认信封，按 lamport 排序合并上传；
 *    服务端按 env.id 幂等去重，重复包丢弃，不会产生重复对象。
 *  - Node / 测试环境自动使用内存实现（同名 API），浏览器为 IndexedDB。
 *
 * UMD：Node(require) / 浏览器(<script>) 均可加载。
 * ========================================================================= */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.WBO = factory();
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const DB_NAME = 'wb3-offline';
  const DB_VERSION = 1;
  const STORE_OPS = 'pending-ops';
  const STORE_META = 'meta';
  const META_KEY = 'clock';

  /* --------------------------- 内存实现（Node/测试） --------------------------- */

  class MemoryOffline {
    constructor() { this.ops = new Map(); this.meta = new Map(); }
    static open() { return Promise.resolve(new MemoryOffline()); }
    putPending(env) { this.ops.set(env.id, env); return Promise.resolve(); }
    putPendingAll(envs) { for (const e of envs) this.ops.set(e.id, e); return Promise.resolve(); }
    removePending(ids) {
      for (const id of ids) this.ops.delete(id);
      return Promise.resolve();
    }
    allPending() {
      const list = Array.from(this.ops.values());
      list.sort((a, b) => a.lamport - b.lamport || (a.id < b.id ? -1 : 1));
      return Promise.resolve(list);
    }
    pendingCount() { return Promise.resolve(this.ops.size); }
    saveClock(state) { this.meta.set(META_KEY, state); return Promise.resolve(); }
    loadClock() { return Promise.resolve(this.meta.get(META_KEY) || null); }
    clear() { this.ops.clear(); this.meta.clear(); return Promise.resolve(); }
  }

  /* --------------------------- IndexedDB 实现（浏览器） --------------------------- */

  /** 极简 Promise 化 IndexedDB 封装，无外部依赖；不支持时返回 null */
  class IDBOffline {
    constructor(db) { this.db = db; }

    static open() {
      return new Promise((resolve) => {
        let indexedDB = null;
        try {
          indexedDB = (typeof self !== 'undefined' && (self.indexedDB || self.mozIndexedDB)) ||
            (typeof globalThis !== 'undefined' && globalThis.indexedDB) || null;
        } catch (_) { indexedDB = null; }
        if (!indexedDB) { resolve(null); return; }

        let req;
        try { req = indexedDB.open(DB_NAME, DB_VERSION); } catch (_) { resolve(null); return; }
        req.onupgradeneeded = () => {
          const db = req.result;
          if (!db.objectStoreNames.contains(STORE_OPS)) db.createObjectStore(STORE_OPS, { keyPath: 'id' });
          if (!db.objectStoreNames.contains(STORE_META)) db.createObjectStore(STORE_META);
        };
        req.onsuccess = () => resolve(new IDBOffline(req.result));
        req.onerror = () => resolve(null);
      });
    }

    _tx(store, mode) { return this.db.transaction(store, mode).objectStore(store); }

    static _wrap(req) { return new Promise((resolve, reject) => {
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    }); }

    putPending(env) { return IDBOffline._wrap(this._tx(STORE_OPS, 'readwrite').put(env)).then(() => {}); }
    async putPendingAll(envs) {
      const tx = this.db.transaction(STORE_OPS, 'readwrite');
      const st = tx.objectStore(STORE_OPS);
      for (const e of envs) st.put(e);
      await new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = () => rej(tx.error); });
    }
    async removePending(ids) {
      const tx = this.db.transaction(STORE_OPS, 'readwrite');
      const st = tx.objectStore(STORE_OPS);
      for (const id of ids) st.delete(id);
      await new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = () => rej(tx.error); });
    }
    async allPending() {
      const list = await IDBOffline._wrap(this._tx(STORE_OPS, 'readonly').getAll());
      list.sort((a, b) => a.lamport - b.lamport || (a.id < b.id ? -1 : 1));
      return list;
    }
    async pendingCount() { return IDBOffline._wrap(this._tx(STORE_OPS, 'readonly').count()); }
    saveClock(state) {
      return IDBOffline._wrap(this._tx(STORE_META, 'readwrite').put(state, META_KEY)).then(() => {});
    }
    loadClock() { return IDBOffline._wrap(this._tx(STORE_META, 'readonly').get(META_KEY)); }
    async clear() {
      const tx = this.db.transaction([STORE_OPS, STORE_META], 'readwrite');
      tx.objectStore(STORE_OPS).clear();
      tx.objectStore(STORE_META).clear();
      await new Promise((res, rej) => { tx.oncomplete = res; tx.onerror = () => rej(tx.error); });
    }
  }

  /** 打开离线存储：浏览器优先 IndexedDB，失败或 Node 环境回退内存实现 */
  async function openOffline(opts) {
    if (opts && opts.memory) return MemoryOffline.open();
    const db = await IDBOffline.open();
    return db || MemoryOffline.open();
  }

  return { openOffline, MemoryOffline, IDBOffline, DB_NAME, STORE_OPS };
});
