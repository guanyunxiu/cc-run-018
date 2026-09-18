# 协作白板 v3 · CRDT 内核 + 二进制 P2P 传输

在 v2「CRDT 多人并发内核」之上，v3 解决**传输带宽、消息可靠性、断线续传、快照加速、
慢客户端背压、离线编辑合并**六类问题。原生 HTML/CSS/JS + Canvas 前端，Node.js + ws 后端，
零额外依赖（内核/传输层同时被浏览器与 Node 加载）。v2 JSON 协议完全保留，新旧可互通。

## 目录结构

```
whiteboard-sync/
├── package.json
├── server.js            # 房间/权威 CRDT/因果缓冲/快照/日志压缩/v3 二进制面/信令中继/背压降级/心跳
├── public/
│   ├── kernel.js        # ★ 共享协作内核 v2（CRDT、时钟、撤销、压感、平滑、橡皮、识别）
│   ├── transport.js     # ★ v3 传输层：二进制协议 + 可靠序列层 + 分块 + 背压队列 + VC 增量 + 虚拟网络
│   ├── offline.js       # ★ v3 离线持久化（IndexedDB；Node 内存适配器）
│   ├── mesh.js          # ★ v3 P2P：WebRTC Mesh 信令 + PeerSession 可靠数据面 + NetClient 门面
│   ├── app.js           # 对象化渲染 / 指针手势 / 工具栏 / 网络层（v3 协商，失败回退 v2）
│   ├── index.html
│   └── style.css
├── test-kernel.js       # 262 项内核一致性测试
├── test-transport.js    # 65 项传输层测试（二进制/可靠层/分块/增量/背压/离线）
├── test-frontend.js     # 21 项 v2 前端状态机测试（JSON 回退路径）
├── test-frontend3.js    # 13 项 v3 前端测试（协商/二进制 ack/fseq 校验/degrade/离线）
├── test-smoke.js        # 26 项 v2 真实 ws 协议测试
└── test-smoke3.js       # 30 项 v3 真实服务端 + 虚拟有损 P2P 网络测试
```

## 启动与测试

```bash
npm install
npm start                 # http://localhost:8080/

npm test                  # 六套测试全跑（419 项）
node test-transport.js    # 传输层（无需服务端，含 30%/60% 丢包确定性模拟）
node test-smoke3.js       # v3 协议（自动拉起 server.js）
node test-frontend3.js    # v3 前端（stub DOM，模拟二进制服务器）
```

## 一、数据面 / 控制面分离（需求 1）

- **WebSocket 只跑信令与控制**：协议握手、join、snapshot/delta、ack、WebRTC
  offer/answer/candidate 中继（`signal`）、成员 roster（`peers`）、大对象**引用**登记
  （`bigAnnounce`，只存 hash/size/持有者，字节不过服务器）、degrade/resume/rate。
- **大操作与媒体流走 WebRTC DataChannel，P2P 直连低延迟**：编码后超过
  `LARGE_BYTES`(8KB) 的信封（长笔迹、图片）算 SHA-256 分块，经 unordered DC 直传；
  全互联 Mesh 按 sessionId 字典序由小端发起 offer，无 glare；信令全部经 WS 中继。
  无 P2P 对端（NAT 打不通/仅一人）时短等后自动 **WS 兜底**，编辑不丢。
- 字节流不经过服务器：v3 服务端的大对象表只存引用（blobId/holders），新成员据此
  向持有者 P2P 拉块。

## 二、二进制协议（需求 2）

自定义 ArrayBuffer 帧（魔数 `W B` + major + type + flags），见 `transport.js`：

- 笔迹点云列式紧凑打包：**坐标/压感/宽度 Float32LE，时间 Uint32LE**，属性位掩码
  （缺列不写）；键名走热键字典（1 字节）+ varint。
- 橡皮分块：每块 `oid + zigzag(tx,ty)`，块内单元 `cx,cy ∈ 0..15` 两坐标压 1 字节。
- 非整数数值优先 Float32（偏差超 1e-6 自动退 Float64），整数用 zigzag varint。
- 实测 500 点笔迹帧体积约为 JSON 的 **30%**（见 `test-transport.js [2]`）。

## 三、可靠序列层（需求 7、8）

DataChannel 用 `ordered:false, maxRetransmits:0`（SCTP 不额外保证），上层 `ReliableLink`：

| 机制 | 实现 |
| --- | --- |
| 消息去重 | 每方向单调 seq；`seq ≤ 已投递水位` 或窗口内已见过 → 丢弃 |
| 乱序重排 | 缺口前缓存 `reorder` 窗口，连续后按序冲刷，连锁解锁同轮完成 |
| 丢失重传 | 发送端 RTO 指数退避重发 `DATA(flags=rexmit)`；接收端发 `DATA_NACK` 显式索要缺口 |
| 选择确认 | `DATA_ACK {next, sacks[]}`，乱序到达的块直接 SACK，不用等水位 |
| 旧消息防覆盖 | 水位只增不减；stale ACK 不倒退；服务端扇出另有独立 **fseq** 校验 |

`VirtualNetwork`（可设 loss/jitter/dup/延迟的确定性内存网络）在 30% 丢包 + 乱序 + 重复
下验证 120 条消息全部有序精确一次（`test-transport.js [6]`），60% 丢包下 NACK 恢复（[7]）。

大对象另有 `BlobAssembler`：乱序收块、重复块丢弃、按连续区间完成重组。

## 四、增量同步（需求 3）

- join/resume 携带 **lastSeq + 版本向量 VC**；服务端 `sendSyncState` 决策：
  1. `lastSeq ≥ room.seq`：仅握手（空房也回空基线快照）；
  2. 在日志窗口内：只回 **delta**（seq 之后且 VC 判定未覆盖的信封）；
  3. 落后于周期快照：快照 + 快照后少量重放；
  4. 太旧（日志已裁剪）：全量基线快照 + 剩余日志。
- P2P 面：链路建立互发 `peerHello(版本+VC)`，`missingForPeer(log, theirVC)` 按
  **发送者本地计数**挑出缺失信封增量 gossip；已持有 id 幂等丢弃。

## 五、快照加速（需求 4）

服务端**每 100 个物化操作**落一份快照（`SNAPSHOT_EVERY`，保留最近 3 份），
快照即 `Doc.snapshot()`（含逐单元橡皮 protector，不削弱选择性撤销）。
新客户端先加载快照，再重放快照 seq 之后的少量操作。手动触发：`POST /api/snapshot?roomId=`。

## 六、背压处理（需求 5）

- **客户端发送队列** `CoalescingQueue`：相同 `squashKey` 的高频手势帧在发出前被最新帧
  替换（coalesce）；高水位先淘汰可合并中间帧，仍满则 `blocked`（生产者停止生成，
  NetClient 监听 `bufferedAmount` 低水位续发）；UI 提示「网络拥塞·降帧中」。
- **服务端广播队列** `OutboundMeter`：每个扇出帧分配 fseq 并按字节记账，客户端 ACK
  水位后释放；在途字节超 512KB（或积压超 1024 帧）判定慢客户端。
- **慢客户端自动降级为快照同步**：丢弃积压 → 发 `degrade` → 发一份最新快照重置 →
  客户端 ACK 该快照后恢复正常增量。降级期间不收增量，杜绝快照/增量竞态与旧状态覆盖。
  测试钩子：`POST /api/degrade?roomId=&sid=`。

## 七、离线编辑（需求 6）

- 本地操作乐观预提交的同时写入 **IndexedDB**（store `pending-ops`，按信封 id 主键）；
  收到服务端 ACK 才删除。崩溃/断电/断网都不丢编辑。
- 时钟（local 计数 / lamport / VC）存 store `meta`，重开页面后信封 id
  `clientId:local` **继续单调、绝不复用**。
- 重连后 `restoreOfflineAndFlush`：本地缺失先幂等重放进 Doc，再按原 id 经
  `replayOffline` 合并上传；服务端 `applied` 集合幂等去重，重复包丢弃，
  绝不会产生重复对象（`test-smoke3.js [7]`）。
- Node/不支持 IndexedDB 的环境自动用同名内存适配器。

## 八、协议版本化（需求 9）

- 帧头魔数后 1 字节 major；握手块 `{name:'wb3', major, minor, caps}`。
- `negotiate`：异名协议拒绝；**major 不一致 → 文本 error + WS close 4001**；
  新客户端 minor 更新时**降级**到服务端 minor。
- 未知帧类型/未知 op kind 跳过不崩（向前兼容）；键字典追加不复用。
- v3 浏览器协商失败（旧服务器无 proto 响应）**自动回退 v2 JSON 全流程**，
  同房间 v2/v3 客户端互通（服务端按 `client.bin` 分别扇出）。

## 二进制帧一览（`/ws`）

```
C→S  hello(隐式于JSON join.proto) · join{roomId,userId,lastSeq,vc} · ops{fseq?,envelopes}
     ack{ids,lastSeq,fseq,sacks} · resume{lastSeq,vc} · ping · rate
     signal{to,payload} · bigAnnounce{refs}                      // 信令/控制，无大字节
S→C  joined{major,minor,caps,sessionId,lastSeq,snapEvery,vc} · snapshot{fseq,...} ·
     delta{fseq,fromSeq,envelopes} · ops{fseq,envelopes} · ack ·
     peers{sessionIds} · signal{from,payload} · degrade{reason,seq} · pong · error
P2P  DATA{seq,payload=peerHello|ops|bigChunk|bigReq|bigAck} + DATA_ACK/DATA_NACK
```

HTTP（新增）：`POST /api/snapshot?roomId=`、`POST /api/degrade?roomId=&sid=`；
`GET /api/rooms`、`/api/room` 增加 `snapshots/blobRefs/binMembers/degraded` 字段。

## 九、v2 既有能力（保持不变）

LWW-Element-Map CRDT（seq 不参与冲突仲裁）、版本向量因果投递、选择性撤销（逆操作 +
字段级/单元级架空检测）、原子事务、squashKey 操作压缩、分数 z 序、压感笔迹宽度模型、
RDP + Catmull-Rom/B 样条、像素/对象/整笔橡皮、$1 手写识别。详见下文 v2 章节。

---

<details>
<summary>v2 设计详节（CRDT 一致性模型 / 撤销 / 压感 / 橡皮 / 识别）</summary>

## 一、一致性模型（需求 1、3）

- **LWW-Element-Map CRDT**：白板是一组对象（`oid`），每个字段是一个 LWW 寄存器，
  写入带 `(lamport, clientId)` 时间戳。任意副本对同一组信封折叠出**相同结果**，
  与网络到达顺序无关 —— 这就是「不能只靠服务端 seq 排序」的核心：**seq 仅用于日志排序/观测，不参与冲突仲裁**。
- 每个信封携带：
  - `clientId`：操作者；
  - `lamport`：Lamport 逻辑时钟；
  - `clock`：版本向量（依赖向量），声明「我见过各节点的第几条操作」；
  - `id`：`<clientId>:<localCount>`，全局幂等。
- **因果投递**：`CausalBuffer` 用 happens-before 规则（发送者序号连续 + 依赖向量不超前）
  保证 `create` 必先于后续 `set`；缺依赖的信封先挂起，补齐后按序冲刷。

## 二、对象模型与操作类型（需求 2）

对象类型：`stroke / rect / ellipse / triangle / arrow / line / text / note / image / group`。
操作 `kind`：`create / set / delete / restore / group / ungroup / layer / erase`，
覆盖图形、文本、便签、图片、选择、移动、缩放、旋转、删除、图层调整、组合、解组。

- 移动/缩放/旋转统一写对象的仿射变换字段 `tr{tx,ty,sx,sy,r}`，对笔迹和图形一视同仁。
- 图层顺序用**十进制分数索引**（fractional indexing）：`zBetween(a,b)` 逐位长除取严格中点，
  并发同位置插入产生相同键，再由 `(lamport,clientId)` 确定平局，永不重排。

## 三、选择性撤销（需求 4，验收场景 2）

撤销**不回滚历史**，而是发一条「逆操作」信封（`op.inv = {originId, originLamport, polarity}`）。
内核折叠逆写入时做**架空检测**：

> 若原操作之后（含并发更大 lamport）存在**他人的普通写入**到同一字段，逆操作空转（void）。
> 他人的撤销/重做不算新鲜意图；自身的记录不阻塞；保护是**字段级**的（`create` 的撤销是对象级）。

因此：
- 只撤销自己的操作（`UndoManager` 只记录本人发出的顶层编辑/事务）；
- A 撤销自己旧笔迹时，若 B 已修改该区域，**A 的撤销不会覆盖 B 的结果**；
- 没被他人碰过的字段正常恢复，互不影响（per-field）。
- 历史面板支持对任意一条自己的历史操作做**选择性撤销**（`undoSelective`），不限于栈顶。

## 四、事务与原子操作组（需求 5，验收场景 3）

多对象编辑（一次粘贴多个元素、一次移动多个选中对象）的多个信封用 `WB.atomic()` 绑定同一 `txnId`。
因果缓冲保证整组要么一起就绪、要么全部挂起 —— **其他客户端要么全部看到移动，要么看不到，绝不会只移动一半**。

## 五、操作压缩（需求 6）

连续移动/缩放的高频帧都带 `squashKey`（精确到「手势 × 对象」），
`WB.squash(log)` 把相同 key 的一串信封折叠为携带最终状态的一条。
服务端超过阈值自动压缩，并提供 `POST /api/compact?roomId=`；
超出硬上限时建立快照水位（snapshot watermark），日志体积有界，晚加入者走快照 + 增量。

## 六、压感笔迹（需求 7，验收场景 4）

采样点升级为 `{x, y, p 压感, tx/ty 倾斜, t 时间戳, w 宽度}`。
宽度由两端共享的同一确定性公式计算，发送端预算好 `w` 随点传输：

```
w(p) = base · (kP + (1-kP)·pressure) · 1/(1 + kS·v/vRef)
        └── 压感变宽 ──┘                └── 速度变细 ──┘
```

接收端无需重放，逐点宽度两端逐值一致。

## 七、平滑与简化（需求 8）

- 发送前用 **Ramer–Douglas–Peucker（RDP）** 简化点集（保留压感等属性）；
- 渲染支持 **向心 Catmull-Rom**（默认，已验证均匀情形退化为标准 Bezier 系数）、
  **三次均匀 B 样条**、线性、以及 v1 的中点二次贝塞尔。

## 八、橡皮擦（需求 9，验收场景 5）

- **像素擦**：笔迹被划成 `16×16` 个单元的块（块边长 = `cellSize × 16`，cellSize 取笔迹宽度）。
  只同步被触碰的块 + 块内单元下标（`erase {chunks:[{oid,tx,ty,cells:[[cx,cy],…]}]}`），
  增量同步、增量重绘，不全量重画；擦除状态在 CRDT 里按**单元（cell）粒度**做 LWW 寄存器折叠，撤销按格恢复。
  - 选择性撤销也是单元级：A、B 先后擦同一分块里的不同单元，A 撤销只恢复 A 擦过的格子，
    B 擦的格子保留；只有他人在 A 之后**重擦同一格**时，该格的恢复才空转（同块其它格不连坐）。
  - 快照/日志压缩时每个单元除当前获胜记录外还保留各他人最新写入（protector），
    晚加入者与水位之后重放的撤销仍得到逐单元一致的结果。
- **对象擦**：沿擦除路径命中整个对象 → `delete`（可多对象事务）。
- **整笔擦**：命中笔迹 → `delete`。

## 九、笔刷与识别（需求 10）

- 笔刷：钢笔、**荧光笔**（半透明 multiply）、**虚线**（setLineDash）、**纹理笔**（沿线盖点）；
- 工具：箭头、矩形/椭圆/三角/直线（一笔**图形识别**或直接插入）；
- **手写转文字**：内置 $1 Unistroke 识别器（重采样 64 点 → 旋转归一 → 缩放 → 黄金角搜索），
  内置数字与常用符号模板，命中阈值后笔迹转 `text` 对象（原笔迹不入库，无重复）。

## 消息协议（JSON over WebSocket，`/ws`）

```jsonc
// C → S
{ "type": "join", "roomId": "r1", "userId": "u-a", "lastSeq": 0 }
{ "type": "ops",  "envelopes": [ /* 单条或同一 txnId 的事务原子组 */ ] }
{ "type": "ping" }

// 信封
{ "id": "u-a:7", "clientId": "u-a", "lamport": 7,
  "clock": { "u-a": 7, "u-b": 3 },
  "txnId": "txn-…", "squashKey": "move:<gesture>:<oid>",
  "op": { "kind": "set", "oid": "obj-1", "fields": {"x": 100}, "prev": {"x": 80} } }

// S → C
{ "type": "joined",  "roomId": "r1", "userId": "u-a", "lastSeq": 16 }
{ "type": "snapshot","watermark": 0, "snapshot": { /* 折叠后的对象/擦除/组 + known VC */ },
                      "envelopes": [ /* 水位之后仍在日志的增量 */ ] }
{ "type": "ops",     "envelopes": [ /* 因果广播（不含发送者自己） */ ] }
{ "type": "ack",     "ids": ["u-a:7"], "lastSeq": 16 }
{ "type": "pong" | "error" }
```

HTTP：`GET /api/rooms`、`GET /api/room?roomId=`、`GET /api/compact?roomId=`（运维/测试压缩）。

## 验收场景 ↔ 测试

| 验收场景 | 测试 |
| --- | --- |
| 1. 三客户端同画并发，最终一致、无重复 | `test-smoke.js [1]`、`test-kernel.js [1]`（6 种乱序全收敛） |
| 2. A 撤销旧笔迹不破坏 B 的后续修改 | `test-smoke.js [2]`、`test-kernel.js [2]/[2b]` |
| 3. 多对象移动原子（全有或全无） | `test-smoke.js [3]`、`test-kernel.js [3]`（事务缺半时 0/5 可见） |
| 4. 压感笔迹两端宽度一致 | `test-smoke.js [4]`、`test-kernel.js [4]` |
| 5. 橡皮分块两端一致 | `test-smoke.js [5]`、`test-kernel.js [8b]` |

## 设计要点

- **为什么 seq 不再解决冲突**：seq 是单点全序，无法表达「B、C 都基于 A 的状态并发修改」这种偏序；
  CRDT 让每个副本本地确定性收敛，服务端只负责定序、广播、物化快照。
- **为什么撤销用逆操作而不是删日志**：协作系统不能改写他人已收到的历史；逆操作是一条普通新操作，
  同样参与因果/广播/压缩，架空检测保证「选择性」——只在不与他人意图冲突时生效。
- **为什么变换用独立 `tr` 而不是改 x/y**：笔迹是点云没有包围盒基准，统一仿射字段让移动/缩放/旋转
  对所有类型语义一致，命中测试用逆变换把屏幕点映回对象局部坐标。
- **乐观预提交 + 幂等重发**：本地操作立即上屏，未 ack 的断线期间积压，重连后按原 `id` 重发，
  服务端 `applied` 集合幂等去重，绝不重复入库。

> 说明：白板内容保存在服务端内存中，进程重启后清空（重连客户端以服务端快照为准）；
> 持久化可在 `Doc.snapshot()` 之上接入 Redis/数据库。

</details>

## v3 验收场景 ↔ 测试

| 需求 | 测试 |
| --- | --- |
| 1. WS 仅信令、大操作走 DataChannel P2P | `test-smoke3.js [6]`（blob 分块 P2P 重组，字节不过服务器）、`[8]` 混合互通 |
| 2. 二进制协议 Float32/Uint32 | `test-transport.js [1][2]`（500 点帧 ≈ JSON 30%，时间精确、坐标 ε 内） |
| 3. lastSeq + VC 增量同步 | `test-smoke3.js [3]`（窗口内只回 ≤3 条 delta） |
| 4. 每 N 操作快照加速 | `test-smoke3.js [4]`（快照 + 0 条重放即见当前对象） |
| 5. 发送/广播队列背压 + 慢客户端降级 | `test-transport.js [12][13]`、`test-smoke3.js [5]`（degrade→快照→ACK 恢复） |
| 6. IndexedDB 离线编辑合并 | `test-transport.js [14]`、`test-smoke3.js [7]`、`test-frontend3.js`（时钟持久化） |
| 7. 去重/乱序/重复丢弃/丢失重传 | `test-transport.js [6][7][9][10]`（30%/60% 丢包、乱序冲刷、块重组） |
| 8. 收发队列序号校验防旧覆盖 | `test-transport.js [8]`、`test-frontend3.js`（fseq=3/4 在水位 5 后丢弃） |
| 9. 协议版本化降级/拒绝 | `test-transport.js [5]`、`test-smoke3.js [1]`（major 拒绝 close 4001、minor 降级） |

