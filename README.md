# 🌐 P2P 配置同步系统

基于 WebRTC DataChannel 的去中心化配置同步系统，无需中心服务器存储配置数据。

## ✨ 功能特性

### 核心功能
- **P2P 连接**: 基于 WebRTC DataChannel 实现浏览器间直接通信
- **信令服务器**: 仅用于交换 SDP 信令，不存储任何配置数据
- **CRDT 算法**: 使用无冲突复制数据类型解决并发修改冲突
- **DAG 历史**: 变更历史以有向无环图存储，支持分支合并
- **撤销/重做**: 完整的操作历史回溯功能

### 技术亮点
- **去中心化**: 配置数据仅在对等节点间同步，无中心存储
- **实时同步**: 毫秒级配置变更广播
- **冲突解决**: 基于 Lamport 时间戳 + 节点ID的确定性冲突解决
- **离线支持**: 节点离线后重新连接自动同步状态
- **多节点支持**: 支持 N 个节点形成网状拓扑

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────────┐
│                     信令服务器 (仅交换SDP)                    │
│              Node.js + WebSocket (端口 8080)                  │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              │  SDP 信令交换
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│   浏览器节点A  │     │   浏览器节点B  │     │   浏览器节点C  │
│               │     │               │     │               │
│  CRDTMap      │◄───►│  CRDTMap      │◄───►│  CRDTMap      │
│  DAGHistory   │     │  DAGHistory   │     │  DAGHistory   │
│  P2PManager   │     │  P2PManager   │     │  P2PManager   │
│  ConfigSync   │     │  ConfigSync   │     │  ConfigSync   │
└───────────────┘     └───────────────┘     └───────────────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              │
                              ▼
                    WebRTC DataChannel
                    (P2P 网状拓扑连接)
```

## 📁 项目结构

```
tss1/
├── server/
│   └── signaling.js          # 信令服务器 (Node.js + WebSocket)
├── client/
│   ├── index.html            # 前端界面
│   ├── css/
│   │   └── style.css         # 样式文件
│   └── js/
│       ├── crdt.js           # CRDT 算法实现 (CRDTMap, CRDTSet)
│       ├── dag-history.js    # DAG 历史管理与撤销/重做
│       ├── p2p-manager.js    # WebRTC P2P 连接管理
│       ├── config-sync.js    # 配置同步协调器
│       └── app.js            # 前端应用逻辑
├── package.json              # 项目依赖配置
└── README.md                 # 项目文档
```

## 🚀 快速开始

### 1. 安装依赖

```bash
npm install
```

### 2. 启动信令服务器

```bash
npm start
```

服务器将在 `http://localhost:8080` 启动。

### 3. 测试 P2P 同步

1. 打开浏览器，访问 `http://localhost:8080`
2. 在新标签页中再次打开 `http://localhost:8080`（第二个节点）
3. 在两个页面中使用相同的房间ID（默认: `default-room`）点击"连接"
4. 在任一页面修改配置，观察另一页面实时同步

## 🔧 API 文档

### ConfigSync 类

主要的配置同步接口，协调 CRDT、DAG 历史和 P2P 连接。

```javascript
// 创建配置同步实例
const configSync = new ConfigSync(nodeId, roomId, signalingUrl, initialConfig);

// 连接到信令服务器
await configSync.connect();

// 断开连接
configSync.disconnect();

// 配置操作
configSync.set(key, value, message);     // 设置配置项
configSync.get(key);                       // 获取配置项
configSync.delete(key, message);           // 删除配置项
configSync.getConfig();                    // 获取全部配置

// 撤销/重做
configSync.undo();                         // 撤销操作
configSync.redo();                         // 重做操作
configSync.canUndo();                      // 是否可撤销
configSync.canRedo();                      // 是否可重做

// 导入/导出
configSync.exportConfig();                 // 导出JSON配置
configSync.importConfig(jsonString);       // 导入JSON配置

// 批量操作
configSync.batchSet(entries, message);     // 批量设置配置项

// 历史查询
configSync.getHistory();                   // 获取变更历史
configSync.getDAGStructure();              // 获取DAG结构
configSync.getHistoryNode(nodeId);         // 获取历史节点详情
configSync.getStatus();                    // 获取系统状态
```

### CRDTMap 类

基于 Lamport 时间戳的无冲突映射数据类型。

```javascript
const crdt = new CRDTMap(nodeId);

crdt.set(key, value);       // 设置值，返回操作对象
crdt.get(key);              // 获取值
crdt.delete(key);           // 删除值，返回操作对象
crdt.getAll();              // 获取所有键值对

crdt.applyRemote(operation);    // 应用远程操作
crdt.merge(otherCrdt);          // 合并另一个CRDT的状态
```

### DAGHistory 类

有向无环图存储变更历史，支持撤销/重做。

```javascript
const history = new DAGHistory();

history.createRoot(initialState);       // 创建根节点
history.addOperation(op, newState);     // 添加操作节点
history.mergeOperations(ops, state);    // 合并操作（多父节点）

history.undo();         // 撤销
history.redo();         // 重做
history.canUndo();      // 是否可撤销
history.canRedo();      // 是否可重做

history.getHistory();           // 获取历史列表
history.getDAGStructure();      // 获取DAG拓扑结构
history.getCurrentState();      // 获取当前状态
history.findCommonAncestor(id1, id2);   // 查找最近公共祖先
```

### P2PManager 类

WebRTC DataChannel 连接管理。

```javascript
const p2p = new P2PManager(nodeId, roomId, signalingUrl);

await p2p.connect();        // 连接到信令服务器
p2p.disconnect();           // 断开所有连接

p2p.sendToPeer(peerId, message);   // 发送消息给指定节点
p2p.broadcast(message);             // 广播消息给所有节点

p2p.getConnectedPeers();    // 获取已连接节点列表
p2p.getPeerCount();         // 获取在线节点数量
p2p.getStatus();            // 获取连接状态
```

## 🧠 CRDT 冲突解决算法

### 原理
使用 **Last-Write-Wins (LWW)** 策略，结合：
1. **Lamport 逻辑时钟**: 保证事件的偏序关系
2. **节点ID**: 当时钟相同时，使用节点ID打破平局

### 操作元数据
每个操作包含：
```javascript
{
  id: "op-0000000001-nodeA-abc123",
  type: "set",              // "set" 或 "delete"
  key: "app.title",
  value: "My App",
  timestamp: "0000000001-nodeA",
  nodeId: "nodeA",
  metadata: {
    lamport: 1,             // Lamport 时钟
    nodeId: "nodeA"
  }
}
```

### 冲突解决规则
```
IF newOp.lamport > currentOp.lamport:
    应用新操作
ELSE IF newOp.lamport < currentOp.lamport:
    忽略新操作
ELSE:
    // 时钟相同，比较节点ID
    IF newOp.nodeId > currentOp.nodeId:
        应用新操作
    ELSE:
        忽略新操作
```

## 🌲 DAG 有向无环图历史

### 数据结构
```
Root (初始状态)
  │
  ▼
Op1 (设置 app.version = "1.0.0")
  │
  ▼
Op2 (设置 ui.theme = "dark")
  │
  ▼
Op3 (批量更新) ◄──┐
  │                │
  ▼                │
Merge (合并远程操作)
  │
  ▼
Op4 (撤销 Op2)
```

### 撤销/重做机制
- **撤销栈**: 记录可撤销的操作ID
- **重做栈**: 记录已撤销的操作ID
- **状态恢复**: 撤销时将状态回滚到父节点的完整状态

## 🔌 消息协议

### 信令消息（WebSocket）
```javascript
// 加入房间
{ type: 'join', clientId, roomId }

// SDP 交换
{ type: 'offer', from, targetId, payload: RTCSessionDescription }
{ type: 'answer', from, targetId, payload: RTCSessionDescription }
{ type: 'ice-candidate', from, targetId, payload: RTCIceCandidate }
```

### 数据通道消息（WebRTC）
```javascript
// 配置操作
{
  type: 'operation',
  operation: { id, type, key, value, ... },
  historyNodeId: 'op-xxx'
}

// 状态同步（新节点加入时）
{
  type: 'state-sync',
  crdtState: { state, metadata, clock },
  history: { nodes, edges, currentHead, ... },
  fromNodeId: 'nodeA'
}
```

## 🧪 测试场景

### 基本功能测试
1. **单节点操作**: 添加、修改、删除配置项，验证撤销/重做
2. **双节点同步**: 两个浏览器标签页，验证配置实时同步
3. **并发修改**: 两个节点同时修改同一配置项，验证CRDT冲突解决
4. **三节点网状**: 三个节点互相连接，验证广播和网状拓扑

### 高级场景测试
1. **离线重连**: 节点A离线时修改，重新连接后自动同步
2. **状态合并**: 节点A和B独立修改后，连接时自动合并状态
3. **DAG分支**: 两个节点独立产生新操作，合并后形成多父节点

## 🔒 安全说明

1. **信令服务器**: 仅转发 SDP 信令，不存储任何配置数据
2. **P2P 通信**: WebRTC DataChannel 默认加密（DTLS-SRTP）
3. **节点身份**: 节点ID自动生成并存储在 localStorage
4. **跨源限制**: 需在相同域名下运行，或配置适当的 CORS

## 📝 注意事项

1. **浏览器兼容性**: 需要支持 WebRTC 的现代浏览器（Chrome、Firefox、Safari、Edge）
2. **STUN/TURN**: 默认使用 Google STUN 服务器，生产环境建议部署自己的 TURN 服务器
3. **房间隔离**: 不同房间ID的节点无法互相发现
4. **内存限制**: 历史记录会持续增长，长期运行需考虑历史裁剪

## 🔮 扩展方向

- [ ] 配置项级别的权限控制
- [ ] 支持大型二进制配置数据的分片传输
- [ ] 历史记录裁剪和压缩
- [ ] 配置变更的 WebHook 通知
- [ ] 配置版本标签和回滚
- [ ] 端到端加密 (E2EE)

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
