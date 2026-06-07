class App {
  constructor() {
    this.nodeId = this._generateNodeId();
    this.configSync = null;
    this.editingKey = null;

    this._initElements();
    this._initEventListeners();
    this._updateUI();
    this._log('系统初始化完成', 'info');
    this._log(`节点ID: ${this.nodeId}`, 'info');
  }

  _generateNodeId() {
    try {
      const stored = localStorage.getItem('p2p-node-id');
      if (stored) return stored;

      const newId = `node-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
      localStorage.setItem('p2p-node-id', newId);
      return newId;
    } catch (e) {
      return `node-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
    }
  }

  _initElements() {
    this.nodeIdEl = document.getElementById('nodeId');
    this.roomIdEl = document.getElementById('roomId');
    this.connectionStatusEl = document.getElementById('connectionStatus');
    this.peerCountEl = document.getElementById('peerCount');

    this.roomInput = document.getElementById('roomInput');
    this.signalingInput = document.getElementById('signalingInput');
    this.connectBtn = document.getElementById('connectBtn');
    this.disconnectBtn = document.getElementById('disconnectBtn');

    this.addBtn = document.getElementById('addBtn');
    this.undoBtn = document.getElementById('undoBtn');
    this.redoBtn = document.getElementById('redoBtn');
    this.exportBtn = document.getElementById('exportBtn');
    this.importBtn = document.getElementById('importBtn');
    this.configEditorEl = document.getElementById('configEditor');
    this.configJsonEl = document.getElementById('configJson');

    this.peersListEl = document.getElementById('peersList');
    this.historyListEl = document.getElementById('historyList');
    this.refreshHistoryBtn = document.getElementById('refreshHistoryBtn');
    this.showDAGBtn = document.getElementById('showDAGBtn');

    this.statusDisplayEl = document.getElementById('statusDisplay');
    this.logPanelEl = document.getElementById('logPanel');

    this.testNestedBtn = document.getElementById('testNestedBtn');
    this.testSimulateBtn = document.getElementById('testSimulateBtn');
    this.testConcurrentBtn = document.getElementById('testConcurrentBtn');

    this.addConfigModal = document.getElementById('addConfigModal');
    this.configKeyInput = document.getElementById('configKeyInput');
    this.configValueInput = document.getElementById('configValueInput');
    this.configMsgInput = document.getElementById('configMsgInput');
    this.cancelAddBtn = document.getElementById('cancelAddBtn');
    this.confirmAddBtn = document.getElementById('confirmAddBtn');

    this.dagModal = document.getElementById('dagModal');
    this.dagDisplayEl = document.getElementById('dagDisplay');
    this.closeDAGBtn = document.getElementById('closeDAGBtn');

    this.importModal = document.getElementById('importModal');
    this.importTextarea = document.getElementById('importTextarea');
    this.cancelImportBtn = document.getElementById('cancelImportBtn');
    this.confirmImportBtn = document.getElementById('confirmImportBtn');
  }

  _initEventListeners() {
    this.connectBtn.addEventListener('click', () => this._connect());
    this.disconnectBtn.addEventListener('click', () => this._disconnect());

    this.addBtn.addEventListener('click', () => this._showAddModal());
    this.undoBtn.addEventListener('click', () => this._undo());
    this.redoBtn.addEventListener('click', () => this._redo());
    this.exportBtn.addEventListener('click', () => this._exportConfig());
    this.importBtn.addEventListener('click', () => this._showImportModal());

    this.refreshHistoryBtn.addEventListener('click', () => this._updateHistory());
    this.showDAGBtn.addEventListener('click', () => this._showDAG());

    this.cancelAddBtn.addEventListener('click', () => this._hideAddModal());
    this.confirmAddBtn.addEventListener('click', () => this._confirmAddConfig());

    this.closeDAGBtn.addEventListener('click', () => this._hideDAGModal());

    this.cancelImportBtn.addEventListener('click', () => this._hideImportModal());
    this.confirmImportBtn.addEventListener('click', () => this._confirmImport());

    this.addConfigModal.addEventListener('click', (e) => {
      if (e.target === this.addConfigModal) this._hideAddModal();
    });
    this.dagModal.addEventListener('click', (e) => {
      if (e.target === this.dagModal) this._hideDAGModal();
    });
    this.importModal.addEventListener('click', (e) => {
      if (e.target === this.importModal) this._hideImportModal();
    });

    this.testNestedBtn.addEventListener('click', () => this._testNestedMerge());
    this.testSimulateBtn.addEventListener('click', () => this._testSimulateIceFailure());
    this.testConcurrentBtn.addEventListener('click', () => this._testConcurrentModifications());

    setInterval(() => this._updateStatus(), 2000);
  }

  async _connect() {
    const roomId = this.roomInput.value.trim() || 'default-room';
    const signalingUrl = this.signalingInput.value.trim();

    if (!signalingUrl) {
      this._log('请输入信令服务器地址', 'error');
      return;
    }

    this._log(`正在连接到信令服务器: ${signalingUrl}`, 'info');
    this._setConnectionStatus('connecting');

    try {
      const initialConfig = {
        'app.version': '1.0.0',
        'app.title': 'P2P Config Sync',
        'ui.theme': 'light',
        'features.notifications': true,
        'performance.cache.ttl': 3600
      };

      this.configSync = new ConfigSync(this.nodeId, roomId, signalingUrl, initialConfig);

      this._setupConfigSyncListeners();

      await this.configSync.connect();

      this._setConnectionStatus('connected');
      this.roomIdEl.textContent = roomId;
      this.connectBtn.disabled = true;
      this.disconnectBtn.disabled = false;
      this.addBtn.disabled = false;

      this._log(`已加入房间: ${roomId}`, 'success');
      this._updateUI();
    } catch (err) {
      this._log(`连接失败: ${err.message}`, 'error');
      this._setConnectionStatus('disconnected');
    }
  }

  _setupConfigSyncListeners() {
    this.configSync.on('config-changed', (data) => {
      const changeDesc = data.key || data.path || '';
      this._log(`配置变更: ${changeDesc} ${data.remote ? '(远程)' : '(本地)'}`, data.remote ? 'info' : 'success');
      this._updateUI();
    });

    this.configSync.on('operation-applied', (data) => {
      const source = data.local ? '本地' : `远程(${data.from})`;
      const conflict = data.conflict ? ' [冲突已解决]' : '';
      this._log(`操作应用 [${source}]: ${data.operation.type} ${data.operation.path || data.operation.key}${conflict}`, data.conflict ? 'warning' : 'info');
    });

    this.configSync.on('peer-connected', (data) => {
      this._log(`节点已连接: ${data.peerId}`, 'success');
      this._updatePeers();
    });

    this.configSync.on('peer-reconnected', (data) => {
      this._log(`节点重连成功: ${data.peerId}`, 'success');
      this._updatePeers();
    });

    this.configSync.on('peer-reconnecting', (data) => {
      this._log(`节点正在重连: ${data.peerId} (第${data.attempt}次尝试`, 'warning');
      this._updatePeers();
    });

    this.configSync.on('peer-disconnected', (data) => {
      this._log(`节点已断开: ${data.peerId}`, 'warning');
      this._updatePeers();
    });

    this.configSync.on('ice-failed', (data) => {
      this._log(`ICE连接失败: ${data.peerId}，将尝试重连...`, 'error');
    });

    this.configSync.on('sync-started', (data) => {
      this._log(`开始同步: ${data.peerId}`, 'info');
    });

    this.configSync.on('sync-complete', (data) => {
      const type = data.incremental ? '增量' : '全量';
      this._log(`同步完成: ${data.peerId} (${type}, ${data.operationCount || 0}个操作)`, 'success');
    });

    this.configSync.on('connected', (data) => {
      this._log('P2P连接已建立', 'success');
      this._updatePeers();
    });

    this.configSync.on('disconnected', () => {
      this._log('P2P连接已断开', 'warning');
      this._updatePeers();
    });

    this.configSync.on('history-changed', () => {
      this._updateHistory();
    });
  }

  _disconnect() {
    if (this.configSync) {
      this.configSync.disconnect();
      this.configSync = null;
    }

    this._setConnectionStatus('disconnected');
    this.connectBtn.disabled = false;
    this.disconnectBtn.disabled = true;
    this.addBtn.disabled = true;
    this.peersListEl.innerHTML = '<div class="empty-state">暂无连接的节点</div>';
    this._log('已断开连接', 'warning');
    this._updateUI();
  }

  _setConnectionStatus(status) {
    this.connectionStatusEl.textContent = {
      'connected': '已连接',
      'disconnected': '未连接',
      'connecting': '连接中...'
    }[status] || status;

    this.connectionStatusEl.className = `status-badge status-${status}`;
  }

  _showAddModal(key = null) {
    this.editingKey = key;
    this.configKeyInput.value = key || '';
    this.configValueInput.value = '';
    this.configMsgInput.value = '';

    if (key && this.configSync) {
      const currentValue = this.configSync.get(key);
      this.configValueInput.value = JSON.stringify(currentValue, null, 2);
      this.configKeyInput.disabled = true;
    } else {
      this.configKeyInput.disabled = false;
    }

    this.addConfigModal.classList.add('show');
    setTimeout(() => this.configKeyInput.focus(), 100);
  }

  _hideAddModal() {
    this.addConfigModal.classList.remove('show');
    this.editingKey = null;
  }

  _confirmAddConfig() {
    const key = this.configKeyInput.value.trim();
    const valueStr = this.configValueInput.value.trim();
    const message = this.configMsgInput.value.trim();

    if (!key) {
      this._log('请输入配置键', 'error');
      return;
    }

    if (!valueStr) {
      this._log('请输入配置值', 'error');
      return;
    }

    try {
      const value = JSON.parse(valueStr);

      if (this.configSync) {
        const op = this.configSync.set(key, value, message);
        if (op) {
          this._log(`配置已${this.editingKey ? '更新' : '添加'}: ${key} = ${JSON.stringify(value)}`, 'success');
        }
      }

      this._hideAddModal();
    } catch (err) {
      this._log(`值解析失败: ${err.message}`, 'error');
    }
  }

  _editConfig(key) {
    this._showAddModal(key);
  }

  _deleteConfig(key) {
    if (confirm(`确定要删除配置项 "${key}" 吗？`)) {
      if (this.configSync) {
        const op = this.configSync.delete(key, `删除 ${key}`);
        if (op) {
          this._log(`配置已删除: ${key}`, 'warning');
        }
      }
    }
  }

  _undo() {
    if (this.configSync) {
      const result = this.configSync.undo();
      if (result) {
        this._log('已撤销操作', 'info');
        this._updateUI();
      }
    }
  }

  _redo() {
    if (this.configSync) {
      const result = this.configSync.redo();
      if (result) {
        this._log('已重做操作', 'info');
        this._updateUI();
      }
    }
  }

  _exportConfig() {
    if (this.configSync) {
      const json = this.configSync.exportConfig();
      const blob = new Blob([json], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `config-${Date.now()}.json`;
      a.click();
      URL.revokeObjectURL(url);
      this._log('配置已导出', 'success');
    }
  }

  _showImportModal() {
    this.importTextarea.value = '';
    this.importModal.classList.add('show');
    setTimeout(() => this.importTextarea.focus(), 100);
  }

  _hideImportModal() {
    this.importModal.classList.remove('show');
  }

  _confirmImport() {
    const jsonStr = this.importTextarea.value.trim();
    if (!jsonStr) {
      this._log('请输入JSON配置', 'error');
      return;
    }

    if (this.configSync) {
      const result = this.configSync.importConfig(jsonStr);
      if (result.success) {
        this._log(`成功导入 ${result.operations.length} 个配置项`, 'success');
        this._hideImportModal();
      } else {
        this._log(`导入失败: ${result.error}`, 'error');
      }
    }
  }

  _showDAG() {
    if (this.configSync) {
      const structure = this.configSync.getDAGStructure();
      this._renderDAG(structure);
      this.dagModal.classList.add('show');
    }
  }

  _hideDAGModal() {
    this.dagModal.classList.remove('show');
  }

  _renderDAG(structure) {
    const { nodes, edges } = structure;

    const nodeMap = new Map();
    const levels = [];

    const visited = new Set();

    function getLevel(nodeId, level = 0) {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);

      if (!levels[level]) levels[level] = [];
      levels[level].push(nodeId);

      const nodeEdges = edges.filter(e => e.from === nodeId);
      nodeEdges.forEach(e => getLevel(e.to, level + 1));
    }

    const rootNode = nodes.find(n => n.type === 'root');
    if (rootNode) {
      getLevel(rootNode.id);
    }

    nodes.forEach(n => nodeMap.set(n.id, n));

    let html = '';
    levels.forEach((level, levelIndex) => {
      html += `<div class="dag-level">`;
      level.forEach((nodeId, nodeIndex) => {
        const node = nodeMap.get(nodeId);
        if (node) {
          const isHead = node.isHead ? 'head' : '';
          html += `
            <div class="dag-node ${node.type} ${isHead}" 
                 title="${node.message}\n${new Date(node.timestamp).toLocaleString()}">
              ${isHead ? '⭐ ' : ''}${node.type.slice(0, 3).toUpperCase()}-${node.id.slice(-6)}
            </div>
          `;
          if (nodeIndex < level.length - 1) {
            html += `<span class="dag-edge">|</span>`;
          }
        }
      });
      html += `</div>`;

      if (levelIndex < levels.length - 1) {
        html += `<div class="dag-level"><span style="color: #667eea;">↓</span></div>`;
      }
    });

    this.dagDisplayEl.innerHTML = html || '<div class="empty-state">暂无DAG数据</div>';
  }

  _updateUI() {
    this.nodeIdEl.textContent = this.nodeId;

    if (this.configSync) {
      this._updateConfigEditor();
      this._updateConfigJson();
      this._updateHistory();
      this._updatePeers();
      this._updateUndoRedoButtons();
    } else {
      this.configEditorEl.innerHTML = '<div class="empty-state">请先连接到服务器</div>';
      this.configJsonEl.textContent = '{}';
      this.historyListEl.innerHTML = '<div class="empty-state">暂无变更历史</div>';
    }
  }

  _updateConfigEditor() {
    if (!this.configSync) return;

    const config = this.configSync.getConfig();
    const keys = Object.keys(config);

    if (keys.length === 0) {
      this.configEditorEl.innerHTML = '<div class="empty-state">暂无配置项，点击"添加配置项"开始</div>';
      return;
    }

    let html = '';
    keys.forEach(key => {
      const value = config[key];
      const valueStr = JSON.stringify(value);
      html += `
        <div class="config-item">
          <div class="config-key">${this._escapeHtml(key)}</div>
          <div class="config-value" title="${this._escapeHtml(valueStr)}">${this._escapeHtml(valueStr)}</div>
          <div class="config-actions-item">
            <button class="btn btn-sm btn-primary" onclick="app._editConfig('${this._escapeHtml(key)}')">编辑</button>
            <button class="btn btn-sm btn-danger" onclick="app._deleteConfig('${this._escapeHtml(key)}')">删除</button>
          </div>
        </div>
      `;
    });

    this.configEditorEl.innerHTML = html;
  }

  _updateConfigJson() {
    if (this.configSync) {
      this.configJsonEl.textContent = JSON.stringify(this.configSync.getConfig(), null, 2);
    }
  }

  _updateHistory() {
    if (!this.configSync) return;

    const history = this.configSync.getHistory();

    if (history.length === 0) {
      this.historyListEl.innerHTML = '<div class="empty-state">暂无变更历史</div>';
      return;
    }

    let html = '';
    history.forEach(item => {
      const isHead = item.isHead ? 'head' : '';
      html += `
        <div class="history-item ${item.type} ${isHead}">
          <div class="history-message">
            ${isHead ? '⭐ ' : ''}${this._escapeHtml(item.message)}
          </div>
          <div class="history-meta">
            ${new Date(item.timestamp).toLocaleString()} | 
            ${item.type.toUpperCase()}
          </div>
          ${item.parents && item.parents.length > 0 ? `
            <div class="history-parents">
              父节点: ${item.parents.map(p => p.slice(-8)).join(', ')}
            </div>
          ` : ''}
        </div>
      `;
    });

    this.historyListEl.innerHTML = html;
  }

  _updatePeers() {
    if (!this.configSync) return;

    const status = this.configSync.getStatus();
    const peers = status.peers || {};
    const peerCount = Object.keys(peers).length;
    this.peerCountEl.textContent = peerCount;

    if (peerCount === 0) {
      this.peersListEl.innerHTML = '<div class="empty-state">暂无连接的节点</div>';
      return;
    }

    let html = '';
    for (const [peerId, peerStatus] of Object.entries(peers)) {
      const vc = status.peerVectorClocks ? status.peerVectorClocks[peerId] : null;
      const vcDisplay = vc ? JSON.stringify(vc) : '-';
      const statusText = peerStatus.connected ? '在线' : (peerStatus.reconnecting ? `重连中(${peerStatus.reconnectAttempts})` : '已断开');
      const statusClass = peerStatus.connected ? 'peer-online' : (peerStatus.reconnecting ? 'peer-reconnecting' : 'peer-offline');
      const queueSize = peerStatus.queueSize || 0;
      const dataChannelState = peerStatus.dataChannelState || '-';

      html += `
        <div class="peer-item">
          <div class="peer-header">
            <span class="peer-id">${this._escapeHtml(peerId.slice(-12))}</span>
            <span class="peer-status ${statusClass}">${statusText}</span>
          </div>
          <div class="peer-details">
            <div class="peer-detail-row">
              <span class="peer-detail-label">通道:</span>
              <span class="peer-detail-value">${dataChannelState}</span>
            </div>
            <div class="peer-detail-row">
              <span class="peer-detail-label">队列:</span>
              <span class="peer-detail-value">${queueSize} 条消息</span>
            </div>
            <div class="peer-detail-row peer-vc">
              <span class="peer-detail-label">时钟:</span>
              <span class="peer-detail-value">${this._escapeHtml(vcDisplay)}</span>
            </div>
          </div>
        </div>
      `;
    }

    this.peersListEl.innerHTML = html;
  }

  _updateUndoRedoButtons() {
    if (this.configSync) {
      this.undoBtn.disabled = !this.configSync.canUndo();
      this.redoBtn.disabled = !this.configSync.canRedo();
    }
  }

  _updateStatus() {
    if (this.configSync) {
      const status = this.configSync.getStatus();
      this.statusDisplayEl.textContent = JSON.stringify(status, null, 2);
    }
  }

  _log(message, type = 'info') {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('zh-CN', { hour12: false });

    const entry = document.createElement('div');
    entry.className = 'log-entry';
    entry.innerHTML = `
      <span class="log-time">${timeStr}</span>
      <span class="log-type log-${type}">${type.toUpperCase()}</span>
      <span class="log-message">${this._escapeHtml(message)}</span>
    `;

    this.logPanelEl.appendChild(entry);
    this.logPanelEl.scrollTop = this.logPanelEl.scrollHeight;

    while (this.logPanelEl.children.length > 100) {
      this.logPanelEl.removeChild(this.logPanelEl.firstChild);
    }
  }

  _testNestedMerge() {
    if (!this.configSync) {
      this._log('请先连接到服务器', 'error');
      return;
    }

    this._log('=== 开始嵌套属性合并测试 ===', 'info');

    this.configSync.set('test.a.b', 1, '设置 test.a.b = 1');
    this._log('已设置 test.a.b = 1', 'success');

    setTimeout(() => {
      this.configSync.set('test.a.c', 2, '设置 test.a.c = 2');
      this._log('已设置 test.a.c = 2', 'success');

      setTimeout(() => {
        const config = this.configSync.getConfig();
        this._log(`最终 test 对象: ${JSON.stringify(config.test)}`, 'info');

        if (config.test && config.test.a && config.test.a.b === 1 && config.test.a.c === 2) {
          this._log('✅ 嵌套属性合并成功！两个属性都保留了', 'success');
        } else {
          this._log('❌ 嵌套属性合并失败！部分属性丢失', 'error');
        }
      }, 500);
    }, 500);
  }

  _testSimulateIceFailure() {
    if (!this.configSync) {
      this._log('请先连接到服务器', 'error');
      return;
    }

    const peers = this.configSync.p2p.getConnectedPeers();
    if (peers.length === 0) {
      this._log('当前没有连接的节点，无法模拟ICE失败', 'error');
      return;
    }

    const peerId = peers[0];
    this._log(`=== 模拟与 ${peerId.slice(-12)} 的ICE连接失败 ===`, 'warning');
    this._log('这将触发自动重连机制和断点续传...', 'info');

    this.configSync.set('test.ice.failure', Date.now(), 'ICE失败前的配置');
    this._log('已设置测试配置，等待重连后验证同步', 'info');

    try {
      this.configSync.p2p._simulateIceFailure(peerId);
      this._log('已触发ICE失败模拟，观察自动重连...', 'warning');
    } catch (e) {
      this._log(`模拟失败: ${e.message}`, 'error');
    }
  }

  _testConcurrentModifications() {
    if (!this.configSync) {
      this._log('请先连接到服务器', 'error');
      return;
    }

    const peers = this.configSync.p2p.getConnectedPeers();
    if (peers.length === 0) {
      this._log('当前没有连接的节点，请先打开另一个浏览器窗口并连接', 'warning');
    }

    this._log('=== 并发修改测试 ===', 'info');
    this._log('请在另一个浏览器窗口同时执行以下操作：', 'info');
    this._log('  节点A: 设置 concurrent.a = 1', 'info');
    this._log('  节点B: 设置 concurrent.b = 2', 'info');
    this._log('预期结果: concurrent 对象同时包含 a 和 b', 'info');

    this.configSync.set('concurrent.a', 1, '节点A设置 concurrent.a');
    this._log('本节点已设置 concurrent.a = 1', 'success');

    setTimeout(() => {
      const config = this.configSync.getConfig();
      this._log(`当前 concurrent 对象: ${JSON.stringify(config.concurrent)}`, 'info');

      if (config.concurrent && config.concurrent.a === 1 && config.concurrent.b === 2) {
        this._log('✅ 并发修改合并成功！两个属性都保留了', 'success');
      } else if (config.concurrent && config.concurrent.a === 1) {
        this._log('⏳ 等待节点B的修改同步...', 'warning');
        setTimeout(() => {
          const config2 = this.configSync.getConfig();
          this._log(`最终 concurrent 对象: ${JSON.stringify(config2.concurrent)}`, 'info');
          if (config2.concurrent && config2.concurrent.a === 1 && config2.concurrent.b === 2) {
            this._log('✅ 并发修改合并成功！', 'success');
          }
        }, 2000);
      }
    }, 3000);
  }

  _escapeHtml(str) {
    if (typeof str !== 'string') str = String(str);
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
  }
}

const app = new App();
