class ConfigSync {
  constructor(nodeId, roomId, signalingUrl, initialConfig = {}) {
    this.nodeId = nodeId;
    this.roomId = roomId;
    this.signalingUrl = signalingUrl;

    this.crdt = new CRDTMap(nodeId);
    this.history = new DAGHistory();
    this.p2p = new P2PManager(nodeId, roomId, signalingUrl);

    this.pendingOperations = new Map();
    this.syncedOperations = new Set();
    this.isSyncing = false;

    this.listeners = {
      'config-changed': [],
      'operation-applied': [],
      'peer-connected': [],
      'peer-disconnected': [],
      'connected': [],
      'disconnected': [],
      'history-changed': []
    };

    this._initHistory(initialConfig);
    this._setupP2PListeners();
  }

  _initHistory(initialConfig) {
    for (const [key, value] of Object.entries(initialConfig)) {
      this.crdt.set(key, value);
    }
    this.history.createRoot(this.crdt.getAll());
  }

  _setupP2PListeners() {
    this.p2p.on('peer-connected', (data) => {
      this._emit('peer-connected', data);
      this._sendStateToPeer(data.peerId);
    });

    this.p2p.on('peer-disconnected', (data) => {
      this._emit('peer-disconnected', data);
    });

    this.p2p.on('connected', (data) => {
      this._emit('connected', data);
    });

    this.p2p.on('disconnected', () => {
      this._emit('disconnected');
    });

    this.p2p.on('message', (data) => {
      this._handleMessage(data.peerId, data.data);
    });
  }

  on(event, callback) {
    if (this.listeners[event]) {
      this.listeners[event].push(callback);
    }
  }

  off(event, callback) {
    if (this.listeners[event]) {
      this.listeners[event] = this.listeners[event].filter(cb => cb !== callback);
    }
  }

  _emit(event, data) {
    if (this.listeners[event]) {
      this.listeners[event].forEach(cb => {
        try {
          cb(data);
        } catch (err) {
          console.error(`Error in ${event} listener:`, err);
        }
      });
    }
  }

  async connect() {
    await this.p2p.connect();
  }

  disconnect() {
    this.p2p.disconnect();
  }

  getConfig() {
    return this.crdt.getAll();
  }

  get(key) {
    return this.crdt.get(key);
  }

  set(key, value, message = '') {
    const operation = this.crdt.set(key, value);
    if (!operation) return null;

    this._recordOperation(operation, message);
    this._broadcastOperation(operation);
    this._emit('config-changed', { key, value, operation });

    return operation;
  }

  delete(key, message = '') {
    const operation = this.crdt.delete(key);
    if (!operation) return null;

    this._recordOperation(operation, message);
    this._broadcastOperation(operation);
    this._emit('config-changed', { key, value: undefined, operation });

    return operation;
  }

  _recordOperation(operation, message) {
    const newState = this.crdt.getAll();
    const historyNode = this.history.addOperation(operation, newState, message);
    this.syncedOperations.add(operation.id);
    this._emit('operation-applied', { operation, historyNode, local: true });
    this._emit('history-changed', this.history.getHistory());
  }

  _broadcastOperation(operation) {
    this.p2p.broadcast({
      type: 'operation',
      operation,
      historyNodeId: this.history.currentHead
    });
  }

  _sendStateToPeer(peerId) {
    const state = {
      type: 'state-sync',
      crdtState: {
        state: this.crdt.state,
        metadata: this.crdt.metadata,
        clock: this.crdt.clock
      },
      history: this.history.export(),
      fromNodeId: this.nodeId
    };

    this.p2p.sendToPeer(peerId, state);
  }

  _handleMessage(peerId, data) {
    switch (data.type) {
      case 'operation':
        this._handleOperation(peerId, data);
        break;
      case 'state-sync':
        this._handleStateSync(peerId, data);
        break;
      case 'operation-request':
        this._handleOperationRequest(peerId, data);
        break;
      case 'history-request':
        this._handleHistoryRequest(peerId, data);
        break;
      default:
        console.log('Unknown message type:', data.type);
    }
  }

  _handleOperation(peerId, data) {
    const { operation, historyNodeId } = data;

    if (this.syncedOperations.has(operation.id)) {
      return;
    }

    const result = this.crdt.applyRemote(operation);

    if (result.applied) {
      this.syncedOperations.add(operation.id);

      const newState = this.crdt.getAll();
      const historyNode = this.history.addOperation(operation, newState, operation.message);

      this._emit('config-changed', {
        key: operation.key,
        value: operation.type === 'delete' ? undefined : operation.value,
        operation,
        remote: true,
        from: peerId
      });

      this._emit('operation-applied', {
        operation,
        historyNode,
        local: false,
        from: peerId
      });

      this._emit('history-changed', this.history.getHistory());

      this.p2p.broadcast({
        type: 'operation',
        operation,
        historyNodeId: historyNode.id
      });
    }
  }

  _handleStateSync(peerId, data) {
    console.log(`[ConfigSync] Received state sync from ${peerId}`);

    if (data.fromNodeId === this.nodeId) return;

    const remoteCrdt = new CRDTMap(data.fromNodeId);
    remoteCrdt.state = { ...data.crdtState.state };
    remoteCrdt.metadata = { ...data.crdtState.metadata };
    remoteCrdt.clock = data.crdtState.clock;

    const mergedOps = this.crdt.merge(remoteCrdt);

    if (mergedOps.length > 0) {
      const newState = this.crdt.getAll();
      this.history.mergeOperations(mergedOps, newState, `Merged state from ${peerId}`);

      mergedOps.forEach(op => {
        this.syncedOperations.add(op.id);
      });

      this._emit('config-changed', {
        merged: true,
        operations: mergedOps,
        from: peerId
      });

      this._emit('history-changed', this.history.getHistory());
    }

    this._emit('connected', { peerId, synced: true });
  }

  _handleOperationRequest(peerId, data) {
    const opId = data.operationId;
    const op = this.pendingOperations.get(opId);
    if (op) {
      this.p2p.sendToPeer(peerId, {
        type: 'operation',
        operation: op,
        historyNodeId: data.historyNodeId
      });
    }
  }

  _handleHistoryRequest(peerId, data) {
    this.p2p.sendToPeer(peerId, {
      type: 'state-sync',
      crdtState: {
        state: this.crdt.state,
        metadata: this.crdt.metadata,
        clock: this.crdt.clock
      },
      history: this.history.export(),
      fromNodeId: this.nodeId
    });
  }

  undo() {
    const result = this.history.undo();
    if (!result) return null;

    const { operation, previousState } = result;

    for (const key of Object.keys(this.crdt.state)) {
      delete this.crdt.state[key];
    }
    for (const key of Object.keys(this.crdt.metadata)) {
      delete this.crdt.metadata[key];
    }

    for (const [key, value] of Object.entries(previousState)) {
      this.crdt.set(key, value);
    }

    this._emit('config-changed', {
      undo: true,
      operation,
      previousState
    });

    this._emit('history-changed', this.history.getHistory());

    this.p2p.broadcast({
      type: 'operation',
      operation: {
        ...operation,
        id: `undo-${operation.id}-${Date.now()}`,
        type: operation.type === 'set' ? 'set' : 'set',
        value: operation.prevValue,
        isUndo: true,
        originalOperationId: operation.id
      },
      historyNodeId: this.history.currentHead
    });

    return result;
  }

  redo() {
    const result = this.history.redo();
    if (!result) return null;

    const { operation, newState } = result;

    for (const key of Object.keys(this.crdt.state)) {
      delete this.crdt.state[key];
    }
    for (const key of Object.keys(this.crdt.metadata)) {
      delete this.crdt.metadata[key];
    }

    for (const [key, value] of Object.entries(newState)) {
      this.crdt.set(key, value);
    }

    this._emit('config-changed', {
      redo: true,
      operation,
      newState
    });

    this._emit('history-changed', this.history.getHistory());

    this.p2p.broadcast({
      type: 'operation',
      operation: {
        ...operation,
        id: `redo-${operation.id}-${Date.now()}`,
        isRedo: true,
        originalOperationId: operation.id
      },
      historyNodeId: this.history.currentHead
    });

    return result;
  }

  canUndo() {
    return this.history.canUndo();
  }

  canRedo() {
    return this.history.canRedo();
  }

  getHistory() {
    return this.history.getHistory();
  }

  getDAGStructure() {
    return this.history.getDAGStructure();
  }

  getHistoryNode(nodeId) {
    return this.history.getNode(nodeId);
  }

  getStatus() {
    return {
      nodeId: this.nodeId,
      roomId: this.roomId,
      config: this.getConfig(),
      configKeys: Object.keys(this.getConfig()),
      historyLength: this.history.getHistory().length,
      undoStackSize: this.history.undoStack.length,
      redoStackSize: this.history.redoStack.length,
      ...this.p2p.getStatus()
    };
  }

  exportConfig() {
    return JSON.stringify(this.getConfig(), null, 2);
  }

  importConfig(jsonString) {
    try {
      const config = JSON.parse(jsonString);
      const operations = [];

      for (const [key, value] of Object.entries(config)) {
        const op = this.set(key, value, `Import ${key}`);
        if (op) operations.push(op);
      }

      return { success: true, operations };
    } catch (err) {
      return { success: false, error: err.message };
    }
  }

  batchSet(entries, message = 'Batch update') {
    const operations = [];

    for (const [key, value] of entries) {
      const op = this.crdt.set(key, value);
      if (op) {
        operations.push(op);
      }
    }

    if (operations.length > 0) {
      const newState = this.crdt.getAll();
      const parentIds = operations.map(op => this.history.currentHead);
      const uniqueParents = [...new Set(parentIds)];

      const node = {
        id: this.history._generateId('batch'),
        type: 'batch',
        operations,
        state: JSON.parse(JSON.stringify(newState)),
        parents: uniqueParents,
        timestamp: Date.now(),
        message
      };

      this.history.nodes.set(node.id, node);
      this.history.edges.set(node.id, []);

      uniqueParents.forEach(parentId => {
        const parentEdges = this.history.edges.get(parentId) || [];
        parentEdges.push(node.id);
        this.history.edges.set(parentId, parentEdges);
      });

      this.history.currentHead = node.id;
      this.history.undoStack.push(node.id);
      this.history.redoStack = [];

      operations.forEach(op => {
        this.syncedOperations.add(op.id);
        this.p2p.broadcast({
          type: 'operation',
          operation: op,
          historyNodeId: node.id
        });
      });

      this._emit('config-changed', { batch: true, operations });
      this._emit('history-changed', this.history.getHistory());
    }

    return operations;
  }
}

window.ConfigSync = ConfigSync;
