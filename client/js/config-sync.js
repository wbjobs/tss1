class ConfigSync {
  constructor(nodeId, roomId, signalingUrl, initialConfig = {}) {
    this.nodeId = nodeId;
    this.roomId = roomId;
    this.signalingUrl = signalingUrl;

    this.crdt = new CRDTMap(nodeId);
    this.history = new DAGHistory();
    this.p2p = new P2PManager(nodeId, roomId, signalingUrl);
    this.crypto = new CryptoManager();

    this.pendingOperations = new Map();
    this.syncedOperations = new Set();
    this.peerVectorClocks = new Map();
    this.isSyncing = false;
    this.syncInProgress = new Set();
    this.encryptedPathMetadata = new Map();

    this.listeners = {
      'config-changed': [],
      'operation-applied': [],
      'peer-connected': [],
      'peer-disconnected': [],
      'peer-reconnected': [],
      'peer-reconnecting': [],
      'connected': [],
      'disconnected': [],
      'history-changed': [],
      'sync-started': [],
      'sync-complete': [],
      'encryption-required': [],
      'decryption-error': [],
      'encryption-enabled': [],
      'encryption-disabled': [],
      'rollback-complete': []
    };

    this._initHistory(initialConfig);
    this._setupP2PListeners();
  }

  _initHistory(initialConfig) {
    for (const [key, value] of Object.entries(initialConfig)) {
      this.crdt.set(key, value, `Initial ${key}`);
    }
    this.history.createRoot(this.crdt.getAll());
  }

  _setupP2PListeners() {
    this.p2p.on('peer-connected', (data) => {
      this._emit('peer-connected', data);
      this._startSyncWithPeer(data.peerId);
    });

    this.p2p.on('peer-reconnected', (data) => {
      this._emit('peer-reconnected', data);
      this._startSyncWithPeer(data.peerId);
    });

    this.p2p.on('peer-reconnecting', (data) => {
      this._emit('peer-reconnecting', data);
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

    this.p2p.on('ice-failed', (data) => {
      console.warn(`[ConfigSync] ICE failed with peer ${data.peerId}, will retry`);
    });

    this.p2p.on('vector-clock-update', (data) => {
      if (data.action === 'request' && data.callback) {
        data.callback(this.crdt.getVectorClock());
      }
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
    this.peerVectorClocks.clear();
    this.syncInProgress.clear();
  }

  getConfig() {
    return this.crdt.getAll();
  }

  get(path) {
    return this.crdt.get(path);
  }

  has(path) {
    return this.crdt.has(path);
  }

  async set(path, value, message = '') {
    const encryptedValue = await this._maybeEncryptValue(path, value);
    const operation = this.crdt.set(path, encryptedValue, message);

    if (!operation) return null;

    if (this.crypto.isPathEncrypted(path)) {
      operation.isEncrypted = true;
      operation.encryptedPath = this.crypto.getEncryptedParentPath(path);
      operation.originalValue = value;
    }

    this._recordOperation(operation, message);
    this._broadcastOperation(operation);

    const displayValue = operation.isEncrypted ? value : encryptedValue;
    this._emit('config-changed', { path, key: path, value: displayValue, operation });

    return operation;
  }

  async delete(path, message = '') {
    const operation = this.crdt.delete(path, message);
    if (!operation) return null;

    this._recordOperation(operation, message);
    this._broadcastOperation(operation);
    this._emit('config-changed', { path, key: path, value: undefined, operation });

    return operation;
  }

  async setPasswordForPath(path, password) {
    try {
      await this.crypto.setPasswordForPath(path, password);

      this.encryptedPathMetadata.set(path, {
        enabled: true,
        timestamp: Date.now(),
        setBy: this.nodeId
      });

      const currentValue = this.crdt.get(path);
      if (currentValue !== undefined) {
        const encrypted = await this.crypto.encrypt(currentValue, path);
        this.crdt._setRaw(path, encrypted);

        const metadata = this.crdt.metadata.get(path);
        if (metadata) {
          metadata.isEncrypted = true;
          metadata.encryptedPath = path;
        }
      }

      this._emit('encryption-enabled', { path });
      this._emit('config-changed', { path, encryptionChanged: true, enabled: true });

      return true;
    } catch (e) {
      console.error(`Failed to set password for ${path}:`, e);
      return false;
    }
  }

  removePasswordForPath(path) {
    this.crypto.removePasswordForPath(path);
    this.encryptedPathMetadata.delete(path);

    this._emit('encryption-disabled', { path });
    this._emit('config-changed', { path, encryptionChanged: true, enabled: false });
  }

  async unlockPath(path, password) {
    try {
      await this.crypto.setPasswordForPath(path, password);

      this.encryptedPathMetadata.set(path, {
        enabled: true,
        timestamp: Date.now(),
        setBy: this.nodeId,
        unlocked: true
      });

      await this._decryptAllEncryptedFields();

      this._emit('encryption-enabled', { path, unlocked: true });
      this._emit('history-changed', this.history.getHistory());

      return true;
    } catch (e) {
      this._emit('decryption-error', { path, error: e.message });
      return false;
    }
  }

  isPathEncrypted(path) {
    return this.crypto.isPathEncrypted(path);
  }

  hasKeyForPath(path) {
    return this.crypto.hasKeyForPath(path);
  }

  getEncryptedPaths() {
    return this.crypto.exportEncryptedPaths();
  }

  async _maybeEncryptValue(path, value) {
    if (this.crypto.isPathEncrypted(path) && this.crypto.hasKeyForPath(path)) {
      const encrypted = await this.crypto.encrypt(value, path);
      return encrypted;
    }
    return value;
  }

  async _decryptAllEncryptedFields() {
    const encryptedPaths = this.crypto.exportEncryptedPaths();
    const state = this.crdt.getAll();

    for (const path of encryptedPaths) {
      if (this.crypto.hasKeyForPath(path)) {
        try {
          const value = this.crdt.get(path);
          if (value && value.encrypted) {
            const decrypted = await this.crypto.decrypt(value, path);
            this.crdt._setRaw(path, decrypted);
          }
        } catch (e) {
          console.warn(`Failed to decrypt ${path}:`, e);
        }
      }
    }
  }

  async _decryptOperationIfNeeded(operation) {
    if (!operation || !operation.isEncrypted) {
      return operation;
    }

    try {
      const decryptedValue = await this.crypto.decrypt(operation.value, operation.path);
      return {
        ...operation,
        value: decryptedValue,
        decryptedSuccessfully: true,
        originalEncryptedValue: operation.value
      };
    } catch (e) {
      return {
        ...operation,
        decryptionError: e.message,
        value: operation.value
      };
    }
  }

  _recordOperation(operation, message) {
    const newState = this.crdt.getAll();
    const historyNode = this.history.addOperation(operation, newState, message || operation.message);
    this.syncedOperations.add(operation.id);
    this._emit('operation-applied', { operation, historyNode, local: true });
    this._emit('history-changed', this.history.getHistory());
  }

  async _broadcastOperation(operation) {
    const opToSend = operation.isEncrypted && operation.originalValue
      ? { ...operation, value: operation.value, originalValue: undefined }
      : operation;

    const message = {
      type: 'operation',
      operation: opToSend,
      historyNodeId: this.history.currentHead,
      vectorClock: this.crdt.getVectorClock()
    };

    const results = this.p2p.broadcast(message);

    results.forEach(result => {
      if (result.buffered) {
        console.log(`[ConfigSync] Operation buffered for peer ${result.peerId}`);
      }
    });
  }

  _startSyncWithPeer(peerId) {
    if (this.syncInProgress.has(peerId)) {
      console.log(`[ConfigSync] Sync already in progress with ${peerId}`);
      return;
    }

    this.syncInProgress.add(peerId);
    this._emit('sync-started', { peerId });

    const localVC = this.crdt.getVectorClock();
    const remoteVC = this.peerVectorClocks.get(peerId) || {};

    const localVCObj = VectorClock.fromJSON(localVC);
    const remoteVCObj = VectorClock.fromJSON(remoteVC);

    if (localVCObj.isGreaterThan(remoteVCObj)) {
      console.log(`[ConfigSync] Sending incremental updates to ${peerId}`);
      const operations = this.crdt.getOperationsSince(remoteVC);
      this._sendIncrementalSync(peerId, operations, localVC);
    } else if (remoteVCObj.isGreaterThan(localVCObj) || remoteVCObj.isConcurrentWith(localVCObj)) {
      console.log(`[ConfigSync] Requesting incremental updates from ${peerId}`);
      this._requestIncrementalSync(peerId, localVC);
    } else {
      console.log(`[ConfigSync] Already in sync with ${peerId}`);
      this._sendFullStateIfNeeded(peerId);
      this.syncInProgress.delete(peerId);
      this._emit('sync-complete', { peerId, incremental: false });
    }
  }

  async _sendIncrementalSync(peerId, operations, baseVectorClock) {
    if (operations.length === 0) {
      console.log(`[ConfigSync] No incremental updates needed for ${peerId}`);
      this._sendFullStateIfNeeded(peerId);
      return;
    }

    console.log(`[ConfigSync] Sending ${operations.length} incremental operations to ${peerId}`);

    this.p2p.sendToPeer(peerId, {
      type: 'sync-response',
      incremental: true,
      operations,
      baseVectorClock,
      currentVectorClock: this.crdt.getVectorClock(),
      fromNodeId: this.nodeId
    });

    if (operations.length < 100) {
      this.syncInProgress.delete(peerId);
      this._emit('sync-complete', { peerId, incremental: true, operationCount: operations.length });
    }
  }

  _requestIncrementalSync(peerId, sinceVectorClock) {
    console.log(`[ConfigSync] Requesting incremental sync from ${peerId}`);

    this.p2p.sendToPeer(peerId, {
      type: 'sync-request',
      vectorClock: sinceVectorClock,
      fromNodeId: this.nodeId,
      requestFullStateIfNeeded: true
    });
  }

  _sendFullStateIfNeeded(peerId) {
    const remoteVC = this.peerVectorClocks.get(peerId) || {};
    const localVC = this.crdt.getVectorClock();
    const localVCObj = VectorClock.fromJSON(localVC);
    const remoteVCObj = VectorClock.fromJSON(remoteVC);

    if (localVCObj.isGreaterThan(remoteVCObj) || Object.keys(remoteVC).length === 0) {
      this._sendFullState(peerId);
    }
  }

  _sendFullState(peerId) {
    console.log(`[ConfigSync] Sending full state to ${peerId}`);

    const state = {
      type: 'state-sync',
      crdtState: this.crdt.exportState(),
      history: this.history.export(),
      fromNodeId: this.nodeId,
      vectorClock: this.crdt.getVectorClock(),
      encryptedPaths: this.crypto.exportEncryptedPaths()
    };

    this.p2p.sendToPeer(peerId, state);
  }

  async _handleMessage(peerId, data) {
    switch (data.type) {
      case 'operation':
        await this._handleOperation(peerId, data);
        break;
      case 'sync-request':
        this._handleSyncRequest(peerId, data);
        break;
      case 'sync-response':
        await this._handleSyncResponse(peerId, data);
        break;
      case 'state-sync':
        await this._handleStateSync(peerId, data);
        break;
      case 'operation-request':
        this._handleOperationRequest(peerId, data);
        break;
      case 'history-request':
        this._handleHistoryRequest(peerId, data);
        break;
      case 'rollback':
        await this._handleRollback(peerId, data);
        break;
      default:
        console.log('Unknown message type:', data.type);
    }
  }

  async _handleOperation(peerId, data) {
    const { operation, historyNodeId, vectorClock } = data;

    if (!operation || !operation.id) {
      console.error('[ConfigSync] Invalid operation received');
      return;
    }

    if (this.syncedOperations.has(operation.id)) {
      return;
    }

    const decryptedOp = await this._decryptOperationIfNeeded(operation);

    const result = this.crdt.applyRemote(decryptedOp);

    if (result.applied) {
      this.syncedOperations.add(operation.id);

      if (vectorClock) {
        this.peerVectorClocks.set(peerId, vectorClock);
      }

      const newState = this.crdt.getAll();
      const historyNode = this.history.addOperation(decryptedOp, newState, decryptedOp.message);

      const displayValue = decryptedOp.decryptedSuccessfully
        ? decryptedOp.value
        : (decryptedOp.type === 'delete' ? undefined : decryptedOp.value);

      this._emit('config-changed', {
        path: decryptedOp.path || decryptedOp.key,
        key: decryptedOp.key,
        value: displayValue,
        operation: decryptedOp,
        remote: true,
        from: peerId,
        conflict: result.conflict,
        decryptionError: decryptedOp.decryptionError
      });

      this._emit('operation-applied', {
        operation: decryptedOp,
        historyNode,
        local: false,
        from: peerId,
        conflict: result.conflict,
        decryptionError: decryptedOp.decryptionError
      });

      if (decryptedOp.decryptionError) {
        this._emit('decryption-error', {
          path: decryptedOp.path,
          error: decryptedOp.decryptionError,
          from: peerId
        });
      }

      this._emit('history-changed', this.history.getHistory());

      this._gossipOperation(decryptedOp, historyNode.id, peerId);
    } else if (result.conflict) {
      console.log(`[ConfigSync] Operation conflict, already have newer version of ${operation.key}`);
    }
  }

  _gossipOperation(operation, historyNodeId, excludePeerId) {
    const opToSend = operation.decryptedSuccessfully
      ? { ...operation, value: operation.originalEncryptedValue }
      : operation;

    const message = {
      type: 'operation',
      operation: opToSend,
      historyNodeId,
      vectorClock: this.crdt.getVectorClock()
    };

    const peers = this.p2p.getConnectedPeers();
    peers.forEach(peerId => {
      if (peerId !== excludePeerId) {
        this.p2p.sendToPeer(peerId, message);
      }
    });
  }

  _handleSyncRequest(peerId, data) {
    console.log(`[ConfigSync] Received sync request from ${peerId}`);

    const { vectorClock: requestVC, requestFullStateIfNeeded } = data;

    if (requestVC) {
      const operations = this.crdt.getOperationsSince(requestVC);

      if (operations.length > 0 && operations.length < 500) {
        this._sendIncrementalSync(peerId, operations, this.crdt.getVectorClock());
        return;
      }
    }

    if (requestFullStateIfNeeded !== false) {
      this._sendFullState(peerId);
    }
  }

  async _handleSyncResponse(peerId, data) {
    console.log(`[ConfigSync] Received sync response from ${peerId}`);

    const { operations, baseVectorClock, currentVectorClock, incremental } = data;

    if (incremental && operations && operations.length > 0) {
      console.log(`[ConfigSync] Applying ${operations.length} incremental operations from ${peerId}`);

      const decryptedOps = [];
      for (const op of operations) {
        const decrypted = await this._decryptOperationIfNeeded(op);
        decryptedOps.push(decrypted);
      }

      const appliedOps = this.crdt.applyOperations(decryptedOps);

      if (appliedOps.length > 0) {
        const newState = this.crdt.getAll();

        if (appliedOps.length === 1) {
          this.history.addOperation(appliedOps[0], newState, appliedOps[0].message || 'Remote update');
        } else {
          this.history.mergeOperations(appliedOps, newState, `Applied ${appliedOps.length} remote operations`);
        }

        appliedOps.forEach(op => {
          this.syncedOperations.add(op.id);
        });

        this._emit('config-changed', {
          merged: true,
          incremental: true,
          operations: appliedOps,
          from: peerId
        });

        this._emit('history-changed', this.history.getHistory());
      }

      if (currentVectorClock) {
        this.peerVectorClocks.set(peerId, currentVectorClock);
      }

      this.syncInProgress.delete(peerId);
      this._emit('sync-complete', { peerId, incremental: true, operationCount: appliedOps.length });
    } else {
      this.syncInProgress.delete(peerId);
      this._emit('sync-complete', { peerId, incremental: true, operationCount: 0 });
    }
  }

  async _handleStateSync(peerId, data) {
    console.log(`[ConfigSync] Received state sync from ${peerId}`);

    if (data.fromNodeId === this.nodeId) return;

    const { crdtState, history, vectorClock, encryptedPaths } = data;

    if (encryptedPaths && encryptedPaths.length > 0) {
      this.crypto.importEncryptedPaths(encryptedPaths);
    }

    const tempCrdt = new CRDTMap(data.fromNodeId);
    tempCrdt.importState(crdtState);

    const mergedOps = this.crdt.merge(tempCrdt);

    const decryptedMergedOps = [];
    for (const op of mergedOps) {
      const decrypted = await this._decryptOperationIfNeeded(op);
      decryptedMergedOps.push(decrypted);
    }

    if (decryptedMergedOps.length > 0) {
      const newState = this.crdt.getAll();

      if (decryptedMergedOps.length === 1) {
        this.history.addOperation(decryptedMergedOps[0], newState, decryptedMergedOps[0].message || 'Remote update');
      } else {
        this.history.mergeOperations(decryptedMergedOps, newState, `Merged state from ${peerId}`);
      }

      decryptedMergedOps.forEach(op => {
        this.syncedOperations.add(op.id);
      });

      this._emit('config-changed', {
        merged: true,
        operations: decryptedMergedOps,
        from: peerId
      });

      this._emit('history-changed', this.history.getHistory());
    }

    if (vectorClock) {
      this.peerVectorClocks.set(peerId, vectorClock);
    }

    if (history) {
      try {
        this._mergeHistory(history);
      } catch (e) {
        console.warn('[ConfigSync] Failed to merge history:', e);
      }
    }

    this.syncInProgress.delete(peerId);
    this._emit('connected', { peerId, synced: true });
    this._emit('sync-complete', { peerId, incremental: false, operationCount: decryptedMergedOps.length });
  }

  _mergeHistory(remoteHistory) {
    const localHistory = this.history.export();
    const remoteNodes = new Map(Object.entries(remoteHistory.nodes || {}));

    for (const [nodeId, node] of remoteNodes.entries()) {
      if (!this.history.nodes.has(nodeId)) {
        this.history.nodes.set(nodeId, node);
      }
    }

    const remoteEdges = new Map(Object.entries(remoteHistory.edges || {}));
    for (const [nodeId, edges] of remoteEdges.entries()) {
      const existingEdges = this.history.edges.get(nodeId) || [];
      const mergedEdges = [...new Set([...existingEdges, ...edges])];
      this.history.edges.set(nodeId, mergedEdges);
    }
  }

  _handleOperationRequest(peerId, data) {
    const opId = data.operationId;
    const op = this.pendingOperations.get(opId);
    if (op) {
      this.p2p.sendToPeer(peerId, {
        type: 'operation',
        operation: op,
        historyNodeId: data.historyNodeId,
        vectorClock: this.crdt.getVectorClock()
      });
    }
  }

  _handleHistoryRequest(peerId, data) {
    this.p2p.sendToPeer(peerId, {
      type: 'state-sync',
      crdtState: this.crdt.exportState(),
      history: this.history.export(),
      fromNodeId: this.nodeId,
      vectorClock: this.crdt.getVectorClock(),
      encryptedPaths: this.crypto.exportEncryptedPaths()
    });
  }

  async rollbackToVersion(historyNodeId, message = '') {
    const targetNode = this.history.getNode(historyNodeId);
    if (!targetNode) {
      throw new Error(`History node not found: ${historyNodeId}`);
    }

    const targetState = targetNode.state;
    const currentState = this.crdt.getAll();

    const changes = this._calculateStateChanges(currentState, targetState);

    if (changes.length === 0) {
      console.log('[ConfigSync] No changes needed for rollback');
      return { success: true, changes: [] };
    }

    const rollbackOperations = [];
    for (const change of changes) {
      let op;
      if (change.type === 'set') {
        op = this.crdt.set(change.path, change.newValue, `Rollback: set ${change.path}`);
      } else if (change.type === 'delete') {
        op = this.crdt.delete(change.path, `Rollback: delete ${change.path}`);
      }

      if (op) {
        op.isRollback = true;
        op.rollbackFrom = this.history.currentHead;
        op.rollbackTo = historyNodeId;
        rollbackOperations.push(op);
      }
    }

    if (rollbackOperations.length > 0) {
      const newState = this.crdt.getAll();

      const rollbackNode = {
        id: this.history._generateId('rollback'),
        type: 'rollback',
        operations: rollbackOperations,
        state: JSON.parse(JSON.stringify(newState)),
        parents: [this.history.currentHead, historyNodeId],
        timestamp: Date.now(),
        message: message || `Rollback to version ${historyNodeId.slice(-8)}`,
        rollbackFrom: this.history.currentHead,
        rollbackTo: historyNodeId
      };

      this.history.nodes.set(rollbackNode.id, rollbackNode);
      this.history.edges.set(rollbackNode.id, []);

      rollbackNode.parents.forEach(parentId => {
        const parentEdges = this.history.edges.get(parentId) || [];
        parentEdges.push(rollbackNode.id);
        this.history.edges.set(parentId, parentEdges);
      });

      this.history.currentHead = rollbackNode.id;
      this.history.undoStack.push(rollbackNode.id);
      this.history.redoStack = [];

      rollbackOperations.forEach(op => {
        this.syncedOperations.add(op.id);
        this._broadcastOperation(op);
      });

      this.p2p.broadcast({
        type: 'rollback',
        rollbackNodeId: rollbackNode.id,
        targetNodeId: historyNodeId,
        operations: rollbackOperations,
        fromNodeId: this.nodeId
      });

      this._emit('config-changed', {
        rollback: true,
        rollbackNodeId: rollbackNode.id,
        targetNodeId: historyNodeId,
        operations: rollbackOperations,
        changes
      });

      this._emit('rollback-complete', {
        rollbackNodeId: rollbackNode.id,
        targetNodeId: historyNodeId,
        changes
      });

      this._emit('history-changed', this.history.getHistory());

      return { success: true, changes, rollbackNode };
    }

    return { success: true, changes: [] };
  }

  async _handleRollback(peerId, data) {
    const { rollbackNodeId, targetNodeId, operations, fromNodeId } = data;

    if (fromNodeId === this.nodeId) return;

    console.log(`[ConfigSync] Received rollback from ${peerId}: ${rollbackNodeId}`);

    const appliedOps = [];
    for (const op of operations) {
      if (!this.syncedOperations.has(op.id)) {
        const decryptedOp = await this._decryptOperationIfNeeded(op);
        const result = this.crdt.applyRemote(decryptedOp);

        if (result.applied) {
          this.syncedOperations.add(op.id);
          appliedOps.push(decryptedOp);
        }
      }
    }

    if (appliedOps.length > 0) {
      const newState = this.crdt.getAll();
      const targetNode = this.history.getNode(targetNodeId);

      const rollbackNode = {
        id: rollbackNodeId,
        type: 'rollback',
        operations: appliedOps,
        state: JSON.parse(JSON.stringify(newState)),
        parents: [this.history.currentHead, targetNodeId],
        timestamp: Date.now(),
        message: `Rollback from ${peerId}`,
        rollbackFrom: this.history.currentHead,
        rollbackTo: targetNodeId
      };

      if (!this.history.nodes.has(rollbackNodeId)) {
        this.history.nodes.set(rollbackNodeId, rollbackNode);
        this.history.edges.set(rollbackNodeId, []);

        rollbackNode.parents.forEach(parentId => {
          if (this.history.nodes.has(parentId)) {
            const parentEdges = this.history.edges.get(parentId) || [];
            parentEdges.push(rollbackNodeId);
            this.history.edges.set(parentId, parentEdges);
          }
        });
      }

      this.history.currentHead = rollbackNodeId;
      this.history.undoStack.push(rollbackNodeId);
      this.history.redoStack = [];

      this._emit('config-changed', {
        rollback: true,
        remote: true,
        from: peerId,
        rollbackNodeId,
        targetNodeId,
        operations: appliedOps
      });

      this._emit('rollback-complete', {
        rollbackNodeId,
        targetNodeId,
        remote: true,
        from: peerId,
        changes: appliedOps
      });

      this._emit('history-changed', this.history.getHistory());
    }
  }

  _calculateStateChanges(currentState, targetState) {
    const changes = [];
    const currentPaths = this._getAllPaths(currentState);
    const targetPaths = this._getAllPaths(targetState);

    const allPaths = new Set([...currentPaths, ...targetPaths]);

    for (const path of allPaths) {
      const currentValue = this._getByPath(currentState, path);
      const targetValue = this._getByPath(targetState, path);

      if (currentValue !== targetValue) {
        if (targetValue === undefined) {
          changes.push({ type: 'delete', path, oldValue: currentValue, newValue: undefined });
        } else {
          changes.push({ type: 'set', path, oldValue: currentValue, newValue: targetValue });
        }
      }
    }

    return changes;
  }

  _getAllPaths(obj, prefix = '') {
    const paths = [];
    if (!obj || typeof obj !== 'object') return paths;

    for (const [key, value] of Object.entries(obj)) {
      const fullPath = prefix ? `${prefix}.${key}` : key;
      paths.push(fullPath);

      if (value && typeof value === 'object' && !Array.isArray(value)) {
        paths.push(...this._getAllPaths(value, fullPath));
      }
    }

    return paths;
  }

  _getByPath(obj, path) {
    const parts = path.split('.');
    let current = obj;
    for (const part of parts) {
      if (current === undefined || current === null) return undefined;
      current = current[part];
    }
    return current;
  }

  undo() {
    const result = this.history.undo();
    if (!result) return null;

    const { operation, previousState } = result;

    this.crdt.importState({
      root: previousState,
      metadata: this.crdt.getMetadataMap(),
      vectorClock: this.crdt.getVectorClock(),
      deletedPaths: []
    });

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
      historyNodeId: this.history.currentHead,
      vectorClock: this.crdt.getVectorClock()
    });

    return result;
  }

  redo() {
    const result = this.history.redo();
    if (!result) return null;

    const { operation, newState } = result;

    this.crdt.importState({
      root: newState,
      metadata: this.crdt.getMetadataMap(),
      vectorClock: this.crdt.getVectorClock(),
      deletedPaths: []
    });

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
      historyNodeId: this.history.currentHead,
      vectorClock: this.crdt.getVectorClock()
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

  getVectorClock() {
    return this.crdt.getVectorClock();
  }

  getStatus() {
    const peerVCs = {};
    this.peerVectorClocks.forEach((vc, peerId) => {
      peerVCs[peerId] = vc;
    });

    const encryptedPaths = {};
    this.crypto.exportEncryptedPaths().forEach(path => {
      encryptedPaths[path] = {
        hasKey: this.crypto.hasKeyForPath(path),
        ...this.encryptedPathMetadata.get(path)
      };
    });

    return {
      nodeId: this.nodeId,
      roomId: this.roomId,
      config: this.getConfig(),
      configKeys: Object.keys(this.getConfig()),
      historyLength: this.history.getHistory().length,
      undoStackSize: this.history.undoStack.length,
      redoStackSize: this.history.redoStack.length,
      vectorClock: this.crdt.getVectorClock(),
      peerVectorClocks: peerVCs,
      syncInProgress: Array.from(this.syncInProgress),
      encryptedPaths,
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

  async batchSet(entries, message = 'Batch update') {
    const operations = [];

    for (const [key, value] of entries) {
      const encryptedValue = await this._maybeEncryptValue(key, value);
      const op = this.crdt.set(key, encryptedValue, message);
      if (op) {
        if (this.crypto.isPathEncrypted(key)) {
          op.isEncrypted = true;
          op.encryptedPath = this.crypto.getEncryptedParentPath(key);
          op.originalValue = value;
        }
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
        const opToSend = op.originalValue
          ? { ...op, value: op.value, originalValue: undefined }
          : op;

        this.p2p.broadcast({
          type: 'operation',
          operation: opToSend,
          historyNodeId: node.id,
          vectorClock: this.crdt.getVectorClock()
        });
      });

      this._emit('config-changed', { batch: true, operations });
      this._emit('history-changed', this.history.getHistory());
    }

    return operations;
  }

  setNested(path, value, message = '') {
    return this.set(path, value, message);
  }

  getNested(path) {
    return this.get(path);
  }

  deleteNested(path, message = '') {
    return this.delete(path, message);
  }
}

window.ConfigSync = ConfigSync;
