class VectorClock {
  constructor() {
    this.clocks = new Map();
  }

  increment(nodeId) {
    const current = this.clocks.get(nodeId) || 0;
    this.clocks.set(nodeId, current + 1);
    return current + 1;
  }

  get(nodeId) {
    return this.clocks.get(nodeId) || 0;
  }

  set(nodeId, value) {
    const current = this.clocks.get(nodeId) || 0;
    if (value > current) {
      this.clocks.set(nodeId, value);
    }
  }

  merge(other) {
    const result = new VectorClock();
    for (const [nodeId, value] of this.clocks.entries()) {
      result.set(nodeId, value);
    }
    if (other instanceof VectorClock) {
      for (const [nodeId, value] of other.clocks.entries()) {
        result.set(nodeId, value);
      }
    } else if (other && typeof other === 'object') {
      for (const [nodeId, value] of Object.entries(other)) {
        result.set(nodeId, value);
      }
    }
    return result;
  }

  compare(other) {
    let thisGreater = false;
    let otherGreater = false;

    const allNodes = new Set([
      ...this.clocks.keys(),
      ...(other instanceof VectorClock ? other.clocks.keys() : Object.keys(other || {}))
    ]);

    for (const nodeId of allNodes) {
      const thisVal = this.get(nodeId);
      const otherVal = other instanceof VectorClock ? other.get(nodeId) : (other?.[nodeId] || 0);

      if (thisVal > otherVal) thisGreater = true;
      if (thisVal < otherVal) otherGreater = true;
    }

    if (thisGreater && otherGreater) return 'concurrent';
    if (thisGreater) return 'greater';
    if (otherGreater) return 'less';
    return 'equal';
  }

  isGreaterThan(other) {
    return this.compare(other) === 'greater';
  }

  isLessThan(other) {
    return this.compare(other) === 'less';
  }

  isConcurrentWith(other) {
    return this.compare(other) === 'concurrent';
  }

  toJSON() {
    return Object.fromEntries(this.clocks);
  }

  static fromJSON(json) {
    const vc = new VectorClock();
    if (json) {
      for (const [nodeId, value] of Object.entries(json)) {
        vc.set(nodeId, value);
      }
    }
    return vc;
  }

  clone() {
    const vc = new VectorClock();
    for (const [nodeId, value] of this.clocks.entries()) {
      vc.set(nodeId, value);
    }
    return vc;
  }
}

class CRDTMap {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.root = {};
    this.metadata = new Map();
    this.vectorClock = new VectorClock();
    this.deletedPaths = new Set();
  }

  _tick() {
    const lamport = this.vectorClock.increment(this.nodeId);
    return `${lamport.toString().padStart(10, '0')}-${this.nodeId}`;
  }

  _parsePath(path) {
    if (Array.isArray(path)) return path;
    return path.split('.').filter(p => p.length > 0);
  }

  _getPathKey(path) {
    return Array.isArray(path) ? path.join('.') : path;
  }

  _getObjectByPath(obj, path, createIfMissing = false) {
    const parts = this._parsePath(path);
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      if (current[part] === undefined || current[part] === null) {
        if (createIfMissing) {
          current[part] = {};
        } else {
          return { value: undefined, parent: null, key: null, exists: false };
        }
      }
      if (typeof current[part] !== 'object' || Array.isArray(current[part])) {
        return { value: undefined, parent: null, key: null, exists: false };
      }
      current = current[part];
    }

    const lastKey = parts[parts.length - 1];
    return {
      value: current[lastKey],
      parent: current,
      key: lastKey,
      exists: lastKey in current
    };
  }

  _setByPath(obj, path, value) {
    const parts = this._parsePath(path);
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      if (current[part] === undefined || current[part] === null || typeof current[part] !== 'object') {
        current[part] = {};
      }
      current = current[part];
    }

    const lastKey = parts[parts.length - 1];
    const prevValue = current[lastKey];
    current[lastKey] = value;
    return prevValue;
  }

  _deleteByPath(obj, path) {
    const parts = this._parsePath(path);
    let current = obj;

    for (let i = 0; i < parts.length - 1; i++) {
      const part = parts[i];
      if (!current[part] || typeof current[part] !== 'object') {
        return undefined;
      }
      current = current[part];
    }

    const lastKey = parts[parts.length - 1];
    const prevValue = current[lastKey];
    delete current[lastKey];
    return prevValue;
  }

  _deepMerge(target, source, pathPrefix = '') {
    const mergedOps = [];
    const targetMeta = this.metadata;

    for (const key of Object.keys(source)) {
      const fullPath = pathPrefix ? `${pathPrefix}.${key}` : key;
      const sourceValue = source[key];
      const targetValue = target[key];

      if (sourceValue && typeof sourceValue === 'object' && !Array.isArray(sourceValue)) {
        if (targetValue === undefined || targetValue === null || typeof targetValue !== 'object') {
          target[key] = {};
        }
        const nestedOps = this._deepMerge(target[key], sourceValue, fullPath);
        mergedOps.push(...nestedOps);
      } else {
        const currentMeta = targetMeta.get(fullPath);
        const sourceMeta = targetMeta.get(fullPath);

        if (!currentMeta || this._shouldApply(sourceMeta, currentMeta)) {
          const prevValue = target[key];
          target[key] = sourceValue;

          mergedOps.push({
            type: 'set',
            key: fullPath,
            value: sourceValue,
            prevValue,
            metadata: sourceMeta
          });
        }
      }
    }

    return mergedOps;
  }

  set(path, value, message = '') {
    const timestamp = this._tick();
    const pathKey = this._getPathKey(path);
    const prevValue = this._getObjectByPath(this.root, path).value;

    const metadata = {
      lamport: this.vectorClock.get(this.nodeId),
      nodeId: this.nodeId,
      timestamp,
      vectorClock: this.vectorClock.toJSON()
    };

    const operation = {
      id: `op-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
      type: 'set',
      path: pathKey,
      key: pathKey,
      value: JSON.parse(JSON.stringify(value)),
      prevValue: prevValue !== undefined ? JSON.parse(JSON.stringify(prevValue)) : null,
      timestamp,
      nodeId: this.nodeId,
      message,
      metadata: { ...metadata }
    };

    this._apply(operation);
    return operation;
  }

  delete(path, message = '') {
    const result = this._getObjectByPath(this.root, path);
    if (!result.exists) return null;

    const timestamp = this._tick();
    const pathKey = this._getPathKey(path);

    const metadata = {
      lamport: this.vectorClock.get(this.nodeId),
      nodeId: this.nodeId,
      timestamp,
      vectorClock: this.vectorClock.toJSON()
    };

    const operation = {
      id: `op-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
      type: 'delete',
      path: pathKey,
      key: pathKey,
      prevValue: JSON.parse(JSON.stringify(result.value)),
      timestamp,
      nodeId: this.nodeId,
      message,
      metadata: { ...metadata }
    };

    this._apply(operation);
    return operation;
  }

  get(path) {
    const result = this._getObjectByPath(this.root, path);
    return result.exists ? JSON.parse(JSON.stringify(result.value)) : undefined;
  }

  getAll() {
    return JSON.parse(JSON.stringify(this.root));
  }

  has(path) {
    return this._getObjectByPath(this.root, path).exists;
  }

  _apply(operation) {
    const { type, path, value, metadata } = operation;
    const pathKey = this._getPathKey(path);

    const currentMeta = this.metadata.get(pathKey);
    if (currentMeta && !this._shouldApply(metadata, currentMeta)) {
      return { applied: false, conflict: true, reason: 'stale-operation' };
    }

    let prevValue;
    if (type === 'set') {
      prevValue = this._setByPath(this.root, path, JSON.parse(JSON.stringify(value)));
      this.deletedPaths.delete(pathKey);
    } else if (type === 'delete') {
      prevValue = this._deleteByPath(this.root, path);
      this.deletedPaths.add(pathKey);
    }

    this.metadata.set(pathKey, { ...metadata });

    if (metadata.vectorClock) {
      this.vectorClock = this.vectorClock.merge(VectorClock.fromJSON(metadata.vectorClock));
    }

    return { applied: true, prevValue, conflict: false };
  }

  applyRemote(operation) {
    if (operation.nodeId === this.nodeId) {
      return { applied: false, reason: 'local-operation' };
    }

    const result = this._apply(operation);
    return { ...result, operation };
  }

  _shouldApply(newMeta, currentMeta) {
    if (!currentMeta) return true;
    if (!newMeta) return false;

    if (newMeta.vectorClock && currentMeta.vectorClock) {
      const newVC = VectorClock.fromJSON(newMeta.vectorClock);
      const currentVC = VectorClock.fromJSON(currentMeta.vectorClock);

      if (newVC.isGreaterThan(currentVC)) return true;
      if (newVC.isLessThan(currentVC)) return false;
    }

    if (newMeta.lamport > currentMeta.lamport) {
      return true;
    }
    if (newMeta.lamport < currentMeta.lamport) {
      return false;
    }

    return newMeta.nodeId > currentMeta.nodeId;
  }

  merge(other) {
    if (!other || !other.root) return [];

    const operations = [];
    const mergedOps = this._deepMerge(this.root, other.root);

    for (const op of mergedOps) {
      const timestamp = op.metadata?.timestamp || this._tick();
      const operation = {
        id: `op-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
        type: op.type,
        path: op.key,
        key: op.key,
        value: op.value,
        prevValue: op.prevValue,
        timestamp,
        nodeId: op.metadata?.nodeId || this.nodeId,
        metadata: op.metadata || {
          lamport: this.vectorClock.get(this.nodeId),
          nodeId: this.nodeId,
          timestamp,
          vectorClock: this.vectorClock.toJSON()
        }
      };
      operations.push(operation);
    }

    if (other.vectorClock) {
      this.vectorClock = this.vectorClock.merge(
        other.vectorClock instanceof VectorClock
          ? other.vectorClock
          : VectorClock.fromJSON(other.vectorClock)
      );
    }

    if (other.metadata) {
      const otherMeta = other.metadata instanceof Map
        ? other.metadata
        : new Map(Object.entries(other.metadata || {}));

      for (const [path, meta] of otherMeta.entries()) {
        const currentMeta = this.metadata.get(path);
        if (!currentMeta || this._shouldApply(meta, currentMeta)) {
          this.metadata.set(path, { ...meta });
        }
      }
    }

    return operations;
  }

  getVectorClock() {
    return this.vectorClock.toJSON();
  }

  setVectorClock(vc) {
    this.vectorClock = VectorClock.fromJSON(vc);
  }

  getOperationsSince(sinceVectorClock) {
    const sinceVC = VectorClock.fromJSON(sinceVectorClock);
    const operations = [];

    for (const [path, meta] of this.metadata.entries()) {
      if (meta.vectorClock) {
        const opVC = VectorClock.fromJSON(meta.vectorClock);
        if (opVC.isGreaterThan(sinceVC)) {
          const value = this.get(path);
          if (value !== undefined || this.deletedPaths.has(path)) {
            operations.push({
              type: this.deletedPaths.has(path) ? 'delete' : 'set',
              path,
              key: path,
              value,
              timestamp: meta.timestamp,
              nodeId: meta.nodeId,
              metadata: { ...meta }
            });
          }
        }
      }
    }

    return operations.sort((a, b) => {
      const lamportA = a.metadata?.lamport || 0;
      const lamportB = b.metadata?.lamport || 0;
      if (lamportA !== lamportB) return lamportA - lamportB;
      return a.nodeId.localeCompare(b.nodeId);
    });
  }

  applyOperations(operations) {
    const results = [];
    for (const op of operations) {
      const result = this.applyRemote(op);
      if (result.applied) {
        results.push(op);
      }
    }
    return results;
  }

  exportState() {
    return {
      root: JSON.parse(JSON.stringify(this.root)),
      metadata: Object.fromEntries(this.metadata),
      vectorClock: this.vectorClock.toJSON(),
      deletedPaths: Array.from(this.deletedPaths)
    };
  }

  importState(state) {
    if (!state) return;

    this.root = JSON.parse(JSON.stringify(state.root || {}));
    this.metadata = new Map(Object.entries(state.metadata || {}));
    this.vectorClock = VectorClock.fromJSON(state.vectorClock);
    this.deletedPaths = new Set(state.deletedPaths || []);
  }

  getMetadataMap() {
    return Object.fromEntries(this.metadata);
  }
}

class CRDTSet {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.tombstones = new Map();
    this.values = new Map();
    this.vectorClock = new VectorClock();
  }

  _tick() {
    const lamport = this.vectorClock.increment(this.nodeId);
    return `${lamport.toString().padStart(10, '0')}-${this.nodeId}`;
  }

  add(value, message = '') {
    const timestamp = this._tick();
    const op = {
      id: `op-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
      type: 'add',
      value,
      timestamp,
      nodeId: this.nodeId,
      message,
      metadata: {
        lamport: this.vectorClock.get(this.nodeId),
        nodeId: this.nodeId,
        timestamp,
        vectorClock: this.vectorClock.toJSON()
      }
    };
    this._applyAdd(value, timestamp, op.metadata);
    return op;
  }

  remove(value, message = '') {
    const timestamp = this._tick();
    const op = {
      id: `op-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
      type: 'remove',
      value,
      timestamp,
      nodeId: this.nodeId,
      message,
      metadata: {
        lamport: this.vectorClock.get(this.nodeId),
        nodeId: this.nodeId,
        timestamp,
        vectorClock: this.vectorClock.toJSON()
      }
    };
    this._applyRemove(value, timestamp, op.metadata);
    return op;
  }

  has(value) {
    const addTime = this.values.get(value);
    const removeTime = this.tombstones.get(value);
    if (!addTime) return false;
    if (!removeTime) return true;
    return this._compareTimestamps(addTime, removeTime) > 0;
  }

  getValues() {
    const result = [];
    for (const [value, addTime] of this.values.entries()) {
      const removeTime = this.tombstones.get(value);
      if (!removeTime || this._compareTimestamps(addTime, removeTime) > 0) {
        result.push(value);
      }
    }
    return result;
  }

  _applyAdd(value, timestamp, metadata) {
    const existing = this.values.get(value);
    if (!existing || this._compareTimestamps(timestamp, existing) > 0) {
      this.values.set(value, timestamp);
    }
    if (metadata?.vectorClock) {
      this.vectorClock = this.vectorClock.merge(VectorClock.fromJSON(metadata.vectorClock));
    }
  }

  _applyRemove(value, timestamp, metadata) {
    const existing = this.tombstones.get(value);
    if (!existing || this._compareTimestamps(timestamp, existing) > 0) {
      this.tombstones.set(value, timestamp);
    }
    if (metadata?.vectorClock) {
      this.vectorClock = this.vectorClock.merge(VectorClock.fromJSON(metadata.vectorClock));
    }
  }

  applyRemote(op) {
    if (op.nodeId === this.nodeId) {
      return { applied: false, reason: 'local-operation' };
    }

    if (op.type === 'add') {
      this._applyAdd(op.value, op.timestamp, op.metadata);
    } else if (op.type === 'remove') {
      this._applyRemove(op.value, op.timestamp, op.metadata);
    }

    return { applied: true, operation: op };
  }

  _compareTimestamps(t1, t2) {
    const [lamport1, node1] = t1.split('-');
    const [lamport2, node2] = t2.split('-');

    const l1 = parseInt(lamport1, 10);
    const l2 = parseInt(lamport2, 10);

    if (l1 !== l2) {
      return l1 - l2;
    }
    return node1.localeCompare(node2);
  }

  getVectorClock() {
    return this.vectorClock.toJSON();
  }

  merge(other) {
    const operations = [];

    if (other.values instanceof Map) {
      for (const [value, timestamp] of other.values.entries()) {
        if (!this.values.has(value) || this._compareTimestamps(timestamp, this.values.get(value)) > 0) {
          this.values.set(value, timestamp);
          operations.push({ type: 'add', value, timestamp });
        }
      }
    }

    if (other.tombstones instanceof Map) {
      for (const [value, timestamp] of other.tombstones.entries()) {
        if (!this.tombstones.has(value) || this._compareTimestamps(timestamp, this.tombstones.get(value)) > 0) {
          this.tombstones.set(value, timestamp);
          operations.push({ type: 'remove', value, timestamp });
        }
      }
    }

    if (other.vectorClock) {
      this.vectorClock = this.vectorClock.merge(
        other.vectorClock instanceof VectorClock
          ? other.vectorClock
          : VectorClock.fromJSON(other.vectorClock)
      );
    }

    return operations;
  }
}

window.VectorClock = VectorClock;
window.CRDTMap = CRDTMap;
window.CRDTSet = CRDTSet;
