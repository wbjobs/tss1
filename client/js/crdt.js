class CRDTMap {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.state = {};
    this.metadata = {};
    this.clock = 0;
  }

  _tick() {
    this.clock++;
    return `${this.clock.toString().padStart(10, '0')}-${this.nodeId}`;
  }

  set(key, value) {
    const timestamp = this._tick();
    const operation = {
      id: `op-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
      type: 'set',
      key,
      value,
      timestamp,
      nodeId: this.nodeId,
      prevValue: this.state[key] !== undefined ? this.state[key] : null,
      metadata: {
        lamport: this.clock,
        nodeId: this.nodeId
      }
    };

    this._apply(operation);
    return operation;
  }

  delete(key) {
    if (this.state[key] === undefined) return null;

    const timestamp = this._tick();
    const operation = {
      id: `op-${timestamp}-${Math.random().toString(36).substr(2, 9)}`,
      type: 'delete',
      key,
      prevValue: this.state[key],
      timestamp,
      nodeId: this.nodeId,
      metadata: {
        lamport: this.clock,
        nodeId: this.nodeId
      }
    };

    this._apply(operation);
    return operation;
  }

  get(key) {
    return this.state[key];
  }

  getAll() {
    return { ...this.state };
  }

  _apply(operation) {
    const { type, key, value, timestamp, metadata } = operation;

    const currentMeta = this.metadata[key];
    if (currentMeta && !this._shouldApply(metadata, currentMeta)) {
      return false;
    }

    if (type === 'set') {
      this.state[key] = value;
    } else if (type === 'delete') {
      delete this.state[key];
    }

    this.metadata[key] = { ...metadata, timestamp };

    if (metadata.lamport > this.clock) {
      this.clock = metadata.lamport;
    }

    return true;
  }

  applyRemote(operation) {
    if (operation.nodeId === this.nodeId) {
      return { applied: false, reason: 'local-operation' };
    }

    const applied = this._apply(operation);
    return { applied, operation };
  }

  _shouldApply(newMeta, currentMeta) {
    if (!currentMeta) return true;

    if (newMeta.lamport > currentMeta.lamport) {
      return true;
    }
    if (newMeta.lamport < currentMeta.lamport) {
      return false;
    }

    return newMeta.nodeId > currentMeta.nodeId;
  }

  merge(other) {
    const operations = [];
    for (const key of Object.keys(other.state)) {
      if (!this.metadata[key] || this._shouldApply(other.metadata[key], this.metadata[key])) {
        const op = {
          type: 'set',
          key,
          value: other.state[key],
          timestamp: other.metadata[key].timestamp,
          nodeId: other.metadata[key].nodeId,
          metadata: other.metadata[key]
        };
        this._apply(op);
        operations.push(op);
      }
    }
    return operations;
  }
}

class CRDTSet {
  constructor(nodeId) {
    this.nodeId = nodeId;
    this.tombstones = new Map();
    this.values = new Map();
    this.clock = 0;
  }

  _tick() {
    this.clock++;
    return `${this.clock.toString().padStart(10, '0')}-${this.nodeId}`;
  }

  add(value) {
    const timestamp = this._tick();
    const op = {
      type: 'add',
      value,
      timestamp,
      nodeId: this.nodeId
    };
    this._applyAdd(value, timestamp);
    return op;
  }

  remove(value) {
    const timestamp = this._tick();
    const op = {
      type: 'remove',
      value,
      timestamp,
      nodeId: this.nodeId
    };
    this._applyRemove(value, timestamp);
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

  _applyAdd(value, timestamp) {
    const existing = this.values.get(value);
    if (!existing || this._compareTimestamps(timestamp, existing) > 0) {
      this.values.set(value, timestamp);
    }
  }

  _applyRemove(value, timestamp) {
    const existing = this.tombstones.get(value);
    if (!existing || this._compareTimestamps(timestamp, existing) > 0) {
      this.tombstones.set(value, timestamp);
    }
  }

  applyRemote(op) {
    if (op.type === 'add') {
      this._applyAdd(op.value, op.timestamp);
    } else if (op.type === 'remove') {
      this._applyRemove(op.value, op.timestamp);
    }
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
}

window.CRDTMap = CRDTMap;
window.CRDTSet = CRDTSet;
