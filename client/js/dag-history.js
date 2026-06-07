class DAGHistory {
  constructor() {
    this.nodes = new Map();
    this.edges = new Map();
    this.currentHead = null;
    this.rootId = null;
    this.undoStack = [];
    this.redoStack = [];
  }

  createRoot(initialState = {}) {
    const rootNode = {
      id: this._generateId('root'),
      type: 'root',
      state: JSON.parse(JSON.stringify(initialState)),
      parents: [],
      timestamp: Date.now(),
      message: 'Initial state'
    };

    this.nodes.set(rootNode.id, rootNode);
    this.edges.set(rootNode.id, []);
    this.rootId = rootNode.id;
    this.currentHead = rootNode.id;

    return rootNode;
  }

  addOperation(operation, newState, message = '') {
    const node = {
      id: this._generateId('op'),
      type: 'operation',
      operation,
      state: JSON.parse(JSON.stringify(newState)),
      parents: this.currentHead ? [this.currentHead] : [],
      timestamp: Date.now(),
      message: message || `${operation.type} ${operation.key}`
    };

    this.nodes.set(node.id, node);
    this.edges.set(node.id, []);

    if (this.currentHead) {
      const parentEdges = this.edges.get(this.currentHead) || [];
      parentEdges.push(node.id);
      this.edges.set(this.currentHead, parentEdges);
    }

    this.currentHead = node.id;
    this.undoStack.push(node.id);
    this.redoStack = [];

    return node;
  }

  mergeOperations(operations, newState, message = 'Merge') {
    const parents = operations.map(op => op.id || this.currentHead).filter(Boolean);
    const uniqueParents = [...new Set(parents)];

    const node = {
      id: this._generateId('merge'),
      type: 'merge',
      operations,
      state: JSON.parse(JSON.stringify(newState)),
      parents: uniqueParents,
      timestamp: Date.now(),
      message
    };

    this.nodes.set(node.id, node);
    this.edges.set(node.id, []);

    uniqueParents.forEach(parentId => {
      const parentEdges = this.edges.get(parentId) || [];
      parentEdges.push(node.id);
      this.edges.set(parentId, parentEdges);
    });

    this.currentHead = node.id;

    return node;
  }

  rollback(targetNodeId, operations, newState, message = '') {
    const targetNode = this.nodes.get(targetNodeId);
    if (!targetNode) {
      throw new Error(`Target node not found: ${targetNodeId}`);
    }

    const rollbackNode = {
      id: this._generateId('rollback'),
      type: 'rollback',
      operations,
      state: JSON.parse(JSON.stringify(newState)),
      parents: [this.currentHead, targetNodeId],
      timestamp: Date.now(),
      message: message || `Rollback to ${targetNodeId.slice(-8)}`,
      rollbackFrom: this.currentHead,
      rollbackTo: targetNodeId
    };

    this.nodes.set(rollbackNode.id, rollbackNode);
    this.edges.set(rollbackNode.id, []);

    rollbackNode.parents.forEach(parentId => {
      const parentEdges = this.edges.get(parentId) || [];
      parentEdges.push(rollbackNode.id);
      this.edges.set(parentId, parentEdges);
    });

    this.currentHead = rollbackNode.id;
    this.undoStack.push(rollbackNode.id);
    this.redoStack = [];

    return rollbackNode;
  }

  undo() {
    if (this.undoStack.length === 0) return null;

    const nodeId = this.undoStack.pop();
    const node = this.nodes.get(nodeId);

    if (!node || !node.parents || node.parents.length === 0) {
      this.undoStack.push(nodeId);
      return null;
    }

    const parentId = node.parents[0];
    const parent = this.nodes.get(parentId);

    this.currentHead = parentId;
    this.redoStack.push(nodeId);

    return {
      node,
      previousState: parent ? parent.state : {},
      operation: node.operation
    };
  }

  redo() {
    if (this.redoStack.length === 0) return null;

    const nodeId = this.redoStack.pop();
    const node = this.nodes.get(nodeId);

    if (!node) {
      this.redoStack.push(nodeId);
      return null;
    }

    this.currentHead = nodeId;
    this.undoStack.push(nodeId);

    return {
      node,
      newState: node.state,
      operation: node.operation
    };
  }

  canUndo() {
    return this.undoStack.length > 0;
  }

  canRedo() {
    return this.redoStack.length > 0;
  }

  getHistory() {
    const visited = new Set();
    const result = [];

    const traverse = (nodeId) => {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);

      const node = this.nodes.get(nodeId);
      if (!node) return;

      result.push({
        id: node.id,
        type: node.type,
        message: node.message,
        timestamp: node.timestamp,
        parents: node.parents,
        isHead: nodeId === this.currentHead
      });

      const children = this.edges.get(nodeId) || [];
      children.forEach(childId => traverse(childId));
    };

    if (this.rootId) {
      traverse(this.rootId);
    }

    return result.sort((a, b) => b.timestamp - a.timestamp);
  }

  getCurrentState() {
    if (!this.currentHead) return {};
    const node = this.nodes.get(this.currentHead);
    return node ? JSON.parse(JSON.stringify(node.state)) : {};
  }

  getNode(nodeId) {
    const node = this.nodes.get(nodeId);
    return node ? JSON.parse(JSON.stringify(node)) : null;
  }

  getAncestors(nodeId) {
    const ancestors = [];
    const visited = new Set();

    const traverse = (id) => {
      if (visited.has(id)) return;
      visited.add(id);

      const node = this.nodes.get(id);
      if (!node) return;

      ancestors.push(id);
      node.parents.forEach(parentId => traverse(parentId));
    };

    traverse(nodeId);
    return ancestors;
  }

  findCommonAncestor(nodeId1, nodeId2) {
    const ancestors1 = new Set(this.getAncestors(nodeId1));
    const ancestors2 = this.getAncestors(nodeId2);

    for (const id of ancestors2) {
      if (ancestors1.has(id)) {
        return id;
      }
    }

    return null;
  }

  export() {
    return {
      nodes: Object.fromEntries(this.nodes),
      edges: Object.fromEntries(this.edges),
      currentHead: this.currentHead,
      rootId: this.rootId,
      undoStack: [...this.undoStack],
      redoStack: [...this.redoStack]
    };
  }

  import(data) {
    this.nodes = new Map(Object.entries(data.nodes));
    this.edges = new Map(Object.entries(data.edges));
    this.currentHead = data.currentHead;
    this.rootId = data.rootId;
    this.undoStack = [...data.undoStack];
    this.redoStack = [...data.redoStack];
  }

  _generateId(prefix) {
    return `${prefix}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
  }

  getDAGStructure() {
    const nodes = [];
    const edges = [];
    const visited = new Set();

    const traverse = (nodeId) => {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);

      const node = this.nodes.get(nodeId);
      if (!node) return;

      nodes.push({
        id: node.id,
        type: node.type,
        message: node.message,
        timestamp: node.timestamp,
        isHead: nodeId === this.currentHead
      });

      const children = this.edges.get(nodeId) || [];
      children.forEach(childId => {
        edges.push({ from: nodeId, to: childId });
        traverse(childId);
      });
    };

    if (this.rootId) {
      traverse(this.rootId);
    }

    return { nodes, edges };
  }
}

window.DAGHistory = DAGHistory;
