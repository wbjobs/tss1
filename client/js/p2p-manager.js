class P2PManager {
  constructor(nodeId, roomId, signalingUrl) {
    this.nodeId = nodeId;
    this.roomId = roomId;
    this.signalingUrl = signalingUrl;
    this.peers = new Map();
    this.peerConnections = new Map();
    this.dataChannels = new Map();
    this.peerReconnectAttempts = new Map();
    this.peerIceGatheringStates = new Map();
    this.messageQueue = new Map();
    this.peerVectorClocks = new Map();
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.maxPeerReconnectAttempts = 8;
    this.isConnected = false;

    this.listeners = {
      'peer-connected': [],
      'peer-disconnected': [],
      'peer-reconnecting': [],
      'peer-reconnected': [],
      'message': [],
      'connected': [],
      'disconnected': [],
      'state-change': [],
      'ice-failed': [],
      'vector-clock-update': []
    };

    this.iceServers = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' },
      { urls: 'stun:stun3.l.google.com:19302' },
      { urls: 'stun:stun4.l.google.com:19302' }
    ];

    this.iceGatheringTimeout = 10000;
    this.peerReconnectDelay = 2000;
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
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.signalingUrl);

        this.ws.onopen = () => {
          console.log('[P2P] Connected to signaling server');
          this.reconnectAttempts = 0;
          this.isConnected = true;
          this._joinRoom();
          this._emit('connected', { nodeId: this.nodeId, roomId: this.roomId });
          resolve();
        };

        this.ws.onmessage = (event) => {
          const data = JSON.parse(event.data);
          this._handleSignalingMessage(data);
        };

        this.ws.onerror = (err) => {
          console.error('[P2P] WebSocket error:', err);
          reject(err);
        };

        this.ws.onclose = () => {
          console.log('[P2P] Disconnected from signaling server');
          this.isConnected = false;
          this._emit('disconnected');
          this._handleReconnect();
        };
      } catch (err) {
        reject(err);
      }
    });
  }

  _joinRoom() {
    this._sendSignaling({
      type: 'join',
      clientId: this.nodeId,
      roomId: this.roomId
    });
  }

  _handleSignalingMessage(data) {
    console.log('[P2P] Received signaling message:', data.type);

    switch (data.type) {
      case 'joined':
        this._handleJoined(data);
        break;
      case 'peer-joined':
        this._handlePeerJoined(data.peerId);
        break;
      case 'peer-left':
        this._handlePeerLeft(data.peerId);
        break;
      case 'offer':
        this._handleOffer(data.from, data.payload);
        break;
      case 'answer':
        this._handleAnswer(data.from, data.payload);
        break;
      case 'ice-candidate':
        this._handleIceCandidate(data.from, data.payload);
        break;
      case 'pong':
        break;
    }
  }

  _handleJoined(data) {
    console.log('[P2P] Joined room, peers:', data.peers);
    data.peers.forEach(peerId => {
      this._initiateConnection(peerId, true);
    });
  }

  async _handlePeerJoined(peerId) {
    console.log('[P2P] New peer joined:', peerId);
    this._emit('peer-connected', { peerId });
    this._initiateConnection(peerId, true);
  }

  _handlePeerLeft(peerId) {
    console.log('[P2P] Peer left:', peerId);
    this._cleanupPeer(peerId);
    this._emit('peer-disconnected', { peerId });
  }

  async _initiateConnection(peerId, isInitiator, isReconnect = false) {
    console.log(`[P2P] ${isReconnect ? 'Reconnecting' : isInitiator ? 'Initiating' : 'Accepting'} connection with ${peerId}`);

    if (this.peerConnections.has(peerId) && !isReconnect) {
      const pc = this.peerConnections.get(peerId);
      if (pc.connectionState === 'connected' || pc.connectionState === 'connecting') {
        console.log(`[P2P] Already connected/connecting to ${peerId}`);
        return;
      }
      this._cleanupPeer(peerId, false);
    }

    const pc = new RTCPeerConnection({
      iceServers: this.iceServers,
      iceTransportPolicy: 'all',
      bundlePolicy: 'max-bundle'
    });

    this.peerConnections.set(peerId, pc);
    this.peerIceGatheringStates.set(peerId, 'gathering');

    const iceTimeout = setTimeout(() => {
      const state = this.peerIceGatheringStates.get(peerId);
      if (state === 'gathering') {
        console.log(`[P2P] ICE gathering timeout for ${peerId}, completing anyway`);
        if (pc.iceGatheringState !== 'complete') {
          this._sendSignaling({
            type: pc.localDescription?.type === 'offer' ? 'offer' : 'answer',
            from: this.nodeId,
            targetId: peerId,
            roomId: this.roomId,
            payload: pc.localDescription
          });
        }
        this.peerIceGatheringStates.set(peerId, 'complete');
      }
    }, this.iceGatheringTimeout);

    if (isInitiator) {
      const dc = pc.createDataChannel('config-sync', {
        ordered: true,
        reliable: true,
        protocol: 'json'
      });
      this._setupDataChannel(dc, peerId);
    }

    pc.onicecandidate = (event) => {
      console.log(`[P2P] ICE candidate for ${peerId}:`, event.candidate?.candidate || '(end of candidates)');
      if (event.candidate) {
        this._sendSignaling({
          type: 'ice-candidate',
          from: this.nodeId,
          targetId: peerId,
          roomId: this.roomId,
          payload: event.candidate
        });
      } else {
        console.log(`[P2P] ICE gathering complete for ${peerId}`);
        clearTimeout(iceTimeout);
        this.peerIceGatheringStates.set(peerId, 'complete');
      }
    };

    pc.onicegatheringstatechange = () => {
      console.log(`[P2P] ICE gathering state for ${peerId}: ${pc.iceGatheringState}`);
      if (pc.iceGatheringState === 'complete') {
        clearTimeout(iceTimeout);
        this.peerIceGatheringStates.set(peerId, 'complete');
      }
    };

    pc.oniceconnectionstatechange = () => {
      console.log(`[P2P] ICE connection state for ${peerId}: ${pc.iceConnectionState}`);

      if (pc.iceConnectionState === 'failed') {
        console.error(`[P2P] ICE connection failed for ${peerId}`);
        clearTimeout(iceTimeout);
        this._emit('ice-failed', { peerId, state: pc.iceConnectionState });
        this._handlePeerConnectionFailed(peerId, pc);
      } else if (pc.iceConnectionState === 'disconnected') {
        console.warn(`[P2P] ICE connection disconnected for ${peerId}, attempting to restart ICE`);
        this._attemptIceRestart(peerId, pc);
      } else if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
        this.peerReconnectAttempts.set(peerId, 0);
      }
    };

    pc.onconnectionstatechange = () => {
      console.log(`[P2P] Connection state with ${peerId}: ${pc.connectionState}`);
      this._emit('state-change', { peerId, state: pc.connectionState });

      if (pc.connectionState === 'connected') {
        this.peers.set(peerId, true);
        this.peerReconnectAttempts.set(peerId, 0);
        this._flushMessageQueue(peerId);

        if (isReconnect) {
          this._emit('peer-reconnected', { peerId });
        }

        this._requestSyncFromPeer(peerId);
      } else if (pc.connectionState === 'failed' || pc.connectionState === 'disconnected') {
        this.peers.delete(peerId);
        this._handlePeerConnectionFailed(peerId, pc);
      }
    };

    pc.ondatachannel = (event) => {
      console.log(`[P2P] Received data channel from ${peerId}`);
      this._setupDataChannel(event.channel, peerId);
    };

    pc.onsignalingstatechange = () => {
      console.log(`[P2P] Signaling state for ${peerId}: ${pc.signalingState}`);
    };

    if (isInitiator) {
      try {
        const offer = await pc.createOffer({
          offerToReceiveAudio: false,
          offerToReceiveVideo: false,
          iceRestart: isReconnect
        });
        await pc.setLocalDescription(offer);
        this._sendSignaling({
          type: 'offer',
          from: this.nodeId,
          targetId: peerId,
          roomId: this.roomId,
          payload: offer
        });
      } catch (err) {
        console.error('[P2P] Error creating offer:', err);
        this._cleanupPeer(peerId);
        this._schedulePeerReconnect(peerId);
      }
    }
  }

  async _handleOffer(from, offer) {
    console.log(`[P2P] Received offer from ${from}`);

    let pc = this.peerConnections.get(from);
    if (!pc || pc.connectionState === 'failed' || pc.connectionState === 'disconnected') {
      if (pc) {
        this._cleanupPeer(from, false);
      }
      await this._initiateConnection(from, false);
      pc = this.peerConnections.get(from);
    }

    if (!pc) return;

    try {
      if (pc.signalingState !== 'stable' && pc.signalingState !== 'have-local-offer') {
        console.log(`[P2P] Unexpected signaling state: ${pc.signalingState}, rolling back`);
        await pc.setRemoteDescription(new RTCSessionDescription({ type: 'rollback' }));
      }

      await pc.setRemoteDescription(new RTCSessionDescription(offer));
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      this._sendSignaling({
        type: 'answer',
        from: this.nodeId,
        targetId: from,
        roomId: this.roomId,
        payload: answer
      });
    } catch (err) {
      console.error('[P2P] Error handling offer:', err);
      if (err.name === 'InvalidModificationError') {
        console.log('[P2P] Attempting to recreate peer connection');
        this._cleanupPeer(from, false);
        setTimeout(() => this._initiateConnection(from, false), 1000);
      }
    }
  }

  async _handleAnswer(from, answer) {
    console.log(`[P2P] Received answer from ${from}`);
    const pc = this.peerConnections.get(from);
    if (!pc) return;

    try {
      if (pc.signalingState === 'have-local-offer') {
        await pc.setRemoteDescription(new RTCSessionDescription(answer));
      } else {
        console.log(`[P2P] Ignoring answer in state: ${pc.signalingState}`);
      }
    } catch (err) {
      console.error('[P2P] Error handling answer:', err);
    }
  }

  async _handleIceCandidate(from, candidate) {
    console.log(`[P2P] Received ICE candidate from ${from}`);
    const pc = this.peerConnections.get(from);
    if (!pc) return;

    try {
      if (pc.remoteDescription) {
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
      } else {
        console.log(`[P2P] Buffering ICE candidate for ${from} (no remote description yet)`);
        setTimeout(async () => {
          if (pc.remoteDescription) {
            try {
              await pc.addIceCandidate(new RTCIceCandidate(candidate));
            } catch (e) {
              console.error('[P2P] Error adding buffered ICE candidate:', e);
            }
          }
        }, 1000);
      }
    } catch (err) {
      console.error('[P2P] Error adding ICE candidate:', err);
    }
  }

  async _attemptIceRestart(peerId, pc) {
    console.log(`[P2P] Attempting ICE restart for ${peerId}`);

    if (!pc || pc.connectionState === 'closed') {
      this._schedulePeerReconnect(peerId);
      return;
    }

    try {
      const offer = await pc.createOffer({ iceRestart: true });
      await pc.setLocalDescription(offer);
      this._sendSignaling({
        type: 'offer',
        from: this.nodeId,
        targetId: peerId,
        roomId: this.roomId,
        payload: offer
      });
    } catch (err) {
      console.error('[P2P] ICE restart failed:', err);
      this._schedulePeerReconnect(peerId);
    }
  }

  _handlePeerConnectionFailed(peerId, pc) {
    console.log(`[P2P] Handling peer connection failure for ${peerId}`);

    if (pc && pc.connectionState !== 'closed') {
      try {
        pc.close();
      } catch (e) {}
    }

    this.peerConnections.delete(peerId);
    this.peers.delete(peerId);

    this._schedulePeerReconnect(peerId);
  }

  _schedulePeerReconnect(peerId) {
    const attempts = this.peerReconnectAttempts.get(peerId) || 0;

    if (attempts >= this.maxPeerReconnectAttempts) {
      console.log(`[P2P] Max peer reconnect attempts reached for ${peerId}`);
      this._emit('peer-disconnected', { peerId, permanent: true });
      return;
    }

    const newAttempts = attempts + 1;
    this.peerReconnectAttempts.set(peerId, newAttempts);

    const delay = Math.min(this.peerReconnectDelay * Math.pow(1.5, newAttempts - 1), 30000);

    console.log(`[P2P] Scheduling reconnect for ${peerId} in ${delay}ms (attempt ${newAttempts}/${this.maxPeerReconnectAttempts})`);
    this._emit('peer-reconnecting', { peerId, attempt: newAttempts, delay });

    setTimeout(() => {
      if (!this.isConnected) return;
      const currentPc = this.peerConnections.get(peerId);
      if (currentPc && currentPc.connectionState === 'connected') {
        console.log(`[P2P] Already connected to ${peerId}, skipping reconnect`);
        return;
      }
      this._initiateConnection(peerId, true, true);
    }, delay);
  }

  _setupDataChannel(channel, peerId) {
    this.dataChannels.set(peerId, channel);

    channel.binaryType = 'arraybuffer';

    channel.onopen = () => {
      console.log(`[P2P] Data channel open with ${peerId}`);
      this.peers.set(peerId, true);
      this.peerReconnectAttempts.set(peerId, 0);
      this._emit('peer-connected', { peerId });
      this._flushMessageQueue(peerId);
    };

    channel.onmessage = (event) => {
      try {
        let data;
        if (typeof event.data === 'string') {
          data = JSON.parse(event.data);
        } else if (event.data instanceof ArrayBuffer) {
          data = JSON.parse(new TextDecoder().decode(event.data));
        } else {
          data = JSON.parse(event.data);
        }

        console.log(`[P2P] Received message from ${peerId}:`, data.type);
        this._emit('message', { peerId, data });
      } catch (err) {
        console.error('[P2P] Error parsing message:', err);
      }
    };

    channel.onerror = (err) => {
      console.error(`[P2P] Data channel error with ${peerId}:`, err);
    };

    channel.onclose = () => {
      console.log(`[P2P] Data channel closed with ${peerId}`);
      this.peers.delete(peerId);
      this.dataChannels.delete(peerId);
      this._emit('peer-disconnected', { peerId });
    };

    channel.onbufferedamountlow = () => {
      this._flushMessageQueue(peerId);
    };
  }

  _queueMessage(peerId, message) {
    if (!this.messageQueue.has(peerId)) {
      this.messageQueue.set(peerId, []);
    }
    this.messageQueue.get(peerId).push({
      message,
      timestamp: Date.now()
    });
    console.log(`[P2P] Queued message for ${peerId}, queue size: ${this.messageQueue.get(peerId).length}`);
  }

  _flushMessageQueue(peerId) {
    const queue = this.messageQueue.get(peerId);
    if (!queue || queue.length === 0) return;

    const dc = this.dataChannels.get(peerId);
    if (!dc || dc.readyState !== 'open') {
      console.log(`[P2P] Cannot flush queue for ${peerId}: channel not open`);
      return;
    }

    console.log(`[P2P] Flushing ${queue.length} messages for ${peerId}`);

    while (queue.length > 0 && dc.readyState === 'open') {
      if (dc.bufferedAmount > 65536) {
        console.log(`[P2P] Buffer full for ${peerId}, remaining: ${queue.length} messages`);
        break;
      }

      const item = queue.shift();
      try {
        const messageStr = JSON.stringify(item.message);
        dc.send(messageStr);
      } catch (err) {
        console.error('[P2P] Error sending queued message:', err);
        queue.unshift(item);
        break;
      }
    }
  }

  _requestSyncFromPeer(peerId) {
    const localVC = {};
    this._emit('vector-clock-update', {
      peerId,
      action: 'request',
      callback: (vc) => {
        this.sendToPeer(peerId, {
          type: 'sync-request',
          vectorClock: vc,
          fromNodeId: this.nodeId
        });
      }
    });
  }

  _cleanupPeer(peerId, emitEvent = true) {
    console.log(`[P2P] Cleaning up connection with ${peerId}`);

    const dc = this.dataChannels.get(peerId);
    if (dc) {
      try { dc.close(); } catch (e) {}
      this.dataChannels.delete(peerId);
    }

    const pc = this.peerConnections.get(peerId);
    if (pc) {
      try { pc.close(); } catch (e) {}
      this.peerConnections.delete(peerId);
    }

    this.peers.delete(peerId);
    this.peerIceGatheringStates.delete(peerId);

    if (emitEvent) {
      this._emit('peer-disconnected', { peerId });
    }
  }

  sendToPeer(peerId, message) {
    const dc = this.dataChannels.get(peerId);
    if (dc && dc.readyState === 'open') {
      try {
        const messageStr = JSON.stringify(message);
        dc.send(messageStr);
        return { success: true, buffered: false };
      } catch (err) {
        console.error('[P2P] Error sending message:', err);
        this._queueMessage(peerId, message);
        return { success: false, buffered: true, error: err };
      }
    } else {
      this._queueMessage(peerId, message);
      return { success: false, buffered: true, reason: 'channel-not-open' };
    }
  }

  broadcast(message) {
    const results = [];
    const peerIds = Array.from(new Set([
      ...this.dataChannels.keys(),
      ...this.peers.keys()
    ]));

    peerIds.forEach(peerId => {
      const result = this.sendToPeer(peerId, message);
      results.push({ peerId, ...result });
    });

    return results;
  }

  getConnectedPeers() {
    return Array.from(this.peers.keys());
  }

  getPeerCount() {
    return this.peers.size;
  }

  isPeerConnected(peerId) {
    return this.peers.has(peerId) && this._isDataChannelOpen(peerId);
  }

  _isDataChannelOpen(peerId) {
    const dc = this.dataChannels.get(peerId);
    return dc && dc.readyState === 'open';
  }

  _sendSignaling(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    }
  }

  _handleReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.log('[P2P] Max reconnect attempts reached for signaling server');
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);

    console.log(`[P2P] Reconnecting to signaling in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

    setTimeout(() => {
      this.connect().catch(err => {
        console.error('[P2P] Reconnect failed:', err);
      });
    }, delay);
  }

  setPeerVectorClock(peerId, vectorClock) {
    this.peerVectorClocks.set(peerId, vectorClock);
  }

  getPeerVectorClock(peerId) {
    return this.peerVectorClocks.get(peerId) || {};
  }

  getMessageQueueSize(peerId) {
    const queue = this.messageQueue.get(peerId);
    return queue ? queue.length : 0;
  }

  clearMessageQueue(peerId) {
    if (this.messageQueue.has(peerId)) {
      this.messageQueue.get(peerId).length = 0;
    }
  }

  disconnect() {
    console.log('[P2P] Disconnecting all peers');

    this.messageQueue.clear();
    this.peerReconnectAttempts.clear();
    this.peerIceGatheringStates.clear();
    this.peerVectorClocks.clear();

    this.dataChannels.forEach((dc, peerId) => {
      try { dc.close(); } catch (e) {}
    });

    this.peerConnections.forEach((pc, peerId) => {
      try { pc.close(); } catch (e) {}
    });

    this.dataChannels.clear();
    this.peerConnections.clear();
    this.peers.clear();

    if (this.ws) {
      try { this.ws.close(); } catch (e) {}
      this.ws = null;
    }

    this.isConnected = false;
  }

  getStatus() {
    const peers = {};

    this.peerConnections.forEach((pc, peerId) => {
      const dc = this.dataChannels.get(peerId);
      const isConnected = dc && dc.readyState === 'open';
      const reconnecting = this.peerReconnectAttempts.get(peerId) > 0 && !isConnected;

      peers[peerId] = {
        connected: isConnected,
        reconnecting: reconnecting,
        reconnectAttempts: this.peerReconnectAttempts.get(peerId) || 0,
        iceConnectionState: pc.iceConnectionState,
        dataChannelState: dc ? dc.readyState : 'closed',
        queueSize: this.messageQueue.get(peerId)?.length || 0
      };
    });

    const queueSizes = {};
    this.messageQueue.forEach((queue, peerId) => {
      queueSizes[peerId] = queue.length;
    });

    return {
      nodeId: this.nodeId,
      roomId: this.roomId,
      isSignalingConnected: this.isConnected,
      peerCount: this.getPeerCount(),
      connectedPeers: this.getConnectedPeers(),
      peers: peers,
      peerStates: Object.fromEntries(
        Array.from(this.peerConnections.entries()).map(([id, pc]) => [id, pc.connectionState])
      ),
      iceStates: Object.fromEntries(this.peerIceGatheringStates),
      reconnectAttempts: Object.fromEntries(this.peerReconnectAttempts),
      messageQueueSizes: queueSizes,
      dataChannelStates: Object.fromEntries(
        Array.from(this.dataChannels.entries()).map(([id, dc]) => [id, dc.readyState])
      )
    };
  }

  _simulateIceFailure(peerId) {
    const pc = this.peerConnections.get(peerId);
    if (!pc) {
      throw new Error(`Peer ${peerId} not found`);
    }

    console.log(`[P2PManager] Simulating ICE failure for peer ${peerId}`);
    this._emit('ice-failed', { peerId, state: 'failed' });
    this._handlePeerConnectionFailed(peerId, pc);
  }
}

window.P2PManager = P2PManager;
