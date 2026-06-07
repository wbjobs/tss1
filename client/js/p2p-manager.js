class P2PManager {
  constructor(nodeId, roomId, signalingUrl) {
    this.nodeId = nodeId;
    this.roomId = roomId;
    this.signalingUrl = signalingUrl;
    this.peers = new Map();
    this.peerConnections = new Map();
    this.dataChannels = new Map();
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.isConnected = false;

    this.listeners = {
      'peer-connected': [],
      'peer-disconnected': [],
      'message': [],
      'connected': [],
      'disconnected': [],
      'state-change': []
    };

    this.iceServers = [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' },
      { urls: 'stun:stun2.l.google.com:19302' },
      { urls: 'stun:stun3.l.google.com:19302' },
      { urls: 'stun:stun4.l.google.com:19302' }
    ];
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

  async _initiateConnection(peerId, isInitiator) {
    console.log(`[P2P] ${isInitiator ? 'Initiating' : 'Accepting'} connection with ${peerId}`);

    if (this.peerConnections.has(peerId)) {
      console.log(`[P2P] Already connected to ${peerId}`);
      return;
    }

    const pc = new RTCPeerConnection({ iceServers: this.iceServers });
    this.peerConnections.set(peerId, pc);

    const dc = pc.createDataChannel('config-sync', {
      ordered: true,
      reliable: true
    });

    this._setupDataChannel(dc, peerId);

    pc.onicecandidate = (event) => {
      if (event.candidate) {
        this._sendSignaling({
          type: 'ice-candidate',
          from: this.nodeId,
          targetId: peerId,
          roomId: this.roomId,
          payload: event.candidate
        });
      }
    };

    pc.onconnectionstatechange = () => {
      console.log(`[P2P] Connection state with ${peerId}: ${pc.connectionState}`);
      this._emit('state-change', { peerId, state: pc.connectionState });

      if (pc.connectionState === 'connected') {
        this.peers.set(peerId, true);
      } else if (pc.connectionState === 'failed' || pc.connectionState === 'disconnected') {
        this._cleanupPeer(peerId);
      }
    };

    pc.ondatachannel = (event) => {
      console.log(`[P2P] Received data channel from ${peerId}`);
      this._setupDataChannel(event.channel, peerId);
    };

    if (isInitiator) {
      try {
        const offer = await pc.createOffer();
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
      }
    }
  }

  async _handleOffer(from, offer) {
    console.log(`[P2P] Received offer from ${from}`);

    if (!this.peerConnections.has(from)) {
      await this._initiateConnection(from, false);
    }

    const pc = this.peerConnections.get(from);
    if (!pc) return;

    try {
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
    }
  }

  async _handleAnswer(from, answer) {
    console.log(`[P2P] Received answer from ${from}`);
    const pc = this.peerConnections.get(from);
    if (!pc) return;

    try {
      await pc.setRemoteDescription(new RTCSessionDescription(answer));
    } catch (err) {
      console.error('[P2P] Error handling answer:', err);
    }
  }

  async _handleIceCandidate(from, candidate) {
    console.log(`[P2P] Received ICE candidate from ${from}`);
    const pc = this.peerConnections.get(from);
    if (!pc) return;

    try {
      await pc.addIceCandidate(new RTCIceCandidate(candidate));
    } catch (err) {
      console.error('[P2P] Error adding ICE candidate:', err);
    }
  }

  _setupDataChannel(channel, peerId) {
    this.dataChannels.set(peerId, channel);

    channel.onopen = () => {
      console.log(`[P2P] Data channel open with ${peerId}`);
      this.peers.set(peerId, true);
      this._emit('peer-connected', { peerId });
    };

    channel.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
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
      this._emit('peer-disconnected', { peerId });
    };
  }

  _cleanupPeer(peerId) {
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
  }

  sendToPeer(peerId, message) {
    const dc = this.dataChannels.get(peerId);
    if (dc && dc.readyState === 'open') {
      dc.send(JSON.stringify(message));
      return true;
    }
    return false;
  }

  broadcast(message) {
    const results = [];
    this.dataChannels.forEach((dc, peerId) => {
      if (dc.readyState === 'open') {
        try {
          dc.send(JSON.stringify(message));
          results.push({ peerId, success: true });
        } catch (err) {
          results.push({ peerId, success: false, error: err });
        }
      }
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
      console.log('[P2P] Max reconnect attempts reached');
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);

    console.log(`[P2P] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts}/${this.maxReconnectAttempts})`);

    setTimeout(() => {
      this.connect().catch(err => {
        console.error('[P2P] Reconnect failed:', err);
      });
    }, delay);
  }

  disconnect() {
    console.log('[P2P] Disconnecting all peers');

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
    return {
      nodeId: this.nodeId,
      roomId: this.roomId,
      isSignalingConnected: this.isConnected,
      peerCount: this.getPeerCount(),
      peers: this.getConnectedPeers(),
      peerStates: Object.fromEntries(
        Array.from(this.peerConnections.entries()).map(([id, pc]) => [id, pc.connectionState])
      )
    };
  }
}

window.P2PManager = P2PManager;
