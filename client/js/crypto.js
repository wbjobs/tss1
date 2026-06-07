class CryptoManager {
  constructor() {
    this.encryptionKeys = new Map();
    this.encryptedPaths = new Set();
    this.keyDerivationSalt = 'p2p-config-sync-salt-v1';
  }

  async _deriveKey(password, salt) {
    const encoder = new TextEncoder();
    const passwordKey = await crypto.subtle.importKey(
      'raw',
      encoder.encode(password),
      { name: 'PBKDF2' },
      false,
      ['deriveKey']
    );

    const saltBuffer = encoder.encode(salt || this.keyDerivationSalt);
    return crypto.subtle.deriveKey(
      {
        name: 'PBKDF2',
        salt: saltBuffer,
        iterations: 100000,
        hash: 'SHA-256'
      },
      passwordKey,
      { name: 'AES-GCM', length: 256 },
      false,
      ['encrypt', 'decrypt']
    );
  }

  async setPasswordForPath(path, password) {
    const key = await this._deriveKey(password, path);
    this.encryptionKeys.set(path, key);
    this.encryptedPaths.add(path);
    return true;
  }

  removePasswordForPath(path) {
    this.encryptionKeys.delete(path);
    this.encryptedPaths.delete(path);
  }

  hasKeyForPath(path) {
    return this.encryptionKeys.has(path);
  }

  isPathEncrypted(path) {
    for (const encryptedPath of this.encryptedPaths) {
      if (path === encryptedPath || path.startsWith(encryptedPath + '.')) {
        return true;
      }
    }
    return false;
  }

  getEncryptedParentPath(path) {
    let bestMatch = null;
    for (const encryptedPath of this.encryptedPaths) {
      if (path === encryptedPath || path.startsWith(encryptedPath + '.')) {
        if (!bestMatch || encryptedPath.length > bestMatch.length) {
          bestMatch = encryptedPath;
        }
      }
    }
    return bestMatch;
  }

  async encrypt(data, path) {
    const encryptedPath = this.getEncryptedParentPath(path);
    if (!encryptedPath) {
      return { encrypted: false, data };
    }

    const key = this.encryptionKeys.get(encryptedPath);
    if (!key) {
      throw new Error(`No encryption key available for path: ${encryptedPath}`);
    }

    const encoder = new TextEncoder();
    const dataStr = JSON.stringify(data);
    const dataBuffer = encoder.encode(dataStr);

    const iv = crypto.getRandomValues(new Uint8Array(12));

    const encryptedBuffer = await crypto.subtle.encrypt(
      { name: 'AES-GCM', iv },
      key,
      dataBuffer
    );

    const combined = new Uint8Array(iv.length + encryptedBuffer.byteLength);
    combined.set(iv, 0);
    combined.set(new Uint8Array(encryptedBuffer), iv.length);

    const base64 = btoa(String.fromCharCode.apply(null, combined));

    return {
      encrypted: true,
      encryptedPath,
      data: base64,
      iv: btoa(String.fromCharCode.apply(null, iv))
    };
  }

  async decrypt(encryptedData, path) {
    if (!encryptedData || !encryptedData.encrypted) {
      return encryptedData ? encryptedData.data : null;
    }

    const keyPath = encryptedData.encryptedPath || path;
    const key = this.encryptionKeys.get(keyPath);
    if (!key) {
      throw new Error(`No decryption key available for path: ${keyPath}. Please provide the password.`);
    }

    try {
      const combined = Uint8Array.from(atob(encryptedData.data), c => c.charCodeAt(0));
      const iv = combined.slice(0, 12);
      const ciphertext = combined.slice(12);

      const decryptedBuffer = await crypto.subtle.decrypt(
        { name: 'AES-GCM', iv },
        key,
        ciphertext
      );

      const decoder = new TextDecoder();
      const decryptedStr = decoder.decode(decryptedBuffer);
      return JSON.parse(decryptedStr);
    } catch (e) {
      throw new Error(`Decryption failed for path ${path}: ${e.message}. Wrong password or corrupted data.`);
    }
  }

  async encryptOperation(operation) {
    if (!operation || !operation.path) {
      return operation;
    }

    if (this.isPathEncrypted(operation.path)) {
      try {
        const encrypted = await this.encrypt(operation.value, operation.path);
        if (encrypted.encrypted) {
          return {
            ...operation,
            value: encrypted,
            isEncrypted: true,
            encryptedPath: encrypted.encryptedPath
          };
        }
      } catch (e) {
        console.warn(`Failed to encrypt operation for ${operation.path}:`, e);
      }
    }
    return operation;
  }

  async decryptOperation(operation) {
    if (!operation || !operation.isEncrypted) {
      return operation;
    }

    try {
      const decryptedValue = await this.decrypt(operation.value, operation.path);
      return {
        ...operation,
        value: decryptedValue,
        isEncrypted: false,
        originalEncryptedValue: operation.value
      };
    } catch (e) {
      console.warn(`Failed to decrypt operation for ${operation.path}:`, e);
      return {
        ...operation,
        decryptionError: e.message,
        value: operation.value
      };
    }
  }

  async encryptState(state, paths) {
    const encryptedState = { ...state };
    const encryptedFields = [];

    for (const path of paths) {
      if (this.isPathEncrypted(path)) {
        const value = this._getByPath(encryptedState, path);
        if (value !== undefined) {
          try {
            const encrypted = await this.encrypt(value, path);
            if (encrypted.encrypted) {
              this._setByPath(encryptedState, path, encrypted);
              encryptedFields.push(path);
            }
          } catch (e) {
            console.warn(`Failed to encrypt state at ${path}:`, e);
          }
        }
      }
    }

    return { state: encryptedState, encryptedFields };
  }

  async decryptState(state, paths) {
    const decryptedState = JSON.parse(JSON.stringify(state));
    const errors = [];

    for (const path of paths) {
      const value = this._getByPath(decryptedState, path);
      if (value && value.encrypted) {
        try {
          const decrypted = await this.decrypt(value, path);
          this._setByPath(decryptedState, path, decrypted);
        } catch (e) {
          errors.push({ path, error: e.message });
        }
      }
    }

    return { state: decryptedState, errors };
  }

  _parsePath(path) {
    return path.split('.');
  }

  _getByPath(obj, path) {
    const parts = this._parsePath(path);
    let current = obj;
    for (const part of parts) {
      if (current === undefined || current === null) return undefined;
      current = current[part];
    }
    return current;
  }

  _setByPath(obj, path, value) {
    const parts = this._parsePath(path);
    let current = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (current[parts[i]] === undefined) {
        current[parts[i]] = {};
      }
      current = current[parts[i]];
    }
    current[parts[parts.length - 1]] = value;
  }

  exportEncryptedPaths() {
    return Array.from(this.encryptedPaths);
  }

  importEncryptedPaths(paths) {
    paths.forEach(p => this.encryptedPaths.add(p));
  }

  clearAllKeys() {
    this.encryptionKeys.clear();
    this.encryptedPaths.clear();
  }
}

class CryptoUIHelper {
  static async promptPassword(path, purpose = 'encrypt') {
    const action = purpose === 'encrypt' ? '设置' : '输入解密';
    const password = prompt(`请为配置路径 "${path}" ${action}密码：`);
    if (!password) return null;

    if (purpose === 'encrypt') {
      const confirm = prompt(`请再次输入密码以确认：`);
      if (password !== confirm) {
        alert('两次输入的密码不一致！');
        return null;
      }
    }

    return password;
  }

  static showDecryptionError(path, error) {
    alert(`无法解密配置 "${path}"：\n${error}\n\n请先设置正确的密码。`);
  }

  static showEncryptionSuccess(path) {
    alert(`配置路径 "${path}" 已成功加密！\n其他节点需要输入密码才能查看该内容。`);
  }
}

window.CryptoManager = CryptoManager;
window.CryptoUIHelper = CryptoUIHelper;
