const nodeCrypto = require('crypto');

/**
 * Generate a random UUID v4 string
 * @returns {string} A UUID v4 string in the format xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
 */
function generateUUID() {
  // Use crypto.randomUUID if available (Node.js 14.17.0+)
  if (nodeCrypto && typeof nodeCrypto.randomUUID === 'function') {
    return nodeCrypto.randomUUID();
  }

  // Fallback using crypto.randomBytes for older Node.js versions
  if (nodeCrypto && typeof nodeCrypto.randomBytes === 'function') {
    const bytes = nodeCrypto.randomBytes(16);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = bytes.toString('hex');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
  }

  // Last resort fallback
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * Generate a random UUID v4 string (alias for compatibility)
 * @returns {string} A UUID v4 string
 */
function uuidv4() {
  return generateUUID();
}

module.exports = {
  generateUUID,
  uuidv4,
  // Export as default for convenience
  v4: generateUUID
};