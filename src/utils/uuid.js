/**
 * Generate a random UUID v4 string
 * @returns {string} A UUID v4 string in the format xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
 */
function generateUUID() {
  // Use crypto.randomUUID if available (Node.js 14.17.0+)
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  
  // Fallback implementation for older Node.js versions or browsers
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