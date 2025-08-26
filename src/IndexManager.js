class IndexManager {
  constructor() {
    this.indices = new Map();
  }

  createIndex(field, data) {
    const index = new Map();

    for (const [id, record] of data.entries()) {
      const value = this.getFieldValue(record, field);
      if (value !== undefined) {
        if (!index.has(value)) {
          index.set(value, new Set());
        }
        index.get(value).add(id);
      }
    }

    this.indices.set(field, index);
    return this;
  }

  dropIndex(field) {
    this.indices.delete(field);
    return this;
  }

  updateIndices(id, record) {
    for (const [field, index] of this.indices.entries()) {
      // Remove old entry
      for (const valueSet of index.values()) {
        valueSet.delete(id);
      }

      // Add new entry
      const value = this.getFieldValue(record, field);
      if (value !== undefined) {
        if (!index.has(value)) {
          index.set(value, new Set());
        }
        index.get(value).add(id);
      }
    }
  }

  removeFromIndices(id) {
    for (const index of this.indices.values()) {
      for (const valueSet of index.values()) {
        valueSet.delete(id);
      }
    }
  }

  findByIndex(field, value) {
    const index = this.indices.get(field);
    if (!index) return null;

    return index.get(value) || new Set();
  }

  getFieldValue(record, field) {
    // Support nested fields with dot notation
    const parts = field.split('.');
    let value = record;

    for (const part of parts) {
      if (value && typeof value === 'object') {
        value = value[part];
      } else {
        return undefined;
      }
    }

    return value;
  }

  clear() {
    this.indices.clear();
  }

  hasIndex(field) {
    return this.indices.has(field);
  }

  getIndices() {
    return Array.from(this.indices.keys());
  }
}

module.exports = { IndexManager };