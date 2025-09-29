class IndexManager {
  constructor() {
    this.indices = new Map();
    this.oldValues = new Map();
  }

  createIndex(field, data) {
    const index = new Map();
    const isRangeField = this.isNumericOrDateField(data, field);
    
    if (isRangeField) {
      const sortedIndex = {
        type: 'sorted',
        values: [],
        map: new Map()
      };
      
      for (const [id, record] of data.entries()) {
        const value = this.getFieldValue(record, field);
        if (value !== undefined) {
          sortedIndex.values.push({ value, id });
          sortedIndex.map.set(id, value);
        }
      }
      
      sortedIndex.values.sort((a, b) => {
        if (a.value < b.value) return -1;
        if (a.value > b.value) return 1;
        return 0;
      });
      
      this.indices.set(field, sortedIndex);
    } else {
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
    }
    
    return this;
  }

  isNumericOrDateField(data, field) {
    if (data.size === 0) return false;
    
    let samples = 0;
    let numericCount = 0;
    
    for (const record of data.values()) {
      const value = this.getFieldValue(record, field);
      if (value !== undefined) {
        if (typeof value === 'number' || !isNaN(Date.parse(value))) {
          numericCount++;
        }
        samples++;
        if (samples >= 10) break;
      }
    }
    
    return samples > 0 && numericCount === samples;
  }

  dropIndex(field) {
    this.indices.delete(field);
    return this;
  }

  updateIndices(id, oldRecord, newRecord) {
    for (const [field, index] of this.indices.entries()) {
      if (index.type === 'sorted') {
        this.updateSortedIndex(field, index, id, oldRecord, newRecord);
      } else {
        this.updateHashIndex(field, index, id, oldRecord, newRecord);
      }
    }
  }

  updateHashIndex(field, index, id, oldRecord, newRecord) {
    if (oldRecord) {
      const oldValue = this.getFieldValue(oldRecord, field);
      if (oldValue !== undefined && index.has(oldValue)) {
        index.get(oldValue).delete(id);
        if (index.get(oldValue).size === 0) {
          index.delete(oldValue);
        }
      }
    }
    
    if (newRecord) {
      const newValue = this.getFieldValue(newRecord, field);
      if (newValue !== undefined) {
        if (!index.has(newValue)) {
          index.set(newValue, new Set());
        }
        index.get(newValue).add(id);
      }
    }
  }

  updateSortedIndex(field, index, id, oldRecord, newRecord) {
    if (oldRecord || index.map.has(id)) {
      const oldValue = oldRecord ? this.getFieldValue(oldRecord, field) : index.map.get(id);
      if (oldValue !== undefined) {
        const idx = this.binarySearch(index.values, oldValue, id);
        if (idx >= 0) {
          index.values.splice(idx, 1);
        }
        index.map.delete(id);
      }
    }
    
    if (newRecord) {
      const newValue = this.getFieldValue(newRecord, field);
      if (newValue !== undefined) {
        const insertIdx = this.findInsertPosition(index.values, newValue);
        index.values.splice(insertIdx, 0, { value: newValue, id });
        index.map.set(id, newValue);
      }
    }
  }

  binarySearch(arr, value, id) {
    let left = 0;
    let right = arr.length - 1;
    
    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (arr[mid].value === value && arr[mid].id === id) {
        return mid;
      }
      if (arr[mid].value < value || (arr[mid].value === value && arr[mid].id < id)) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
    
    return -1;
  }

  findInsertPosition(arr, value) {
    let left = 0;
    let right = arr.length;
    
    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      if (arr[mid].value <= value) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    
    return left;
  }

  removeFromIndices(id, record) {
    for (const [field, index] of this.indices.entries()) {
      if (index.type === 'sorted') {
        this.updateSortedIndex(field, index, id, record, null);
      } else {
        this.updateHashIndex(field, index, id, record, null);
      }
    }
  }

  findByIndex(field, value) {
    const index = this.indices.get(field);
    if (!index) return null;
    
    if (index.type === 'sorted') {
      const results = new Set();
      for (const item of index.values) {
        if (item.value === value) {
          results.add(item.id);
        } else if (item.value > value) {
          break;
        }
      }
      return results;
    }
    
    return index.get(value) || new Set();
  }

  findByRange(field, min, max, includeMin = true, includeMax = true) {
    const index = this.indices.get(field);
    if (!index || index.type !== 'sorted') return null;
    
    const results = new Set();
    
    for (const item of index.values) {
      const value = item.value;
      
      if (min !== undefined) {
        if (includeMin ? value < min : value <= min) continue;
      }
      
      if (max !== undefined) {
        if (includeMax ? value > max : value >= max) break;
      }
      
      results.add(item.id);
    }
    
    return results;
  }

  getFieldValue(record, field) {
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
    this.oldValues.clear();
  }

  hasIndex(field) {
    return this.indices.has(field);
  }

  getIndices() {
    return Array.from(this.indices.keys());
  }

  getIndexType(field) {
    const index = this.indices.get(field);
    return index ? index.type || 'hash' : null;
  }
}

module.exports = { IndexManager };