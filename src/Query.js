const { createReadStream } = require('fs');
const { createInterface } = require('readline');

class Query {
  constructor(data, indexManager) {
    this.data = data;
    this.indexManager = indexManager;
    this.conditions = [];
    this.sortField = null;
    this.sortOrder = 'asc';
    this.limitCount = null;
    this.skipCount = 0;
    this.selectedFields = null;
    this.table = null;
    this.indexableConditions = [];
    this.nonIndexableConditions = [];
  }

  where(condition) {
    if (typeof condition === 'function') {
      this.conditions.push(condition);
      this.nonIndexableConditions.push(condition);
    } else if (typeof condition === 'object') {
      const conditionFn = this.buildCondition(condition);
      this.conditions.push(conditionFn);
      this.analyzeCondition(condition);
    }
    return this;
  }

  analyzeCondition(obj) {
    for (const [key, value] of Object.entries(obj)) {
      if (this.indexManager.hasIndex(key)) {
        const indexType = this.indexManager.getIndexType(key);
        
        if (typeof value === 'object' && value !== null) {
          const hasRangeOp = value.$gt !== undefined || value.$gte !== undefined || 
                            value.$lt !== undefined || value.$lte !== undefined;
          
          if (indexType === 'sorted' && hasRangeOp) {
            this.indexableConditions.push({ field: key, condition: value, type: 'range' });
          } else if (value.$eq !== undefined || value.$in !== undefined) {
            this.indexableConditions.push({ field: key, condition: value, type: 'exact' });
          } else {
            this.nonIndexableConditions.push(this.buildCondition({ [key]: value }));
          }
        } else {
          this.indexableConditions.push({ field: key, condition: { $eq: value }, type: 'exact' });
        }
      } else {
        this.nonIndexableConditions.push(this.buildCondition({ [key]: value }));
      }
    }
  }

  and(condition) {
    return this.where(condition);
  }

  or(condition) {
    const prevCondition = this.conditions.pop();
    if (prevCondition) {
      const orCondition = (record) => {
        const cond = typeof condition === 'object' ? 
          this.buildCondition(condition) : condition;
        return prevCondition(record) || cond(record);
      };
      this.conditions.push(orCondition);
      this.nonIndexableConditions.push(orCondition);
    }
    return this;
  }

  buildCondition(obj) {
    return (record) => {
      for (const [key, value] of Object.entries(obj)) {
        if (typeof value === 'object' && value !== null) {
          if (!this.evaluateOperator(record[key], value)) {
            return false;
          }
        } else if (record[key] !== value) {
          return false;
        }
      }
      return true;
    };
  }

  evaluateOperator(fieldValue, operator) {
    if (operator.$eq !== undefined) return fieldValue === operator.$eq;
    if (operator.$ne !== undefined) return fieldValue !== operator.$ne;
    if (operator.$gt !== undefined) return fieldValue > operator.$gt;
    if (operator.$gte !== undefined) return fieldValue >= operator.$gte;
    if (operator.$lt !== undefined) return fieldValue < operator.$lt;
    if (operator.$lte !== undefined) return fieldValue <= operator.$lte;
    if (operator.$in !== undefined) return operator.$in.includes(fieldValue);
    if (operator.$nin !== undefined) return !operator.$nin.includes(fieldValue);
    if (operator.$regex !== undefined) {
      const regex = new RegExp(operator.$regex, operator.$options || '');
      return regex.test(fieldValue);
    }
    if (operator.$exists !== undefined) {
      return (fieldValue !== undefined) === operator.$exists;
    }
    return true;
  }

  sort(field, order = 'asc') {
    this.sortField = field;
    this.sortOrder = order.toLowerCase();
    return this;
  }

  limit(count) {
    this.limitCount = count;
    return this;
  }

  skip(count) {
    this.skipCount = count;
    return this;
  }

  select(fields) {
    this.selectedFields = Array.isArray(fields) ? fields : [fields];
    return this;
  }

  getIndexCandidateSet() {
    if (this.indexableConditions.length === 0) return null;
    
    let candidateSet = null;
    let smallestSet = Infinity;
    
    for (const { field, condition, type } of this.indexableConditions) {
      let ids = new Set();
      
      if (type === 'range') {
        const min = condition.$gt !== undefined ? condition.$gt : condition.$gte;
        const max = condition.$lt !== undefined ? condition.$lt : condition.$lte;
        const includeMin = condition.$gte !== undefined;
        const includeMax = condition.$lte !== undefined;
        
        const rangeIds = this.indexManager.findByRange(field, min, max, includeMin, includeMax);
        if (rangeIds) ids = rangeIds;
      } else if (condition.$eq !== undefined) {
        const indexIds = this.indexManager.findByIndex(field, condition.$eq);
        if (indexIds) ids = new Set(indexIds);
      } else if (condition.$in !== undefined) {
        for (const value of condition.$in) {
          const indexIds = this.indexManager.findByIndex(field, value);
          if (indexIds) {
            for (const id of indexIds) ids.add(id);
          }
        }
      }
      
      if (candidateSet === null) {
        candidateSet = ids;
        smallestSet = ids.size;
      } else {
        const intersection = new Set();
        for (const id of ids) {
          if (candidateSet.has(id)) intersection.add(id);
        }
        candidateSet = intersection;
        if (candidateSet.size === 0) break;
      }
    }
    
    return candidateSet;
  }

  async execute() {
    let results = [];
    
    const candidateSet = this.getIndexCandidateSet();
    
    if (candidateSet !== null) {
      if (candidateSet.size === 0) {
        return [];
      }
      results = await this.executeWithIndex(candidateSet);
    } else if (this.table && (this.table.data.size < this.table.metadata.recordCount)) {
      results = await this.streamResults();
    } else {
      const pendingUpdates = this.table ? this.table.pendingUpdates : new Map();
      const pendingDeletes = this.table ? this.table.pendingDeletes : new Set();
      
      for (const [id, record] of this.data) {
        if (!pendingDeletes.has(id)) {
          const actualRecord = pendingUpdates.get(id) || record;
          results.push(actualRecord);
        }
      }
      
      if (this.table && this.table.pendingWrites.length > 0) {
        for (const record of this.table.pendingWrites) {
          if (!pendingDeletes.has(record._id)) {
            results.push(record);
          }
        }
      }
    }
    
    for (const condition of this.nonIndexableConditions) {
      results = results.filter(condition);
    }
    
    if (this.sortField) {
      this.sortResults(results);
    }
    
    if (this.skipCount > 0) {
      results = results.slice(this.skipCount);
    }
    
    if (this.limitCount !== null) {
      results = results.slice(0, this.limitCount);
    }
    
    if (this.selectedFields) {
      results = this.projectFields(results);
    }
    
    return results;
  }

  async executeWithIndex(candidateSet) {
    const results = [];
    const pendingUpdates = this.table ? this.table.pendingUpdates : new Map();
    const pendingDeletes = this.table ? this.table.pendingDeletes : new Set();
    
    for (const id of candidateSet) {
      if (pendingDeletes.has(id)) continue;
      
      let record = pendingUpdates.get(id) || this.data.get(id);
      if (!record && this.table) {
        record = await this.table.loadRecordById(id);
      }
      if (record) {
        results.push(record);
      }
    }
    
    return results;
  }

  sortResults(results) {
    if (this.limitCount && this.skipCount === 0 && this.limitCount < results.length / 10) {
      this.heapSort(results, this.limitCount);
    } else {
      results.sort((a, b) => {
        const aVal = a[this.sortField];
        const bVal = b[this.sortField];
        
        if (aVal === bVal) return 0;
        
        const comparison = aVal < bVal ? -1 : 1;
        return this.sortOrder === 'asc' ? comparison : -comparison;
      });
    }
  }

  heapSort(arr, k) {
    const compare = (a, b) => {
      const aVal = a[this.sortField];
      const bVal = b[this.sortField];
      
      if (aVal === bVal) return 0;
      
      const comparison = aVal < bVal ? -1 : 1;
      return this.sortOrder === 'desc' ? comparison : -comparison;
    };
    
    const heap = [];
    
    for (let i = 0; i < arr.length; i++) {
      if (heap.length < k) {
        heap.push(arr[i]);
        this.bubbleUp(heap, heap.length - 1, compare);
      } else if (compare(arr[i], heap[0]) < 0) {
        heap[0] = arr[i];
        this.bubbleDown(heap, 0, compare);
      }
    }
    
    heap.sort((a, b) => -compare(a, b));
    
    for (let i = 0; i < k && i < heap.length; i++) {
      arr[i] = heap[i];
    }
  }

  bubbleUp(heap, index, compare) {
    while (index > 0) {
      const parentIndex = Math.floor((index - 1) / 2);
      if (compare(heap[index], heap[parentIndex]) >= 0) break;
      [heap[index], heap[parentIndex]] = [heap[parentIndex], heap[index]];
      index = parentIndex;
    }
  }

  bubbleDown(heap, index, compare) {
    const length = heap.length;
    
    while (true) {
      let smallest = index;
      const left = 2 * index + 1;
      const right = 2 * index + 2;
      
      if (left < length && compare(heap[left], heap[smallest]) < 0) {
        smallest = left;
      }
      
      if (right < length && compare(heap[right], heap[smallest]) < 0) {
        smallest = right;
      }
      
      if (smallest === index) break;
      
      [heap[index], heap[smallest]] = [heap[smallest], heap[index]];
      index = smallest;
    }
  }

  projectFields(results) {
    return results.map(record => {
      const selected = {};
      for (const field of this.selectedFields) {
        if (field in record) {
          selected[field] = record[field];
        }
      }
      return selected;
    });
  }

  async streamResults() {
    const results = [];
    let processedCount = 0;
    const maxToProcess = this.limitCount ? 
      (this.limitCount + this.skipCount) * 2 : Infinity;
    
    const pendingUpdates = this.table ? this.table.pendingUpdates : new Map();
    const pendingDeletes = this.table ? this.table.pendingDeletes : new Set();
    
    try {
      const fileStream = createReadStream(this.table.filePath);
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim() && processedCount < maxToProcess) {
          try {
            const record = JSON.parse(line);
            if (!pendingDeletes.has(record._id)) {
              const actualRecord = pendingUpdates.get(record._id) || record;
              results.push(actualRecord);
              processedCount++;
            }
          } catch (parseError) {
            continue;
          }
        }
        if (processedCount >= maxToProcess) break;
      }
      
      rl.close();
      fileStream.destroy();
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
    
    if (this.table && this.table.pendingWrites.length > 0) {
      for (const record of this.table.pendingWrites) {
        if (!pendingDeletes.has(record._id)) {
          results.push(record);
        }
      }
    }
    
    return results;
  }

  async first() {
    this.limitCount = 1;
    const results = await this.execute();
    return results.length > 0 ? results[0] : null;
  }

  async count() {
    const candidateSet = this.getIndexCandidateSet();
    
    if (candidateSet !== null && this.nonIndexableConditions.length === 0) {
      if (this.table) {
        let count = 0;
        for (const id of candidateSet) {
          if (!this.table.pendingDeletes.has(id)) {
            count++;
          }
        }
        return count;
      }
      return candidateSet.size;
    }
    
    const results = await this.execute();
    return results.length;
  }

  async distinct(field) {
    const results = await this.execute();
    const values = new Set();
    for (const record of results) {
      const value = this.indexManager.getFieldValue(record, field);
      if (value !== undefined) {
        values.add(value);
      }
    }
    return Array.from(values);
  }

  async aggregate(operations) {
    const results = await this.execute();
    const aggregated = {};
    
    for (const [key, operation] of Object.entries(operations)) {
      if (operation.$sum) {
        aggregated[key] = results.reduce((sum, r) => sum + (r[operation.$sum] || 0), 0);
      } else if (operation.$avg) {
        const sum = results.reduce((s, r) => s + (r[operation.$avg] || 0), 0);
        aggregated[key] = results.length > 0 ? sum / results.length : 0;
      } else if (operation.$min) {
        aggregated[key] = Math.min(...results.map(r => r[operation.$min] || Infinity));
      } else if (operation.$max) {
        aggregated[key] = Math.max(...results.map(r => r[operation.$max] || -Infinity));
      } else if (operation.$count) {
        aggregated[key] = results.length;
      }
    }
    
    return aggregated;
  }
}

module.exports = { Query };