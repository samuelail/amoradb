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
  }

  where(condition) {
    if (typeof condition === 'function') {
      this.conditions.push(condition);
    } else if (typeof condition === 'object') {
      this.conditions.push(this.buildCondition(condition));
    }
    return this;
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

  async execute() {
    let results = [];
    
    if (this.table && (this.table.data.size < this.table.metadata.recordCount)) {
      results = await this.streamResults();
    } else {
      results = Array.from(this.data.values());
    }
    
    for (const condition of this.conditions) {
      results = results.filter(condition);
    }
    
    if (this.sortField) {
      results.sort((a, b) => {
        const aVal = a[this.sortField];
        const bVal = b[this.sortField];
        
        if (aVal === bVal) return 0;
        
        const comparison = aVal < bVal ? -1 : 1;
        return this.sortOrder === 'asc' ? comparison : -comparison;
      });
    }
    
    if (this.skipCount > 0) {
      results = results.slice(this.skipCount);
    }
    
    if (this.limitCount !== null) {
      results = results.slice(0, this.limitCount);
    }
    
    if (this.selectedFields) {
      results = results.map(record => {
        const selected = {};
        for (const field of this.selectedFields) {
          if (field in record) {
            selected[field] = record[field];
          }
        }
        return selected;
      });
    }
    
    return results;
  }

  async streamResults() {
    const results = [];
    try {
      const fileStream = createReadStream(this.table.filePath);
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim()) {
          try {
            const record = JSON.parse(line);
            results.push(record);
          } catch (parseError) {
            continue;
          }
        }
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
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
    const results = await this.execute();
    return results.length;
  }

  async distinct(field) {
    const results = await this.execute();
    const values = new Set(results.map(r => r[field]));
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