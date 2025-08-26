const fs = require('fs').promises;
const path = require('path');
const { Query } = require('./Query');
const { IndexManager } = require('./IndexManager');
const { SimpleCache } = require('./SimpleCache');
const { v4: uuidv4 } = require('./utils/uuid');
const { EventEmitter } = require('events');

class Table extends EventEmitter {
  constructor(name, db) {
    super();
    this.name = name;
    this.db = db;
    this.filePath = path.join(db.dbPath, `${name}.json`);
    this.data = new Map();
    this.cache = new SimpleCache(db.options.cacheSize);
    this.indexManager = new IndexManager();
    this.writeQueue = Promise.resolve();
    this.isDirty = false;
    this.autoIncrement = 1;
  }

  async load() {
    try {
      const content = await fs.readFile(this.filePath, 'utf-8');
      const parsed = JSON.parse(content);
      
      if (parsed.data) {
        for (const [id, record] of Object.entries(parsed.data)) {
          this.data.set(id, record);
        }
        this.autoIncrement = parsed.autoIncrement || 1;
      }
      
      if (parsed.indices) {
        for (const field of parsed.indices) {
          this.indexManager.createIndex(field, this.data);
        }
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
  }

  async save() {
    const dataObj = {};
    for (const [id, record] of this.data.entries()) {
      dataObj[id] = record;
    }
    
    const saveData = {
      data: dataObj,
      indices: Array.from(this.indexManager.indices.keys()),
      autoIncrement: this.autoIncrement,
      modified: new Date().toISOString()
    };
    
    const tempPath = `${this.filePath}.tmp`;
    await fs.writeFile(tempPath, JSON.stringify(saveData, null, 2));
    await fs.rename(tempPath, this.filePath);
    this.isDirty = false;
    this.emit('save', this.name);
  }

  queueSave() {
    if (!this.db.options.autoSave) return;
    this.isDirty = true;
    this.writeQueue = this.writeQueue.then(() => this.save());
  }

  insert(record) {
    const id = record.id || record._id || uuidv4();
    const timestamp = new Date().toISOString();
    
    const fullRecord = {
      ...record,
      _id: id,
      _created: timestamp,
      _modified: timestamp
    };
    
    this.data.set(id, fullRecord);
    this.cache.set(id, fullRecord);
    this.indexManager.updateIndices(id, fullRecord);
    this.queueSave();
    this.emit('insert', fullRecord);
    
    return fullRecord;
  }

  insertMany(records) {
    const inserted = [];
    for (const record of records) {
      inserted.push(this.insert(record));
    }
    return inserted;
  }

  get(id) {
    if (this.cache.has(id)) {
      return this.cache.get(id);
    }
    
    const record = this.data.get(id);
    if (record) {
      this.cache.set(id, record);
    }
    return record;
  }

  update(id, updates) {
    const record = this.get(id);
    if (!record) return null;
    
    const updated = {
      ...record,
      ...updates,
      _id: id,
      _created: record._created,
      _modified: new Date().toISOString()
    };
    
    this.data.set(id, updated);
    this.cache.set(id, updated);
    this.indexManager.updateIndices(id, updated);
    this.queueSave();
    this.emit('update', updated);
    
    return updated;
  }

  updateMany(condition, updates) {
    const query = new Query(this.data, this.indexManager);
    const records = query.where(condition).execute();
    const updated = [];
    
    for (const record of records) {
      updated.push(this.update(record._id, updates));
    }
    
    return updated;
  }

  delete(id) {
    const record = this.get(id);
    if (!record) return false;
    
    this.data.delete(id);
    this.cache.delete(id);
    this.indexManager.removeFromIndices(id);
    this.queueSave();
    this.emit('delete', record);
    
    return true;
  }

  deleteMany(condition) {
    const query = new Query(this.data, this.indexManager);
    const records = query.where(condition).execute();
    const deleted = [];
    
    for (const record of records) {
      if (this.delete(record._id)) {
        deleted.push(record);
      }
    }
    
    return deleted;
  }

  find(condition) {
    return new Query(this.data, this.indexManager).where(condition);
  }

  findOne(condition) {
    return new Query(this.data, this.indexManager).where(condition).first();
  }

  findById(id) {
    return this.get(id);
  }

  all() {
    return new Query(this.data, this.indexManager);
  }

  count(condition) {
    if (!condition) {
      return this.data.size;
    }
    return new Query(this.data, this.indexManager).where(condition).count();
  }

  createIndex(field) {
    this.indexManager.createIndex(field, this.data);
    this.queueSave();
    return this;
  }

  dropIndex(field) {
    this.indexManager.dropIndex(field);
    this.queueSave();
    return this;
  }

  async flush() {
    await this.writeQueue;
  }

  async drop() {
    await fs.unlink(this.filePath).catch(() => {});
    this.data.clear();
    this.cache.clear();
    this.emit('drop');
  }

  async truncate() {
    this.data.clear();
    this.cache.clear();
    this.indexManager.clear();
    this.queueSave();
    this.emit('truncate');
  }
}

module.exports = { Table };