const fs = require('fs').promises;
const { createReadStream, createWriteStream } = require('fs');
const { createInterface } = require('readline');
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
    this.filePath = path.join(db.dbPath, `${name}.jsonl`);
    this.metaPath = path.join(db.dbPath, `${name}.meta.json`);
    this.tempPath = path.join(db.dbPath, `${name}.jsonl.tmp`);
    this.data = new Map();
    this.cache = new SimpleCache(db.options.cacheSize);
    this.indexManager = new IndexManager();
    this.writeQueue = Promise.resolve();
    this.isDirty = false;
    this.autoIncrement = 1;
    this.isLoaded = false;
    this.pendingWrites = [];
    this.pendingUpdates = new Map();
    this.pendingDeletes = new Set();
    this.batchSize = 1000;
    this.deletedIds = new Set();
    this.metadata = {
      autoIncrement: 1,
      indices: [],
      recordCount: 0,
      modified: null
    };
    this.compactionThreshold = 0.3;
    this.lastCompaction = Date.now();
    this.compactionInterval = 60000;
  }

  async load() {
    try {
      await this.loadMetadata();
      
      if (this.metadata.recordCount <= this.db.options.cacheSize) {
        await this.loadAllRecords();
      } else {
        await this.buildIndicesFromFile();
      }
      
      this.isLoaded = true;
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
      this.isLoaded = true;
    }
  }

  async loadMetadata() {
    try {
      const content = await fs.readFile(this.metaPath, 'utf-8');
      this.metadata = JSON.parse(content);
      this.autoIncrement = this.metadata.autoIncrement || 1;
      
      for (const field of this.metadata.indices) {
        this.indexManager.createIndex(field, new Map());
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
  }

  async saveMetadata() {
    this.metadata.modified = new Date().toISOString();
    this.metadata.autoIncrement = this.autoIncrement;
    this.metadata.indices = Array.from(this.indexManager.indices.keys());
    this.metadata.recordCount = this.data.size;
    
    await fs.writeFile(this.metaPath, JSON.stringify(this.metadata, null, 2));
  }

  async loadAllRecords() {
    try {
      const fileStream = createReadStream(this.filePath);
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim()) {
          try {
            const record = JSON.parse(line);
            if (!this.deletedIds.has(record._id)) {
              this.data.set(record._id, record);
              this.indexManager.updateIndices(record._id, null, record);
            }
          } catch (parseError) {
          }
        }
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
  }

  async buildIndicesFromFile() {
    try {
      const fileStream = createReadStream(this.filePath);
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim()) {
          try {
            const record = JSON.parse(line);
            if (!this.deletedIds.has(record._id)) {
              this.indexManager.updateIndices(record._id, null, record);
            }
          } catch (parseError) {
          }
        }
      }
    } catch (error) {
      if (error.code !== 'ENOENT') {
        throw error;
      }
    }
  }

  async loadRecordById(id) {
    if (this.deletedIds.has(id) || this.pendingDeletes.has(id)) return null;
    
    if (this.pendingUpdates.has(id)) {
      return this.pendingUpdates.get(id);
    }
    
    if (this.data.has(id)) {
      return this.data.get(id);
    }

    if (this.cache.has(id)) {
      return this.cache.get(id);
    }

    try {
      const fileStream = createReadStream(this.filePath);
      const rl = createInterface({
        input: fileStream,
        crlfDelay: Infinity
      });

      for await (const line of rl) {
        if (line.trim()) {
          try {
            const record = JSON.parse(line);
            if (record._id === id && !this.deletedIds.has(id)) {
              this.cache.set(id, record);
              rl.close();
              fileStream.destroy();
              return record;
            }
          } catch (parseError) {
            continue;
          }
        }
      }
    } catch (error) {
    }

    return null;
  }

  needsCompaction() {
    const totalOps = this.pendingUpdates.size + this.pendingDeletes.size + this.deletedIds.size;
    const timeSinceCompaction = Date.now() - this.lastCompaction;
    
    return (totalOps > this.metadata.recordCount * this.compactionThreshold) ||
           (totalOps > 10000 && timeSinceCompaction > this.compactionInterval);
  }

  async save() {
    await this.flushPendingWrites();
    
    if (!this.needsCompaction() && this.pendingUpdates.size === 0 && this.pendingDeletes.size === 0) {
      return;
    }
    
    const writeStream = createWriteStream(this.tempPath);
    
    try {
      const processedIds = new Set();
      
      if (await fs.stat(this.filePath).catch(() => null)) {
        const fileStream = createReadStream(this.filePath);
        const rl = createInterface({
          input: fileStream,
          crlfDelay: Infinity
        });

        for await (const line of rl) {
          if (line.trim()) {
            try {
              const record = JSON.parse(line);
              const id = record._id;
              
              if (!processedIds.has(id)) {
                processedIds.add(id);
                
                if (this.pendingDeletes.has(id) || this.deletedIds.has(id)) {
                  continue;
                }
                
                const updatedRecord = this.pendingUpdates.get(id) || record;
                writeStream.write(JSON.stringify(updatedRecord) + '\n');
              }
            } catch (parseError) {
              continue;
            }
          }
        }
      }
      
      for (const [id, record] of this.data.entries()) {
        if (!processedIds.has(id) && !this.pendingDeletes.has(id) && !this.deletedIds.has(id)) {
          const updatedRecord = this.pendingUpdates.get(id) || record;
          writeStream.write(JSON.stringify(updatedRecord) + '\n');
        }
      }
      
      await new Promise((resolve, reject) => {
        writeStream.end((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
      
      await fs.rename(this.tempPath, this.filePath);
      
      for (const [id, record] of this.pendingUpdates) {
        this.data.set(id, record);
      }
      for (const id of this.pendingDeletes) {
        this.data.delete(id);
      }
      
      this.pendingUpdates.clear();
      this.pendingDeletes.clear();
      this.deletedIds.clear();
      this.lastCompaction = Date.now();
      
      await this.saveMetadata();
      
      this.isDirty = false;
      this.emit('save', this.name);
    } catch (error) {
      await fs.unlink(this.tempPath).catch(() => {});
      throw error;
    }
  }

  async flushPendingWrites() {
    if (this.pendingWrites.length === 0) return;
    
    try {
      const lines = this.pendingWrites.map(record => JSON.stringify(record) + '\n');
      await fs.appendFile(this.filePath, lines.join(''));
      
      for (const record of this.pendingWrites) {
        this.data.set(record._id, record);
      }
      
      this.pendingWrites = [];
    } catch (error) {
      this.queueSave();
    }
  }

  queueSave() {
    if (!this.db.options.autoSave) return;
    this.isDirty = true;
    
    clearTimeout(this.saveTimer);
    this.saveTimer = setTimeout(() => {
      this.writeQueue = this.writeQueue.then(() => this.save());
    }, 100);
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
    
    this.cache.set(id, fullRecord);
    this.indexManager.updateIndices(id, null, fullRecord);
    this.deletedIds.delete(id);
    this.pendingDeletes.delete(id);
    
    this.pendingWrites.push(fullRecord);
    
    if (this.pendingWrites.length >= this.batchSize) {
      this.flushPendingWrites().catch(() => this.queueSave());
    }
    
    this.emit('insert', fullRecord);
    return fullRecord;
  }

  insertMany(records) {
    const inserted = [];
    const timestamp = new Date().toISOString();
    
    for (const record of records) {
      const id = record.id || record._id || uuidv4();
      
      const fullRecord = {
        ...record,
        _id: id,
        _created: timestamp,
        _modified: timestamp
      };
      
      this.cache.set(id, fullRecord);
      this.indexManager.updateIndices(id, null, fullRecord);
      this.deletedIds.delete(id);
      this.pendingDeletes.delete(id);
      inserted.push(fullRecord);
      this.pendingWrites.push(fullRecord);
    }
    
    if (this.pendingWrites.length >= this.batchSize) {
      this.flushPendingWrites().catch(() => this.queueSave());
    }
    
    for (const record of inserted) {
      this.emit('insert', record);
    }
    
    return inserted;
  }

  async get(id) {
    if (this.deletedIds.has(id) || this.pendingDeletes.has(id)) return null;
    
    if (this.pendingUpdates.has(id)) {
      return this.pendingUpdates.get(id);
    }
    
    if (this.cache.has(id)) {
      return this.cache.get(id);
    }
    
    if (this.data.has(id)) {
      const record = this.data.get(id);
      this.cache.set(id, record);
      return record;
    }
    
    const record = await this.loadRecordById(id);
    return record;
  }

  async update(id, updates) {
    let record = await this.get(id);
    if (!record) return null;
    
    const oldRecord = this.data.get(id) || record;
    
    const updated = {
      ...record,
      ...updates,
      _id: id,
      _created: record._created,
      _modified: new Date().toISOString()
    };
    
    this.pendingUpdates.set(id, updated);
    this.cache.set(id, updated);
    this.indexManager.updateIndices(id, oldRecord, updated);
    this.queueSave();
    this.emit('update', updated);
    
    return updated;
  }

  async updateMany(condition, updates) {
    const query = new Query(this.data, this.indexManager);
    query.table = this;
    const records = await query.where(condition).execute();
    const updated = [];
    
    const timestamp = new Date().toISOString();
    
    for (const record of records) {
      const oldRecord = this.data.get(record._id) || record;
      
      const updatedRecord = {
        ...record,
        ...updates,
        _id: record._id,
        _created: record._created,
        _modified: timestamp
      };
      
      this.pendingUpdates.set(record._id, updatedRecord);
      this.cache.set(record._id, updatedRecord);
      this.indexManager.updateIndices(record._id, oldRecord, updatedRecord);
      updated.push(updatedRecord);
      this.emit('update', updatedRecord);
    }
    
    if (updated.length > 0) {
      this.queueSave();
    }
    
    return updated;
  }

  async delete(id) {
    const record = await this.get(id);
    if (!record) return false;
    
    this.cache.delete(id);
    this.indexManager.removeFromIndices(id, record);
    this.pendingDeletes.add(id);
    
    this.queueSave();
    
    this.emit('delete', record);
    return true;
  }

  async deleteMany(condition) {
    const query = new Query(this.data, this.indexManager);
    query.table = this;
    const records = await query.where(condition).execute();
    const deleted = [];
    
    for (const record of records) {
      this.cache.delete(record._id);
      this.indexManager.removeFromIndices(record._id, record);
      this.pendingDeletes.add(record._id);
      deleted.push(record);
      this.emit('delete', record);
    }
    
    if (deleted.length > 0) {
      this.queueSave();
    }
    
    return deleted;
  }

  find(condition) {
    const query = new Query(this.data, this.indexManager);
    query.table = this;
    return query.where(condition);
  }

  async findOne(condition) {
    const query = new Query(this.data, this.indexManager);
    query.table = this;
    return await query.where(condition).first();
  }

  async findById(id) {
    return await this.get(id);
  }

  all() {
    const query = new Query(this.data, this.indexManager);
    query.table = this;
    return query;
  }

  async count(condition) {
    if (!condition) {
      const deletedCount = this.deletedIds.size + this.pendingDeletes.size;
      if (this.data.size === this.metadata.recordCount || this.metadata.recordCount === 0) {
        return this.data.size + this.pendingWrites.length - deletedCount;
      }
      return Math.max(0, this.metadata.recordCount + this.pendingWrites.length - deletedCount);
    }
    const query = new Query(this.data, this.indexManager);
    query.table = this;
    return await query.where(condition).count();
  }

  createIndex(field) {
    this.indexManager.createIndex(field, this.data);
    if (this.data.size < this.metadata.recordCount) {
      this.buildIndicesFromFile();
    }
    this.queueSave();
    return this;
  }

  dropIndex(field) {
    this.indexManager.dropIndex(field);
    this.queueSave();
    return this;
  }

  async flush() {
    await this.flushPendingWrites();
    await this.writeQueue;
  }

  async drop() {
    clearTimeout(this.saveTimer);
    await fs.unlink(this.filePath).catch(() => {});
    await fs.unlink(this.metaPath).catch(() => {});
    await fs.unlink(this.tempPath).catch(() => {});
    this.data.clear();
    this.cache.clear();
    this.deletedIds.clear();
    this.pendingWrites = [];
    this.pendingUpdates.clear();
    this.pendingDeletes.clear();
    this.emit('drop');
  }

  async truncate() {
    clearTimeout(this.saveTimer);
    this.data.clear();
    this.cache.clear();
    this.indexManager.clear();
    this.deletedIds.clear();
    this.pendingWrites = [];
    this.pendingUpdates.clear();
    this.pendingDeletes.clear();
    this.metadata.recordCount = 0;
    await fs.unlink(this.filePath).catch(() => {});
    await this.saveMetadata();
    this.emit('truncate');
  }
}

module.exports = { Table };