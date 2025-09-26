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
    this.data = new Map();
    this.cache = new SimpleCache(db.options.cacheSize);
    this.indexManager = new IndexManager();
    this.writeQueue = Promise.resolve();
    this.isDirty = false;
    this.autoIncrement = 1;
    this.isLoaded = false;
    this.metadata = {
      autoIncrement: 1,
      indices: [],
      recordCount: 0,
      modified: null
    };
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
            this.data.set(record._id, record);
            this.indexManager.updateIndices(record._id, record);
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
            this.indexManager.updateIndices(record._id, record);
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
    if (this.data.has(id)) {
      return this.data.get(id);
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
            if (record._id === id) {
              this.cache.set(id, record);
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

  async save() {
    const tempPath = `${this.filePath}.tmp`;
    const writeStream = createWriteStream(tempPath);
    
    try {
      for (const record of this.data.values()) {
        writeStream.write(JSON.stringify(record) + '\n');
      }
      
      await new Promise((resolve, reject) => {
        writeStream.end((error) => {
          if (error) reject(error);
          else resolve();
        });
      });
      
      await fs.rename(tempPath, this.filePath);
      await this.saveMetadata();
      
      this.isDirty = false;
      this.emit('save', this.name);
    } catch (error) {
      await fs.unlink(tempPath).catch(() => {});
      throw error;
    }
  }

  async appendRecord(record) {
    try {
      const line = JSON.stringify(record) + '\n';
      await fs.appendFile(this.filePath, line);
    } catch (error) {
      this.queueSave();
    }
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
    
    this.appendRecord(fullRecord).catch(() => {
      this.queueSave();
    });
    
    this.emit('insert', fullRecord);
    return fullRecord;
  }

  insertMany(records) {
    const inserted = [];
    for (const record of records) {
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
      inserted.push(fullRecord);
    }
    
    this.queueSave();
    
    for (const record of inserted) {
      this.emit('insert', record);
    }
    
    return inserted;
  }

  async get(id) {
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

  async updateMany(condition, updates) {
    const query = new Query(this.data, this.indexManager);
    query.table = this;
    const records = await query.where(condition).execute();
    const updated = [];
    
    for (const record of records) {
      const result = await this.update(record._id, updates);
      if (result) updated.push(result);
    }
    
    return updated;
  }

  async delete(id) {
    const record = await this.get(id);
    if (!record) return false;
    
    this.data.delete(id);
    this.cache.delete(id);
    this.indexManager.removeFromIndices(id);
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
      if (await this.delete(record._id)) {
        deleted.push(record);
      }
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
      if (this.data.size === this.metadata.recordCount || this.metadata.recordCount === 0) {
        return this.data.size;
      }
      return this.metadata.recordCount;
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
    await this.writeQueue;
  }

  async drop() {
    await fs.unlink(this.filePath).catch(() => {});
    await fs.unlink(this.metaPath).catch(() => {});
    this.data.clear();
    this.cache.clear();
    this.emit('drop');
  }

  async truncate() {
    this.data.clear();
    this.cache.clear();
    this.indexManager.clear();
    this.metadata.recordCount = 0;
    await fs.unlink(this.filePath).catch(() => {});
    await this.saveMetadata();
    this.emit('truncate');
  }
}

module.exports = { Table };