const fs = require('fs').promises;
const path = require('path');
const { Table } = require('./Table');
const { EventEmitter } = require('events');

class AmoraDB extends EventEmitter {
  constructor(dbName, options = {}) {
    super();
    this.dbName = dbName;
    this.dbPath = path.join(options.dataPath || './data', dbName);
    this.tables = new Map();
    this.options = {
      autoSave: options.autoSave !== false,
      cacheSize: options.cacheSize || 1000,
      indexAutoCreate: options.indexAutoCreate !== false,
      compression: options.compression || false,
      ...options
    };
    this.metadata = {
      version: '1.0.0',
      created: null,
      modified: null,
      tables: {}
    };
    this.initialized = false;
  }

  async init() {
    if (this.initialized) return this;
    
    try {
      await this.ensureDataDirectory();
      await this.loadMetadata();
      await this.loadTables();
      this.initialized = true;
      this.emit('ready', this);
    } catch (error) {
      this.emit('error', error);
      throw error;
    }
    
    return this;
  }

  async ensureDataDirectory() {
    await fs.mkdir(this.dbPath, { recursive: true });
  }

  async loadMetadata() {
    const metaPath = path.join(this.dbPath, '_metadata.json');
    try {
      const data = await fs.readFile(metaPath, 'utf-8');
      this.metadata = JSON.parse(data);
    } catch (error) {
      if (error.code === 'ENOENT') {
        this.metadata.created = new Date().toISOString();
        await this.saveMetadata();
      } else {
        throw error;
      }
    }
  }

  async saveMetadata() {
    const metaPath = path.join(this.dbPath, '_metadata.json');
    this.metadata.modified = new Date().toISOString();
    await fs.writeFile(metaPath, JSON.stringify(this.metadata, null, 2));
  }

  async loadTables() {
    const files = await fs.readdir(this.dbPath);
    const tableFiles = files.filter(f => f.endsWith('.json') && !f.startsWith('_'));
    
    for (const file of tableFiles) {
      const tableName = path.basename(file, '.json');
      if (this.metadata.tables[tableName]) {
        const table = new Table(tableName, this);
        await table.load();
        this.tables.set(tableName, table);
      }
    }
  }

  table(name) {
    if (!this.initialized) {
      throw new Error('Database not initialized. Call init() first.');
    }
    
    if (!this.tables.has(name)) {
      const table = new Table(name, this);
      this.tables.set(name, table);
      this.metadata.tables[name] = {
        created: new Date().toISOString(),
        modified: new Date().toISOString(),
        indices: [],
        schema: {}
      };
      this.saveMetadata();
    }
    
    return this.tables.get(name);
  }

  async dropTable(name) {
    if (this.tables.has(name)) {
      const table = this.tables.get(name);
      await table.drop();
      this.tables.delete(name);
      delete this.metadata.tables[name];
      await this.saveMetadata();
    }
  }

  async listTables() {
    return Array.from(this.tables.keys());
  }

  async close() {
    for (const table of this.tables.values()) {
      await table.flush();
    }
    this.emit('close');
  }

  async backup(backupPath) {
    const backupDir = backupPath || `${this.dbPath}_backup_${Date.now()}`;
    await fs.mkdir(backupDir, { recursive: true });
    
    const files = await fs.readdir(this.dbPath);
    for (const file of files) {
      const src = path.join(this.dbPath, file);
      const dest = path.join(backupDir, file);
      await fs.copyFile(src, dest);
    }
    
    return backupDir;
  }

  async drop() {
    await this.close();
    await fs.rm(this.dbPath, { recursive: true, force: true });
  }
}

module.exports = AmoraDB;