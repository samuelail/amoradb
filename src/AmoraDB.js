const fs = require('fs').promises;
const { createReadStream } = require('fs');
const { createInterface } = require('readline');
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
      version: '1.1.0',
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
      await this.migrateFromJSON();
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

  async migrateFromJSON() {
    try {
      const files = await fs.readdir(this.dbPath);
      const jsonFiles = files.filter(f => f.endsWith('.json') && !f.startsWith('_'));
      
      for (const file of jsonFiles) {
        const tableName = path.basename(file, '.json');
        const jsonPath = path.join(this.dbPath, file);
        const jsonlPath = path.join(this.dbPath, `${tableName}.jsonl`);
        const metaPath = path.join(this.dbPath, `${tableName}.meta.json`);
        
        try {
          await fs.access(jsonlPath);
          continue;
        } catch {
        }
        
        try {
          const jsonContent = await fs.readFile(jsonPath, 'utf-8');
          const parsed = JSON.parse(jsonContent);
          
          let recordCount = 0;
          let autoIncrement = 1;
          const indices = [];
          
          if (parsed.data) {
            const jsonlLines = [];
            for (const [id, record] of Object.entries(parsed.data)) {
              jsonlLines.push(JSON.stringify(record));
              recordCount++;
            }
            
            if (jsonlLines.length > 0) {
              await fs.writeFile(jsonlPath, jsonlLines.join('\n') + '\n');
            }
            
            autoIncrement = parsed.autoIncrement || 1;
            if (parsed.indices) {
              indices.push(...parsed.indices);
            }
          }
          
          const metadata = {
            autoIncrement,
            indices,
            recordCount,
            modified: parsed.modified || new Date().toISOString()
          };
          
          await fs.writeFile(metaPath, JSON.stringify(metadata, null, 2));
          await fs.unlink(jsonPath);
          
        } catch (migrationError) {
        }
      }
    } catch (error) {
    }
  }

  async loadTables() {
    const files = await fs.readdir(this.dbPath);
    const tableFiles = files.filter(f => f.endsWith('.jsonl'));
    
    for (const file of tableFiles) {
      const tableName = path.basename(file, '.jsonl');
      
      if (!this.metadata.tables[tableName]) {
        this.metadata.tables[tableName] = {
          created: new Date().toISOString(),
          modified: new Date().toISOString(),
          indices: [],
          schema: {}
        };
      }
      
      const table = new Table(tableName, this);
      await table.load();
      this.tables.set(tableName, table);
    }
    
    if (tableFiles.length > 0) {
      await this.saveMetadata();
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

  async getStats() {
    const stats = {
      tables: this.tables.size,
      totalRecords: 0,
      cacheStats: {}
    };

    for (const [name, table] of this.tables.entries()) {
      const recordCount = await table.count();
      stats.totalRecords += recordCount;
      stats.cacheStats[name] = table.cache.getStats();
    }

    return stats;
  }

  async optimize() {
    for (const [name, table] of this.tables.entries()) {
      await table.flush();
      
      const indices = table.indexManager.getIndices();
      for (const field of indices) {
        table.indexManager.dropIndex(field);
        table.createIndex(field);
      }
    }
  }
}

module.exports = AmoraDB;