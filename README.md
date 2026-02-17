# AmoraDB

A lightweight, file-based NoSQL database for Node.js with MongoDB-like query syntax and zero external dependencies.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/badge/node-%3E%3D%2014.0.0-brightgreen)](https://nodejs.org)

## Overview

AmoraDB is an embedded document database that stores data as JSONL (newline-delimited JSON) files on disk. It provides a chainable query builder, in-memory indexing, an LRU cache, and an event system — all with no external dependencies. Everything runs off Node.js built-ins (`fs`, `path`, `crypto`, `events`, `readline`).

It works well for Electron/desktop apps, microservices, CLI tools, offline-first applications, and any Node.js project that needs persistent document storage without running a separate database server.

## Installation

```bash
npm install amoradb
```

## Quick Start

```javascript
const AmoraDB = require('amoradb');

const db = new AmoraDB('myapp');
await db.init();

const users = db.table('users');

// Insert a record (synchronous, writes are batched to disk)
const user = users.insert({
  name: 'Sarah Connor',
  email: 'sarah@resistance.com',
  age: 29
});

// Query with operators
const results = await users
  .find({ age: { $gte: 18 } })
  .sort('name', 'asc')
  .limit(10)
  .execute();

// Update
await users.update(user._id, { status: 'active' });

// Close (flushes pending writes to disk)
await db.close();
```

## How Data Is Stored

```
data/
└── myapp/
    ├── _metadata.json       # Database metadata and index definitions
    ├── users.jsonl           # One JSON object per line
    ├── users.meta.json       # Table-level metadata (record count, indices)
    ├── orders.jsonl
    └── orders.meta.json
```

Each record is a JSON object stored on its own line. Every record gets automatic metadata fields:

```javascript
{
  _id: "550e8400-e29b-41d4-a716-446655440000",  // UUID v4 (auto-generated, or supply your own)
  _created: "2024-01-01T00:00:00.000Z",
  _modified: "2024-01-01T00:00:00.000Z",
  // ...your fields
}
```

Inserts are synchronous in-memory and batched to disk asynchronously. Updates and deletes are tracked in memory and compacted into the file periodically (when mutations exceed 30% of total records) or on `flush()`/`close()`.

## Query Language

### Basic Queries

```javascript
// All records
await users.all().execute();

// Find with conditions
await users.find({ status: 'active' }).execute();

// Find one
await users.findOne({ email: 'user@example.com' });

// Find by ID
await users.findById('uuid-here');

// Function predicates work too
await users.find(record => record.age > 28).execute();
```

### Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equals | `{ age: { $eq: 25 } }` |
| `$ne` | Not equals | `{ status: { $ne: 'deleted' } }` |
| `$gt` | Greater than | `{ age: { $gt: 18 } }` |
| `$gte` | Greater than or equal | `{ score: { $gte: 90 } }` |
| `$lt` | Less than | `{ price: { $lt: 100 } }` |
| `$lte` | Less than or equal | `{ quantity: { $lte: 5 } }` |
| `$in` | Value in array | `{ role: { $in: ['admin', 'mod'] } }` |
| `$nin` | Value not in array | `{ status: { $nin: ['banned', 'deleted'] } }` |
| `$regex` | Pattern match | `{ email: { $regex: '@gmail\\.com' } }` |
| `$exists` | Field exists | `{ phone: { $exists: true } }` |

Multiple operators on the same field are ANDed together: `{ price: { $gt: 10, $lt: 50 } }`.

The `$regex` operator supports an optional `$options` key for flags (e.g., `{ name: { $regex: 'john', $options: 'i' } }` for case-insensitive matching).

### Chaining

```javascript
const results = await users
  .find({ age: { $gte: 18 } })
  .and({ subscription: 'premium' })
  .or({ role: 'vip' })
  .sort('joinDate', 'desc')
  .skip(0)
  .limit(20)
  .select(['email', 'name'])
  .execute();
```

- `where()` / `and()` — add an AND condition
- `or()` — add an OR condition
- `sort(field, 'asc' | 'desc')` — sort results
- `limit(n)` / `skip(n)` — pagination
- `select(fields[])` — project specific fields
- `distinct(field)` — return unique values (supports dot-notation like `'address.city'`)
- `first()` — return just the first match
- `count()` — return the count of matches

### Aggregation

```javascript
const stats = await users
  .find({ active: true })
  .aggregate({
    totalUsers: { $count: true },
    totalAge: { $sum: 'age' },
    avgAge: { $avg: 'age' },
    minAge: { $min: 'age' },
    maxAge: { $max: 'age' }
  });
```

## Indexing

Create indices on frequently queried fields to speed up lookups. The index type is auto-detected: numeric and date fields get a sorted index (binary search for range queries), while string/categorical fields get a hash index (fast equality and `$in` lookups).

```javascript
await users.createIndex('email');
await users.createIndex('age');

// Remove an index
users.dropIndex('email');
```

Indices are rebuilt from the data file on startup. Index definitions are persisted in the metadata files so they survive restarts.

The query engine automatically uses available indices when evaluating conditions, and falls back to a full scan for non-indexed fields.

## Caching

AmoraDB uses an in-memory LRU cache. When a table's total record count fits within the cache size, all records are held in memory. For larger tables, records are loaded on demand and the least recently used entries are evicted when the cache is full.

```javascript
const db = new AmoraDB('myapp', {
  cacheSize: 5000  // default is 1000
});

// Check cache performance
const stats = users.cache.getStats();
// { size, maxSize, hits, misses, hitRate }
```

For tables that exceed the cache size, queries stream the JSONL file line-by-line rather than loading everything into memory.

## Events

Both the database and individual tables emit events via Node.js `EventEmitter`.

```javascript
// Table events
users.on('insert', (record) => { /* ... */ });
users.on('update', (record) => { /* ... */ });
users.on('delete', (record) => { /* ... */ });
users.on('save', (tableName) => { /* ... */ });
users.on('truncate', () => { /* ... */ });
users.on('drop', () => { /* ... */ });

// Database events
db.on('ready', (db) => { /* ... */ });
db.on('error', (err) => { /* ... */ });
db.on('close', () => { /* ... */ });
```

## API Reference

### Database

| Method | Description |
|--------|-------------|
| `new AmoraDB(name, options?)` | Create a database instance |
| `await db.init()` | Initialize the database, load existing tables |
| `db.table(name)` | Get or create a table |
| `await db.dropTable(name)` | Delete a table and its files |
| `await db.listTables()` | List all table names |
| `await db.backup(path?)` | Copy all files to a backup directory |
| `await db.close()` | Flush all pending writes and close |
| `await db.drop()` | Close and delete the entire database directory |
| `await db.getStats()` | Get record counts and cache stats per table |
| `await db.optimize()` | Flush writes and rebuild all indices |

### Table

| Method | Description |
|--------|-------------|
| `insert(record)` | Insert a single record (sync) |
| `insertMany(records)` | Insert multiple records (sync) |
| `find(query)` | Returns a chainable query builder |
| `findOne(query)` | Find first matching record |
| `findById(id)` | Find record by `_id` |
| `all()` | Query builder for all records |
| `await update(id, changes)` | Update a record by ID |
| `await updateMany(query, changes)` | Update all matching records |
| `await delete(id)` | Delete a record by ID |
| `await deleteMany(query)` | Delete all matching records |
| `await count(query?)` | Count records (optionally filtered) |
| `await createIndex(field)` | Create an index on a field |
| `dropIndex(field)` | Remove an index |
| `await flush()` | Force flush pending writes to disk |
| `await truncate()` | Clear all records from the table |
| `await drop()` | Delete the table and its files |

### Query Builder

| Method | Description |
|--------|-------------|
| `where(condition)` | Add a filter condition |
| `and(condition)` | AND another condition |
| `or(condition)` | OR another condition |
| `sort(field, order)` | Sort by field (`'asc'` or `'desc'`) |
| `limit(n)` | Limit number of results |
| `skip(n)` | Skip n results |
| `select(fields)` | Project specific fields |
| `await execute()` | Run the query, return results array |
| `await first()` | Return first match |
| `await count()` | Return count of matches |
| `await distinct(field)` | Return unique values |
| `await aggregate(ops)` | Run aggregation operations |

## Configuration

```javascript
const db = new AmoraDB('myapp', {
  dataPath: './custom/path',  // where to store database files (default: './data')
  cacheSize: 2000,            // LRU cache capacity (default: 1000)
  autoSave: true              // auto-flush writes on a debounced timer (default: true)
});
```

## Limitations

- **Single process**: Designed for use within a single Node.js process. Multi-process file locking is on the roadmap.
- **No transactions**: Writes use atomic file rename for durability, but there is no rollback or multi-operation transaction mechanism.

## Roadmap

- [ ] TypeScript definitions (in progress)
- [ ] Multi-process support with file locking
- [ ] Data compression
- [ ] Encrypted storage
- [ ] Browser support (IndexedDB backend)
- [ ] Replication and sync
- [ ] Query optimizer improvements
- [ ] Migration system

## Contributing

Contributions are welcome.

```bash
git clone https://github.com/samuelail/amoradb.git
cd amoradb
npm install
npm test
```

## License

MIT - Samuel Ailemen

---

[GitHub Issues](https://github.com/samuelail/amoradb/issues)
