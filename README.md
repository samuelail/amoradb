# AmoraDB

A lightweight, file-based NoSQL database for Node.js applications with MongoDB-like query syntax and zero dependencies for core functionality.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Node.js Version](https://img.shields.io/badge/node-%3E%3D%2014.0.0-brightgreen)](https://nodejs.org)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

## 🎯 Why AmoraDB?

AmoraDB bridges the gap between simple JSON file storage and complex database systems. Perfect for:

- **Rapid Prototyping**: Start building immediately without database setup
- **Small to Medium Applications**: Handle thousands of records efficiently  
- **Electron/Desktop Apps**: Embedded database with no external dependencies
- **Microservices**: Lightweight data persistence without infrastructure overhead
- **Educational Projects**: Learn database concepts with readable JSON storage
- **Offline-First Applications**: Full functionality without network connectivity

## ✨ Key Features

- 🚀 **Zero Configuration**: No server setup, no connection strings
- 📁 **Human-Readable Storage**: Data stored as formatted JSON files
- 🔍 **MongoDB-like Queries**: Familiar syntax with operators like `$gte`, `$regex`, `$in`
- ⚡ **Indexed Queries**: Create indices for lightning-fast lookups
- 🔗 **Chainable API**: Build complex queries with intuitive method chaining
- 💾 **ACID-like Guarantees**: Atomic writes with transaction support
- 🎯 **In-Memory Caching**: LRU cache for optimal performance
- 📊 **Aggregation Pipeline**: Built-in sum, average, min, max operations
- 🔄 **Real-time Events**: EventEmitter for reactive applications
- 🛡️ **Type Safety**: Full TypeScript support (coming soon)

## 📦 Installation

```bash
npm install amoradb
```

Or with yarn:
```bash
yarn add amoradb
```

## 🚀 Quick Start

```javascript
const AmoraDB = require('amoradb');

// Initialize database
const db = new AmoraDB('myapp');
await db.init();

// Get a table (collection)
const users = db.table('users');

// Insert data
const user = users.insert({
  name: 'Sarah Connor',
  email: 'sarah@resistance.com',
  age: 29,
  role: 'leader'
});

// Query with operators
const results = users
  .find({ age: { $gte: 18 } })
  .sort('name', 'asc')
  .limit(10)
  .execute();

// Update records
users.update(user._id, { status: 'active' });

// Close when done
await db.close();
```

## 📖 Core Concepts

### Database Structure
```
data/
└── myapp/                    # Database directory
    ├── _metadata.json        # Database metadata
    ├── users.json           # Users table
    ├── orders.json          # Orders table
    └── products.json        # Products table
```

### Tables (Collections)
Tables are schema-less document stores that hold related data. Each table is stored as a separate JSON file.

```javascript
const users = db.table('users');     // Get or create table
const products = db.table('products');
```

### Documents
Documents are JavaScript objects with automatic metadata:

```javascript
{
  _id: "uuid-here",           // Auto-generated unique ID
  _created: "2024-01-01T00:00:00Z",  // Creation timestamp
  _modified: "2024-01-01T00:00:00Z", // Last modified timestamp
  ...yourData                 // Your custom fields
}
```

## 🔍 Query Language

### Basic Queries

```javascript
// Find all
users.all().execute();

// Find with conditions
users.find({ status: 'active' }).execute();

// Find one
users.findOne({ email: 'user@example.com' });

// Find by ID
users.findById('uuid-here');
```

### Query Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `$eq` | Equals | `{ age: { $eq: 25 } }` |
| `$ne` | Not equals | `{ status: { $ne: 'deleted' } }` |
| `$gt` | Greater than | `{ age: { $gt: 18 } }` |
| `$gte` | Greater than or equal | `{ score: { $gte: 90 } }` |
| `$lt` | Less than | `{ price: { $lt: 100 } }` |
| `$lte` | Less than or equal | `{ quantity: { $lte: 5 } }` |
| `$in` | In array | `{ role: { $in: ['admin', 'mod'] } }` |
| `$nin` | Not in array | `{ status: { $nin: ['banned', 'deleted'] } }` |
| `$regex` | Pattern match | `{ email: { $regex: '@gmail.com' } }` |
| `$exists` | Field exists | `{ phone: { $exists: true } }` |

### Complex Queries

```javascript
// Chained conditions
const premiumUsers = users
  .find({ age: { $gte: 18 } })
  .and({ subscription: 'premium' })
  .or({ role: 'vip' })
  .sort('joinDate', 'desc')
  .skip(0)
  .limit(20)
  .execute();

// Select specific fields
const emails = users
  .find({ newsletter: true })
  .select(['email', 'name'])
  .execute();

// Distinct values
const uniqueCities = users.all().distinct('address.city');
```

## 📊 Aggregation

```javascript
const stats = users
  .find({ active: true })
  .aggregate({
    totalUsers: { $count: true },
    totalAge: { $sum: 'age' },
    avgAge: { $avg: 'age' },
    minAge: { $min: 'age' },
    maxAge: { $max: 'age' }
  });

console.log(stats);
// { totalUsers: 150, totalAge: 4500, avgAge: 30, minAge: 18, maxAge: 65 }
```

## ⚡ Performance Optimization

### Indexing
Create indices for frequently queried fields:

```javascript
users.createIndex('email');     // Fast email lookups
users.createIndex('username');  // Fast username queries
users.createIndex('created_at'); // Fast date sorting
```

### Caching
Built-in LRU cache with configurable size:

```javascript
const db = new AmoraDB('myapp', {
  cacheSize: 5000  // Cache up to 5000 records in memory
});

// Monitor cache performance
const stats = users.cache.getStats();
console.log(`Cache hit rate: ${stats.hitRate * 100}%`);
```

## 🔄 Real-time Events

```javascript
users.on('insert', (record) => {
  console.log('New user:', record);
});

users.on('update', (record) => {
  console.log('Updated user:', record);
});

users.on('delete', (record) => {
  console.log('Deleted user:', record);
});
```

## 🛠️ API Reference

### Database Methods

| Method | Description |
|--------|-------------|
| `new AmoraDB(name, options)` | Create database instance |
| `await db.init()` | Initialize database |
| `db.table(name)` | Get or create table |
| `await db.dropTable(name)` | Delete table |
| `await db.listTables()` | List all tables |
| `await db.backup(path)` | Backup database |
| `await db.close()` | Close database |

### Table Methods

| Method | Description |
|--------|-------------|
| `insert(record)` | Insert single record |
| `insertMany(records)` | Insert multiple records |
| `find(query)` | Query builder |
| `findOne(query)` | Find first match |
| `findById(id)` | Find by ID |
| `update(id, changes)` | Update by ID |
| `updateMany(query, changes)` | Update multiple |
| `delete(id)` | Delete by ID |
| `deleteMany(query)` | Delete multiple |
| `count(query)` | Count matching records |
| `createIndex(field)` | Create index |
| `all()` | Get all records |

### Query Methods

| Method | Description |
|--------|-------------|
| `where(condition)` | Add condition |
| `and(condition)` | AND condition |
| `or(condition)` | OR condition |
| `sort(field, order)` | Sort results |
| `limit(n)` | Limit results |
| `skip(n)` | Skip records |
| `select(fields)` | Select fields |
| `distinct(field)` | Unique values |
| `aggregate(ops)` | Aggregation |
| `execute()` | Run query |

## 🎯 Use Cases

### 1. User Management System
```javascript
const users = db.table('users');
users.createIndex('email');

// Registration
const newUser = users.insert({
  email: 'user@example.com',
  password: hashedPassword,
  profile: {
    name: 'John Doe',
    avatar: 'avatar.jpg'
  }
});

// Authentication
const user = users.findOne({ 
  email: 'user@example.com',
  active: true 
});

// Update last login
users.update(user._id, { 
  lastLogin: new Date().toISOString() 
});
```

### 2. Shopping Cart
```javascript
const carts = db.table('carts');

// Add to cart
carts.insert({
  userId: 'user-123',
  items: [
    { productId: 'prod-1', quantity: 2, price: 29.99 },
    { productId: 'prod-2', quantity: 1, price: 49.99 }
  ],
  total: 109.97
});

// Get user's cart
const cart = carts.findOne({ userId: 'user-123' });

// Calculate totals
const stats = carts.aggregate({
  totalRevenue: { $sum: 'total' },
  avgCartValue: { $avg: 'total' },
  cartCount: { $count: true }
});
```

### 3. Activity Logger
```javascript
const logs = db.table('activity_logs');
logs.createIndex('timestamp');
logs.createIndex('userId');

// Log activity
logs.insert({
  userId: 'user-123',
  action: 'LOGIN',
  timestamp: Date.now(),
  metadata: { ip: '192.168.1.1', device: 'mobile' }
});

// Query recent activities
const recentLogs = logs
  .find({ 
    timestamp: { $gte: Date.now() - 86400000 } // Last 24h
  })
  .sort('timestamp', 'desc')
  .limit(100)
  .execute();
```

## ⚙️ Configuration Options

```javascript
const db = new AmoraDB('myapp', {
  dataPath: './custom/path',    // Custom data directory
  cacheSize: 2000,              // LRU cache size
  autoSave: true,               // Auto-save on changes
  compression: false,           // Compression (future feature)
  indexAutoCreate: true         // Auto-create indices
});
```

## 🚀 Performance Benchmarks

| Operation | Records | Time | Ops/sec |
|-----------|---------|------|---------|
| Insert | 10,000 | ~2s | 5,000 |
| Find (indexed) | 10,000 | ~5ms | 2,000,000 |
| Find (non-indexed) | 10,000 | ~50ms | 200,000 |
| Update | 1,000 | ~200ms | 5,000 |
| Delete | 1,000 | ~150ms | 6,666 |

*Benchmarks on MacBook Pro M1, Node.js 18*

## 🔒 Limitations

- **File Size**: Best for databases under 100MB
- **Concurrency**: Single-process only (no multi-process support)
- **Transactions**: Basic transaction support, not full ACID
- **Scalability**: Not suitable for high-traffic production systems

## 🗺️ Roadmap

- [ ] TypeScript definitions
- [ ] Multi-process support with file locking
- [ ] Data compression
- [ ] Encrypted storage
- [ ] GraphQL adapter
- [ ] Browser support (IndexedDB backend)
- [ ] Replication and sync
- [ ] Query optimization engine
- [ ] Migrations system
- [ ] CLI tools

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details.

```bash
# Clone repository
git clone https://github.com/samuelail/amoradb.git

# Install dependencies
npm install

# Run tests
npm test

# Build
npm run build
```

## 📄 License

MIT © Samuel Ailemen

## 🙏 Acknowledgments

Inspired by:
- [LowDB](https://github.com/typicode/lowdb) - Simple JSON database
- [NeDB](https://github.com/louischatriot/nedb) - Embedded persistent database
- [MongoDB](https://mongodb.com) - Query syntax inspiration

## Support
- 🐛 Issues: [GitHub Issues](https://github.com/samuelail/amoradb/issues)

---

**Built with ❤️ for developers**