const AmoraDB = require('../index');
const fs = require('fs').promises;
const path = require('path');

const TEST_DB_PATH = path.join(__dirname, 'test-data');

let passed = 0;
let failed = 0;
let total = 0;

function assert(condition, message) {
  total++;
  if (condition) {
    passed++;
    console.log(`  \x1b[32m✓\x1b[0m ${message}`);
  } else {
    failed++;
    console.log(`  \x1b[31m✗\x1b[0m ${message}`);
  }
}

function assertEqual(actual, expected, message) {
  const pass = JSON.stringify(actual) === JSON.stringify(expected);
  total++;
  if (pass) {
    passed++;
    console.log(`  \x1b[32m✓\x1b[0m ${message}`);
  } else {
    failed++;
    console.log(`  \x1b[31m✗\x1b[0m ${message}`);
    console.log(`      expected: ${JSON.stringify(expected)}`);
    console.log(`      actual:   ${JSON.stringify(actual)}`);
  }
}

async function cleanup() {
  await fs.rm(TEST_DB_PATH, { recursive: true, force: true }).catch(() => {});
}

// ─── Database Lifecycle ───────────────────────────────────────────────

async function testDatabaseInit() {
  console.log('\n─── Database Initialization ───');

  const db = new AmoraDB('test-init', { dataPath: TEST_DB_PATH });
  await db.init();

  assert(db.initialized === true, 'db.initialized is true after init()');
  assert(db.metadata.version === '1.1.0', 'metadata version is 1.1.0');
  assert(db.metadata.created !== null, 'metadata.created is set');

  // Double init should be safe
  const result = await db.init();
  assert(result === db, 'double init returns same instance');

  await db.drop();
}

async function testDatabaseInitError() {
  console.log('\n─── Database Init Error ───');

  const db = new AmoraDB('test-init-err', { dataPath: TEST_DB_PATH });

  let threw = false;
  try {
    db.table('anything');
  } catch (e) {
    threw = true;
  }
  assert(threw, 'table() throws before init()');
}

async function testDatabaseClose() {
  console.log('\n─── Database Close ───');

  const db = new AmoraDB('test-close', { dataPath: TEST_DB_PATH });
  await db.init();

  const users = db.table('users');
  users.insert({ name: 'Alice', age: 30 });
  await users.update((await users.findOne({ name: 'Alice' }))._id, { age: 31 });

  await db.close();

  // Reopen and verify data persisted
  const db2 = new AmoraDB('test-close', { dataPath: TEST_DB_PATH });
  await db2.init();
  const alice = await db2.table('users').findOne({ name: 'Alice' });

  assert(alice !== null, 'record exists after close and reopen');
  assertEqual(alice.age, 31, 'updated value persisted through close()');

  await db2.drop();
}

async function testDatabaseDrop() {
  console.log('\n─── Database Drop ───');

  const db = new AmoraDB('test-drop', { dataPath: TEST_DB_PATH });
  await db.init();
  db.table('users').insert({ name: 'Alice' });
  await db.table('users').flush();
  await db.drop();

  let exists = true;
  try {
    await fs.access(path.join(TEST_DB_PATH, 'test-drop'));
    exists = true;
  } catch {
    exists = false;
  }
  assert(!exists, 'database directory removed after drop()');
}

async function testDatabaseBackup() {
  console.log('\n─── Database Backup ───');

  const db = new AmoraDB('test-backup', { dataPath: TEST_DB_PATH });
  await db.init();
  db.table('users').insert({ name: 'Alice' });
  await db.table('users').flush();

  const backupDir = path.join(TEST_DB_PATH, 'my-backup');
  const result = await db.backup(backupDir);

  assertEqual(result, backupDir, 'backup returns the backup path');

  const files = await fs.readdir(backupDir);
  assert(files.length > 0, 'backup directory has files');

  await db.drop();
  await fs.rm(backupDir, { recursive: true, force: true }).catch(() => {});
}

async function testDatabaseStats() {
  console.log('\n─── Database Stats ───');

  const db = new AmoraDB('test-stats', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');
  users.insert({ name: 'Alice' });
  users.insert({ name: 'Bob' });

  const stats = await db.getStats();
  assertEqual(stats.tables, 1, 'stats shows 1 table');
  assertEqual(stats.totalRecords, 2, 'stats shows 2 records');
  assert(stats.cacheStats.users !== undefined, 'cache stats present for users table');

  await db.drop();
}

async function testListAndDropTable() {
  console.log('\n─── List and Drop Tables ───');

  const db = new AmoraDB('test-tables', { dataPath: TEST_DB_PATH });
  await db.init();
  db.table('users');
  db.table('orders');

  let tables = await db.listTables();
  assert(tables.includes('users'), 'users table listed');
  assert(tables.includes('orders'), 'orders table listed');

  await db.dropTable('orders');
  tables = await db.listTables();
  assert(!tables.includes('orders'), 'orders table removed after drop');
  assert(tables.includes('users'), 'users table still exists');

  await db.drop();
}

// ─── Insert Operations ───────────────────────────────────────────────

async function testInsertSingle() {
  console.log('\n─── Insert Single ───');

  const db = new AmoraDB('test-insert', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const record = users.insert({ name: 'Alice', age: 30 });

  assert(record._id !== undefined, 'inserted record has _id');
  assert(record._created !== undefined, 'inserted record has _created');
  assert(record._modified !== undefined, 'inserted record has _modified');
  assertEqual(record.name, 'Alice', 'inserted record has correct name');
  assertEqual(record.age, 30, 'inserted record has correct age');

  await db.drop();
}

async function testInsertMany() {
  console.log('\n─── Insert Many ───');

  const db = new AmoraDB('test-insert-many', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const records = users.insertMany([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
    { name: 'Charlie', age: 35 }
  ]);

  assertEqual(records.length, 3, 'insertMany returns 3 records');
  assert(records.every(r => r._id), 'all records have _id');
  assertEqual(records[0].name, 'Alice', 'first record is Alice');
  assertEqual(records[2].name, 'Charlie', 'third record is Charlie');

  await db.drop();
}

async function testInsertWithCustomId() {
  console.log('\n─── Insert with Custom ID ───');

  const db = new AmoraDB('test-custom-id', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const record = users.insert({ _id: 'custom-123', name: 'Alice' });
  assertEqual(record._id, 'custom-123', 'custom _id preserved');

  const found = await users.findById('custom-123');
  assertEqual(found.name, 'Alice', 'findById with custom id works');

  await db.drop();
}

// ─── Query Operations ────────────────────────────────────────────────

async function testFindAndFindOne() {
  console.log('\n─── Find and FindOne ───');

  const db = new AmoraDB('test-find', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30, city: 'NYC' },
    { name: 'Bob', age: 25, city: 'LA' },
    { name: 'Charlie', age: 35, city: 'NYC' },
    { name: 'Diana', age: 28, city: 'LA' }
  ]);

  const nycUsers = await users.find({ city: 'NYC' }).execute();
  assertEqual(nycUsers.length, 2, 'find NYC returns 2 results');

  const alice = await users.findOne({ name: 'Alice' });
  assertEqual(alice.name, 'Alice', 'findOne returns correct record');
  assertEqual(alice.age, 30, 'findOne record has correct age');

  const nobody = await users.findOne({ name: 'Nobody' });
  assertEqual(nobody, null, 'findOne returns null for no match');

  await db.drop();
}

async function testQueryOperators() {
  console.log('\n─── Query Operators ───');

  const db = new AmoraDB('test-operators', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30, active: true },
    { name: 'Bob', age: 25, active: false },
    { name: 'Charlie', age: 35, active: true },
    { name: 'Diana', age: 28, active: true },
    { name: 'Eve', age: 22 }
  ]);

  // $eq
  const eq = await users.find({ age: { $eq: 30 } }).execute();
  assertEqual(eq.length, 1, '$eq: finds 1 record with age 30');

  // $ne
  const ne = await users.find({ age: { $ne: 30 } }).execute();
  assertEqual(ne.length, 4, '$ne: finds 4 records with age != 30');

  // $gt
  const gt = await users.find({ age: { $gt: 30 } }).execute();
  assertEqual(gt.length, 1, '$gt: finds 1 record with age > 30');

  // $gte
  const gte = await users.find({ age: { $gte: 30 } }).execute();
  assertEqual(gte.length, 2, '$gte: finds 2 records with age >= 30');

  // $lt
  const lt = await users.find({ age: { $lt: 28 } }).execute();
  assertEqual(lt.length, 2, '$lt: finds 2 records with age < 28');

  // $lte
  const lte = await users.find({ age: { $lte: 28 } }).execute();
  assertEqual(lte.length, 3, '$lte: finds 3 records with age <= 28');

  // $in
  const inOp = await users.find({ name: { $in: ['Alice', 'Bob'] } }).execute();
  assertEqual(inOp.length, 2, '$in: finds 2 records');

  // $nin
  const nin = await users.find({ name: { $nin: ['Alice', 'Bob'] } }).execute();
  assertEqual(nin.length, 3, '$nin: finds 3 records');

  // $regex
  const regex = await users.find({ name: { $regex: '^[A-C]' } }).execute();
  assertEqual(regex.length, 3, '$regex: finds Alice, Bob, Charlie');

  // $exists
  const exists = await users.find({ active: { $exists: true } }).execute();
  assertEqual(exists.length, 4, '$exists true: finds 4 records with active field');

  const notExists = await users.find({ active: { $exists: false } }).execute();
  assertEqual(notExists.length, 1, '$exists false: finds 1 record without active field');

  await db.drop();
}

async function testCombinedOperators() {
  console.log('\n─── Combined Operators ($gt + $lt) ───');

  const db = new AmoraDB('test-combined', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 15 },
    { name: 'Bob', age: 25 },
    { name: 'Charlie', age: 35 },
    { name: 'Diana', age: 45 }
  ]);

  const range = await users.find({ age: { $gt: 20, $lt: 40 } }).execute();
  assertEqual(range.length, 2, '$gt + $lt: finds Bob (25) and Charlie (35)');
  const names = range.map(r => r.name).sort();
  assertEqual(names, ['Bob', 'Charlie'], '$gt + $lt: correct records returned');

  const range2 = await users.find({ age: { $gte: 25, $lte: 35 } }).execute();
  assertEqual(range2.length, 2, '$gte + $lte: inclusive range works');

  await db.drop();
}

async function testOrConditions() {
  console.log('\n─── OR Conditions ───');

  const db = new AmoraDB('test-or', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30, city: 'NYC' },
    { name: 'Bob', age: 25, city: 'LA' },
    { name: 'Charlie', age: 35, city: 'Chicago' }
  ]);

  const results = await users.find({ city: 'NYC' }).or({ city: 'LA' }).execute();
  assertEqual(results.length, 2, 'OR returns NYC and LA users');
  const names = results.map(r => r.name).sort();
  assertEqual(names, ['Alice', 'Bob'], 'OR returns correct records');

  await db.drop();
}

async function testAndConditions() {
  console.log('\n─── AND Conditions ───');

  const db = new AmoraDB('test-and', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30, city: 'NYC' },
    { name: 'Bob', age: 25, city: 'NYC' },
    { name: 'Charlie', age: 30, city: 'LA' }
  ]);

  const results = await users.find({ city: 'NYC' }).and({ age: 30 }).execute();
  assertEqual(results.length, 1, 'AND narrows to 1 result');
  assertEqual(results[0].name, 'Alice', 'AND returns correct record');

  await db.drop();
}

async function testFunctionConditions() {
  console.log('\n─── Function Conditions ───');

  const db = new AmoraDB('test-func-cond', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
    { name: 'Charlie', age: 35 }
  ]);

  const results = await users.find(r => r.age > 28).execute();
  assertEqual(results.length, 2, 'function condition finds 2 records');

  await db.drop();
}

// ─── Sort, Limit, Skip, Select ───────────────────────────────────────

async function testSortLimitSkipSelect() {
  console.log('\n─── Sort, Limit, Skip, Select ───');

  const db = new AmoraDB('test-sort', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Charlie', age: 35 },
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
    { name: 'Diana', age: 28 },
    { name: 'Eve', age: 22 }
  ]);

  // Sort ascending
  const asc = await users.all().sort('age', 'asc').execute();
  assertEqual(asc[0].name, 'Eve', 'sort asc: youngest first');
  assertEqual(asc[4].name, 'Charlie', 'sort asc: oldest last');

  // Sort descending
  const desc = await users.all().sort('age', 'desc').execute();
  assertEqual(desc[0].name, 'Charlie', 'sort desc: oldest first');

  // Limit
  const limited = await users.all().sort('age', 'asc').limit(2).execute();
  assertEqual(limited.length, 2, 'limit: returns 2 records');
  assertEqual(limited[0].name, 'Eve', 'limit: first record correct');

  // Skip
  const skipped = await users.all().sort('age', 'asc').skip(2).execute();
  assertEqual(skipped.length, 3, 'skip: returns 3 records');
  assertEqual(skipped[0].name, 'Diana', 'skip: first result is 3rd youngest');

  // Skip + Limit
  const page = await users.all().sort('age', 'asc').skip(1).limit(2).execute();
  assertEqual(page.length, 2, 'skip+limit: returns 2 records');
  assertEqual(page[0].name, 'Bob', 'skip+limit: correct first record');

  // Select
  const selected = await users.all().select(['name']).execute();
  assert(selected[0].name !== undefined, 'select: name included');
  assert(selected[0].age === undefined, 'select: age excluded');
  assert(selected[0]._id === undefined, 'select: _id excluded');

  await db.drop();
}

// ─── Update Operations ──────────────────────────────────────────────

async function testUpdateSingle() {
  console.log('\n─── Update Single ───');

  const db = new AmoraDB('test-update', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const alice = users.insert({ name: 'Alice', age: 30 });
  // Small delay so _modified timestamp differs
  await new Promise(r => setTimeout(r, 5));
  const updated = await users.update(alice._id, { age: 31 });

  assertEqual(updated.age, 31, 'update returns new age');
  assertEqual(updated.name, 'Alice', 'update preserves name');
  assertEqual(updated._id, alice._id, 'update preserves _id');
  assert(updated._modified !== alice._modified, '_modified timestamp changed');

  const fetched = await users.get(alice._id);
  assertEqual(fetched.age, 31, 'get() returns updated value');

  // Update non-existent
  const noUpdate = await users.update('nonexistent-id', { age: 99 });
  assertEqual(noUpdate, null, 'update non-existent returns null');

  await db.drop();
}

async function testUpdateMany() {
  console.log('\n─── Update Many ───');

  const db = new AmoraDB('test-update-many', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30, city: 'NYC' },
    { name: 'Bob', age: 25, city: 'NYC' },
    { name: 'Charlie', age: 35, city: 'LA' }
  ]);

  const updated = await users.updateMany({ city: 'NYC' }, { verified: true });
  assertEqual(updated.length, 2, 'updateMany returns 2 updated records');
  assert(updated.every(r => r.verified === true), 'all updated records have verified=true');

  const charlie = await users.findOne({ name: 'Charlie' });
  assertEqual(charlie.verified, undefined, 'non-matching record not updated');

  await db.drop();
}

// ─── Delete Operations ──────────────────────────────────────────────

async function testDeleteSingle() {
  console.log('\n─── Delete Single ───');

  const db = new AmoraDB('test-delete', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const alice = users.insert({ name: 'Alice' });
  users.insert({ name: 'Bob' });

  const result = await users.delete(alice._id);
  assertEqual(result, true, 'delete returns true');

  const found = await users.get(alice._id);
  assertEqual(found, null, 'deleted record returns null');

  const count = await users.count();
  assertEqual(count, 1, 'count is 1 after delete');

  // Delete non-existent
  const noDelete = await users.delete('nonexistent-id');
  assertEqual(noDelete, false, 'delete non-existent returns false');

  await db.drop();
}

async function testDeleteMany() {
  console.log('\n─── Delete Many ───');

  const db = new AmoraDB('test-delete-many', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', city: 'NYC' },
    { name: 'Bob', city: 'NYC' },
    { name: 'Charlie', city: 'LA' }
  ]);

  const deleted = await users.deleteMany({ city: 'NYC' });
  assertEqual(deleted.length, 2, 'deleteMany returns 2 deleted records');

  const count = await users.count();
  assertEqual(count, 1, 'count is 1 after deleteMany');

  const remaining = await users.findOne({ name: 'Charlie' });
  assert(remaining !== null, 'non-matching record still exists');

  await db.drop();
}

// ─── Count ──────────────────────────────────────────────────────────

async function testCount() {
  console.log('\n─── Count ───');

  const db = new AmoraDB('test-count', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  assertEqual(await users.count(), 0, 'empty table count is 0');

  users.insertMany([
    { name: 'Alice', city: 'NYC' },
    { name: 'Bob', city: 'LA' },
    { name: 'Charlie', city: 'NYC' }
  ]);

  assertEqual(await users.count(), 3, 'count is 3 after inserts');
  assertEqual(await users.count({ city: 'NYC' }), 2, 'conditional count is 2');

  await users.delete((await users.findOne({ name: 'Alice' }))._id);
  assertEqual(await users.count(), 2, 'count is 2 after delete');

  await db.drop();
}

// ─── Distinct ───────────────────────────────────────────────────────

async function testDistinct() {
  console.log('\n─── Distinct ───');

  const db = new AmoraDB('test-distinct', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', city: 'NYC' },
    { name: 'Bob', city: 'LA' },
    { name: 'Charlie', city: 'NYC' },
    { name: 'Diana', city: 'Chicago' }
  ]);

  const cities = await users.all().distinct('city');
  assertEqual(cities.sort(), ['Chicago', 'LA', 'NYC'], 'distinct returns unique cities');

  await db.drop();
}

// ─── Aggregation ────────────────────────────────────────────────────

async function testAggregation() {
  console.log('\n─── Aggregation ───');

  const db = new AmoraDB('test-agg', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', score: 90 },
    { name: 'Bob', score: 80 },
    { name: 'Charlie', score: 70 }
  ]);

  const agg = await users.all().aggregate({
    totalScore: { $sum: 'score' },
    avgScore: { $avg: 'score' },
    minScore: { $min: 'score' },
    maxScore: { $max: 'score' },
    count: { $count: true }
  });

  assertEqual(agg.totalScore, 240, '$sum: 240');
  assertEqual(agg.avgScore, 80, '$avg: 80');
  assertEqual(agg.minScore, 70, '$min: 70');
  assertEqual(agg.maxScore, 90, '$max: 90');
  assertEqual(agg.count, 3, '$count: 3');

  await db.drop();
}

async function testAggregationFalsyValues() {
  console.log('\n─── Aggregation with Falsy Values ───');

  const db = new AmoraDB('test-agg-falsy', { dataPath: TEST_DB_PATH });
  await db.init();
  const items = db.table('items');

  items.insertMany([
    { name: 'A', value: 0 },
    { name: 'B', value: 5 },
    { name: 'C', value: 10 }
  ]);

  const agg = await items.all().aggregate({
    total: { $sum: 'value' },
    min: { $min: 'value' },
    max: { $max: 'value' },
    avg: { $avg: 'value' }
  });

  assertEqual(agg.min, 0, '$min treats 0 correctly (not as missing)');
  assertEqual(agg.max, 10, '$max correct');
  assertEqual(agg.total, 15, '$sum treats 0 correctly');
  assert(Math.abs(agg.avg - 5) < 0.001, '$avg treats 0 correctly');

  await db.drop();
}

// ─── Indexing ───────────────────────────────────────────────────────

async function testIndexCreation() {
  console.log('\n─── Index Creation ───');

  const db = new AmoraDB('test-index', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 },
    { name: 'Charlie', age: 35 }
  ]);
  await users.flush();

  await users.createIndex('name');
  assert(users.indexManager.hasIndex('name'), 'index exists on name');

  // Query using index
  const result = await users.find({ name: 'Alice' }).execute();
  assertEqual(result.length, 1, 'indexed query returns correct result');
  assertEqual(result[0].name, 'Alice', 'indexed query returns correct record');

  await db.drop();
}

async function testIndexWithRangeQueries() {
  console.log('\n─── Index with Range Queries ───');

  const db = new AmoraDB('test-index-range', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 20 },
    { name: 'Bob', age: 25 },
    { name: 'Charlie', age: 30 },
    { name: 'Diana', age: 35 },
    { name: 'Eve', age: 40 }
  ]);
  await users.flush();

  await users.createIndex('age');
  assert(users.indexManager.hasIndex('age'), 'age index created');

  const range = await users.find({ age: { $gte: 25, $lte: 35 } }).execute();
  assertEqual(range.length, 3, 'range query returns 3 results');
  const names = range.map(r => r.name).sort();
  assertEqual(names, ['Bob', 'Charlie', 'Diana'], 'range query returns correct records');

  await db.drop();
}

async function testDropIndex() {
  console.log('\n─── Drop Index ───');

  const db = new AmoraDB('test-drop-index', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insert({ name: 'Alice', age: 30 });
  await users.flush();
  await users.createIndex('name');

  assert(users.indexManager.hasIndex('name'), 'index exists before drop');
  users.dropIndex('name');
  assert(!users.indexManager.hasIndex('name'), 'index removed after drop');

  // Queries still work without index
  const result = await users.find({ name: 'Alice' }).execute();
  assertEqual(result.length, 1, 'query works after dropping index');

  await db.drop();
}

async function testIndexPendingWrites() {
  console.log('\n─── Index Queries Include Pending Writes ───');

  const db = new AmoraDB('test-index-pending', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insert({ name: 'Alice', age: 30 });
  await users.flush();
  await users.createIndex('name');

  // Insert without flushing
  users.insert({ name: 'Bob', age: 25 });

  const results = await users.find({ name: 'Bob' }).execute();
  assertEqual(results.length, 1, 'indexed query finds unflushed record');
  assertEqual(results[0].name, 'Bob', 'unflushed record has correct name');

  await db.drop();
}

async function testMetadataIndicesSync() {
  console.log('\n─── Metadata Indices Sync ───');

  const db = new AmoraDB('test-meta-idx', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insert({ name: 'Alice', age: 30 });
  await users.flush();
  await users.createIndex('name');
  await users.createIndex('age');

  await db.close();

  // Read database-level metadata
  const metaPath = path.join(TEST_DB_PATH, 'test-meta-idx', '_metadata.json');
  const metaContent = await fs.readFile(metaPath, 'utf-8');
  const metadata = JSON.parse(metaContent);

  assert(metadata.tables.users.indices.includes('name'), 'name index in database metadata');
  assert(metadata.tables.users.indices.includes('age'), 'age index in database metadata');

  await fs.rm(path.join(TEST_DB_PATH, 'test-meta-idx'), { recursive: true, force: true });
}

// ─── Persistence ────────────────────────────────────────────────────

async function testDataPersistence() {
  console.log('\n─── Data Persistence ───');

  const db = new AmoraDB('test-persist', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice', age: 30 },
    { name: 'Bob', age: 25 }
  ]);
  await db.close();

  const db2 = new AmoraDB('test-persist', { dataPath: TEST_DB_PATH });
  await db2.init();
  const users2 = db2.table('users');

  const count = await users2.count();
  assertEqual(count, 2, 'records persist across restart');

  const alice = await users2.findOne({ name: 'Alice' });
  assert(alice !== null, 'Alice found after restart');
  assertEqual(alice.age, 30, 'Alice age correct after restart');

  await db2.drop();
}

async function testUpdatePersistence() {
  console.log('\n─── Update Persistence ───');

  const db = new AmoraDB('test-update-persist', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const alice = users.insert({ name: 'Alice', age: 30 });
  await users.flush();
  await users.update(alice._id, { age: 31 });
  await db.close();

  const db2 = new AmoraDB('test-update-persist', { dataPath: TEST_DB_PATH });
  await db2.init();
  const found = await db2.table('users').findOne({ name: 'Alice' });

  assertEqual(found.age, 31, 'updated value persists after close+reopen');

  await db2.drop();
}

async function testDeletePersistence() {
  console.log('\n─── Delete Persistence ───');

  const db = new AmoraDB('test-delete-persist', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const alice = users.insert({ name: 'Alice' });
  users.insert({ name: 'Bob' });
  await users.flush();
  await users.delete(alice._id);
  await db.close();

  const db2 = new AmoraDB('test-delete-persist', { dataPath: TEST_DB_PATH });
  await db2.init();
  const count = await db2.table('users').count();
  assertEqual(count, 1, 'delete persists after close+reopen');

  const found = await db2.table('users').findOne({ name: 'Alice' });
  assertEqual(found, null, 'deleted record not found after restart');

  await db2.drop();
}

// ─── Truncate ───────────────────────────────────────────────────────

async function testTruncate() {
  console.log('\n─── Truncate ───');

  const db = new AmoraDB('test-truncate', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice' },
    { name: 'Bob' },
    { name: 'Charlie' }
  ]);
  await users.flush();

  await users.truncate();
  const count = await users.count();
  assertEqual(count, 0, 'count is 0 after truncate');

  // Can still insert after truncate
  users.insert({ name: 'Diana' });
  assertEqual(await users.count(), 1, 'can insert after truncate');

  await db.drop();
}

// ─── Cache ──────────────────────────────────────────────────────────

async function testCache() {
  console.log('\n─── Cache ───');

  const db = new AmoraDB('test-cache', { dataPath: TEST_DB_PATH, cacheSize: 5 });
  await db.init();
  const users = db.table('users');

  for (let i = 0; i < 10; i++) {
    users.insert({ name: `User${i}`, age: 20 + i });
  }
  await users.flush();

  // Access a record to put it in cache
  const user5 = await users.findOne({ name: 'User5' });
  assert(user5 !== null, 'can find User5');

  const stats = users.cache.getStats();
  assert(stats.maxSize === 5, 'cache maxSize is 5');
  assert(stats.size <= 5, 'cache size does not exceed max');

  await db.drop();
}

// ─── Events ─────────────────────────────────────────────────────────

async function testEvents() {
  console.log('\n─── Events ───');

  const db = new AmoraDB('test-events', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  let insertEvent = null;
  let updateEvent = null;
  let deleteEvent = null;

  users.on('insert', (record) => { insertEvent = record; });
  users.on('update', (record) => { updateEvent = record; });
  users.on('delete', (record) => { deleteEvent = record; });

  const alice = users.insert({ name: 'Alice', age: 30 });
  assert(insertEvent !== null, 'insert event fired');
  assertEqual(insertEvent.name, 'Alice', 'insert event has correct data');

  await users.update(alice._id, { age: 31 });
  assert(updateEvent !== null, 'update event fired');
  assertEqual(updateEvent.age, 31, 'update event has new data');

  await users.delete(alice._id);
  assert(deleteEvent !== null, 'delete event fired');
  assertEqual(deleteEvent.name, 'Alice', 'delete event has correct record');

  await db.drop();
}

// ─── Edge Cases ─────────────────────────────────────────────────────

async function testEmptyQueries() {
  console.log('\n─── Empty Queries ───');

  const db = new AmoraDB('test-empty', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  const results = await users.all().execute();
  assertEqual(results.length, 0, 'all() on empty table returns empty array');

  const first = await users.all().first();
  assertEqual(first, null, 'first() on empty table returns null');

  const count = await users.all().count();
  assertEqual(count, 0, 'count() on empty table returns 0');

  await db.drop();
}

async function testAllReturnedFromAll() {
  console.log('\n─── all() Returns All Records ───');

  const db = new AmoraDB('test-all', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insertMany([
    { name: 'Alice' },
    { name: 'Bob' },
    { name: 'Charlie' }
  ]);

  const results = await users.all().execute();
  assertEqual(results.length, 3, 'all() returns all 3 records');

  await db.drop();
}

async function testNestedFieldAccess() {
  console.log('\n─── Nested Field Access ───');

  const db = new AmoraDB('test-nested', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insert({ name: 'Alice', address: { city: 'NYC', zip: '10001' } });
  users.insert({ name: 'Bob', address: { city: 'LA', zip: '90001' } });

  // Distinct on nested field
  const cities = await users.all().distinct('address.city');
  assertEqual(cities.sort(), ['LA', 'NYC'], 'distinct works on nested fields');

  await db.drop();
}

async function testCountAfterFlush() {
  console.log('\n─── Count Accuracy After Flush ───');

  const db = new AmoraDB('test-count-flush', { dataPath: TEST_DB_PATH });
  await db.init();
  const users = db.table('users');

  users.insert({ name: 'Alice' });
  users.insert({ name: 'Bob' });
  assertEqual(await users.count(), 2, 'count is 2 before flush');

  await users.flush();
  assertEqual(await users.count(), 2, 'count is still 2 after flush');

  users.insert({ name: 'Charlie' });
  assertEqual(await users.count(), 3, 'count is 3 with 1 pending + 2 flushed');

  await db.drop();
}

// ─── Run All Tests ──────────────────────────────────────────────────

async function runAll() {
  console.log('\n\x1b[1m═══ AmoraDB Test Suite ═══\x1b[0m');

  await cleanup();

  try {
    // Database lifecycle
    await testDatabaseInit();
    await testDatabaseInitError();
    await testDatabaseClose();
    await testDatabaseDrop();
    await testDatabaseBackup();
    await testDatabaseStats();
    await testListAndDropTable();

    // Insert
    await testInsertSingle();
    await testInsertMany();
    await testInsertWithCustomId();

    // Query
    await testFindAndFindOne();
    await testQueryOperators();
    await testCombinedOperators();
    await testOrConditions();
    await testAndConditions();
    await testFunctionConditions();

    // Sort, limit, skip, select
    await testSortLimitSkipSelect();

    // Update
    await testUpdateSingle();
    await testUpdateMany();

    // Delete
    await testDeleteSingle();
    await testDeleteMany();

    // Count
    await testCount();

    // Distinct
    await testDistinct();

    // Aggregation
    await testAggregation();
    await testAggregationFalsyValues();

    // Indexing
    await testIndexCreation();
    await testIndexWithRangeQueries();
    await testDropIndex();
    await testIndexPendingWrites();
    await testMetadataIndicesSync();

    // Persistence
    await testDataPersistence();
    await testUpdatePersistence();
    await testDeletePersistence();

    // Truncate
    await testTruncate();

    // Cache
    await testCache();

    // Events
    await testEvents();

    // Edge cases
    await testEmptyQueries();
    await testAllReturnedFromAll();
    await testNestedFieldAccess();
    await testCountAfterFlush();
  } catch (err) {
    console.error('\n\x1b[31mTest crashed:\x1b[0m', err);
    failed++;
  }

  await cleanup();

  console.log(`\n\x1b[1m═══ Results ═══\x1b[0m`);
  console.log(`  Total:  ${total}`);
  console.log(`  \x1b[32mPassed: ${passed}\x1b[0m`);
  if (failed > 0) {
    console.log(`  \x1b[31mFailed: ${failed}\x1b[0m`);
  } else {
    console.log(`  Failed: 0`);
  }
  console.log('');

  process.exit(failed > 0 ? 1 : 0);
}

runAll();
