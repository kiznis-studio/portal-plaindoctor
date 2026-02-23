#!/usr/bin/env node
// Export PlainDoctor SQLite → chunked SQL seed files for D1
// Usage: node scripts/export-seed.mjs
// Input: /storage/plaindoctor/plaindoctor.db
// Output: /storage/plaindoctor/seed/*.sql

import Database from 'better-sqlite3';
import { existsSync, mkdirSync, writeFileSync, rmSync } from 'fs';

const DB_PATH = '/storage/plaindoctor/plaindoctor.db';
const SEED_DIR = '/storage/plaindoctor/seed';
const CHUNK_SIZE = 1000;

if (!existsSync(DB_PATH)) {
  console.error('Database not found:', DB_PATH);
  process.exit(1);
}

// Clean seed directory
if (existsSync(SEED_DIR)) rmSync(SEED_DIR, { recursive: true });
mkdirSync(SEED_DIR, { recursive: true });

const db = new Database(DB_PATH, { readonly: true });

function escapeSQL(val) {
  if (val === null || val === undefined) return 'NULL';
  return `'${String(val).replace(/'/g, "''")}'`;
}

// For small tables — loads all rows at once
function exportSmallTable(tableName, orderBy, filePrefix, chunkSize = CHUNK_SIZE) {
  const rows = db.prepare(`SELECT * FROM ${tableName} ORDER BY ${orderBy}`).all();
  const columns = Object.keys(rows[0] || {});

  if (rows.length === 0) {
    console.log(`  ${tableName}: 0 rows, skipping`);
    return 0;
  }

  let fileNum = 0;
  for (let i = 0; i < rows.length; i += chunkSize) {
    fileNum++;
    const chunk = rows.slice(i, i + chunkSize);
    const pad = String(fileNum).padStart(4, '0');
    const fileName = `${filePrefix}_${pad}.sql`;

    let sql = '';
    if (i === 0) {
      const createSql = db.prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`).get(tableName);
      if (createSql) {
        sql += `DROP TABLE IF EXISTS ${tableName};\n`;
        sql += createSql.sql + ';\n\n';
      }
    }

    const values = chunk.map(row => {
      const vals = columns.map(col => escapeSQL(row[col])).join(',');
      return `(${vals})`;
    }).join(',\n');

    sql += `INSERT INTO ${tableName} (${columns.join(',')}) VALUES\n${values};\n`;
    writeFileSync(`${SEED_DIR}/${fileName}`, sql);
  }

  console.log(`  ${tableName}: ${rows.length.toLocaleString()} rows → ${fileNum} files`);
  return fileNum;
}

// For large tables — streams with LIMIT/OFFSET to avoid OOM
function exportLargeTable(tableName, orderBy, filePrefix, chunkSize = CHUNK_SIZE) {
  // Get columns from first row
  const firstRow = db.prepare(`SELECT * FROM ${tableName} LIMIT 1`).get();
  if (!firstRow) {
    console.log(`  ${tableName}: 0 rows, skipping`);
    return 0;
  }
  const columns = Object.keys(firstRow);
  const totalCount = db.prepare(`SELECT COUNT(*) as cnt FROM ${tableName}`).get().cnt;

  // Get CREATE TABLE statement
  const createSql = db.prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`).get(tableName);

  const stmt = db.prepare(`SELECT * FROM ${tableName} ORDER BY ${orderBy} LIMIT ? OFFSET ?`);
  let fileNum = 0;
  let offset = 0;

  while (offset < totalCount) {
    fileNum++;
    const chunk = stmt.all(chunkSize, offset);
    if (chunk.length === 0) break;

    const pad = String(fileNum).padStart(5, '0');
    const fileName = `${filePrefix}_${pad}.sql`;

    let sql = '';
    if (offset === 0 && createSql) {
      sql += `DROP TABLE IF EXISTS ${tableName};\n`;
      sql += createSql.sql + ';\n\n';
    }

    const values = chunk.map(row => {
      const vals = columns.map(col => escapeSQL(row[col])).join(',');
      return `(${vals})`;
    }).join(',\n');

    sql += `INSERT INTO ${tableName} (${columns.join(',')}) VALUES\n${values};\n`;
    writeFileSync(`${SEED_DIR}/${fileName}`, sql);
    offset += chunkSize;

    if (fileNum % 500 === 0) {
      console.log(`    ${tableName}: ${offset.toLocaleString()} / ${totalCount.toLocaleString()} exported...`);
    }
  }

  console.log(`  ${tableName}: ${totalCount.toLocaleString()} rows → ${fileNum} files`);
  return fileNum;
}

console.log('Exporting PlainDoctor seed files...\n');

let totalFiles = 0;

// Small aggregation tables — safe to load all at once
totalFiles += exportSmallTable('specialties', 'provider_count DESC', '01_specialties');
totalFiles += exportSmallTable('states', 'name COLLATE NOCASE', '02_states');
totalFiles += exportSmallTable('cities', 'provider_count DESC', '03_cities');
totalFiles += exportSmallTable('specialty_state', 'provider_count DESC', '04_specialty_state');

// Large providers table — stream with LIMIT/OFFSET
totalFiles += exportLargeTable('providers', 'state, last_name COLLATE NOCASE', '05_providers');

// Create indices file
const indices = [
  'CREATE INDEX IF NOT EXISTS idx_providers_state ON providers(state)',
  'CREATE INDEX IF NOT EXISTS idx_providers_specialty ON providers(specialty_code)',
  'CREATE INDEX IF NOT EXISTS idx_providers_city_state ON providers(city, state)',
  'CREATE INDEX IF NOT EXISTS idx_providers_last_name ON providers(last_name COLLATE NOCASE)',
  'CREATE INDEX IF NOT EXISTS idx_providers_slug ON providers(slug)',
  'CREATE INDEX IF NOT EXISTS idx_specialties_slug ON specialties(slug)',
  'CREATE INDEX IF NOT EXISTS idx_states_slug ON states(slug)',
  'CREATE INDEX IF NOT EXISTS idx_cities_state ON cities(state)',
  'CREATE INDEX IF NOT EXISTS idx_cities_slug ON cities(slug)',
  'CREATE INDEX IF NOT EXISTS idx_specialty_state_spec ON specialty_state(specialty_code)',
  'CREATE INDEX IF NOT EXISTS idx_specialty_state_state ON specialty_state(state)',
];
writeFileSync(`${SEED_DIR}/99_indices.sql`, indices.join(';\n') + ';\n');
totalFiles++;

db.close();

console.log(`\nTotal: ${totalFiles} seed files in ${SEED_DIR}`);
console.log('Done!');
