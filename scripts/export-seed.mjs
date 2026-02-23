#!/usr/bin/env node
// Export PlainDoctor SQLite → chunked SQL seed files for D1
// Usage: node scripts/export-seed.mjs
// Input: /storage/plaindoctor/plaindoctor.db
// Output: /storage/plaindoctor/seed/*.sql

import Database from 'better-sqlite3';
import { existsSync, mkdirSync, writeFileSync, rmSync } from 'fs';

const DB_PATH = '/storage/plaindoctor/plaindoctor.db';
const SEED_DIR = '/storage/plaindoctor/seed';
const SMALL_CHUNK = 1000;
const LARGE_CHUNK = 2000; // Larger chunks to reduce file count

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

function getCreateTable(tableName) {
  const row = db.prepare(`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`).get(tableName);
  return row ? row.sql : null;
}

// For small tables — loads all rows at once
function exportSmallTable(tableName, orderBy, filePrefix) {
  const rows = db.prepare(`SELECT * FROM ${tableName} ORDER BY ${orderBy}`).all();
  if (rows.length === 0) {
    console.log(`  ${tableName}: 0 rows, skipping`);
    return 0;
  }

  const columns = Object.keys(rows[0]);
  let fileNum = 0;

  for (let i = 0; i < rows.length; i += SMALL_CHUNK) {
    fileNum++;
    const chunk = rows.slice(i, i + SMALL_CHUNK);
    const pad = String(fileNum).padStart(4, '0');

    let sql = '';
    if (i === 0) {
      const createSql = getCreateTable(tableName);
      if (createSql) {
        sql += `DROP TABLE IF EXISTS ${tableName};\n${createSql};\n\n`;
      }
    }

    const values = chunk.map(row => {
      return `(${columns.map(col => escapeSQL(row[col])).join(',')})`;
    }).join(',\n');

    sql += `INSERT INTO ${tableName} (${columns.join(',')}) VALUES\n${values};\n`;
    writeFileSync(`${SEED_DIR}/${filePrefix}_${pad}.sql`, sql);
  }

  console.log(`  ${tableName}: ${rows.length.toLocaleString()} rows → ${fileNum} files`);
  return fileNum;
}

// For large tables — uses rowid-based cursor (O(1) per page, not O(n) like OFFSET)
function exportLargeTable(tableName, filePrefix) {
  const firstRow = db.prepare(`SELECT * FROM ${tableName} LIMIT 1`).get();
  if (!firstRow) {
    console.log(`  ${tableName}: 0 rows, skipping`);
    return 0;
  }

  const columns = Object.keys(firstRow);
  const totalCount = db.prepare(`SELECT COUNT(*) as cnt FROM ${tableName}`).get().cnt;
  const createSql = getCreateTable(tableName);

  // Use rowid-based cursor for fast pagination
  const stmt = db.prepare(`SELECT * FROM ${tableName} WHERE rowid > ? ORDER BY rowid LIMIT ?`);

  let fileNum = 0;
  let lastRowid = 0;
  let exported = 0;

  while (exported < totalCount) {
    // Get chunk using rowid cursor
    const chunk = stmt.all(lastRowid, LARGE_CHUNK);
    if (chunk.length === 0) break;

    fileNum++;
    const pad = String(fileNum).padStart(5, '0');

    let sql = '';
    if (fileNum === 1 && createSql) {
      sql += `DROP TABLE IF EXISTS ${tableName};\n${createSql};\n\n`;
    }

    const values = chunk.map(row => {
      return `(${columns.map(col => escapeSQL(row[col])).join(',')})`;
    }).join(',\n');

    sql += `INSERT INTO ${tableName} (${columns.join(',')}) VALUES\n${values};\n`;
    writeFileSync(`${SEED_DIR}/${filePrefix}_${pad}.sql`, sql);

    // Get the last rowid from this chunk
    const lastRow = chunk[chunk.length - 1];
    const lastRowidRow = db.prepare(`SELECT rowid FROM ${tableName} WHERE npi = ?`).get(lastRow.npi);
    lastRowid = lastRowidRow?.rowid || lastRowid + LARGE_CHUNK;

    exported += chunk.length;
    if (fileNum % 500 === 0) {
      console.log(`    ${exported.toLocaleString()} / ${totalCount.toLocaleString()} exported...`);
    }
  }

  console.log(`  ${tableName}: ${exported.toLocaleString()} rows → ${fileNum} files`);
  return fileNum;
}

console.log('Exporting PlainDoctor seed files...\n');

let totalFiles = 0;

// Small aggregation tables
totalFiles += exportSmallTable('specialties', 'provider_count DESC', '01_specialties');
totalFiles += exportSmallTable('states', 'name COLLATE NOCASE', '02_states');
totalFiles += exportSmallTable('cities', 'provider_count DESC', '03_cities');
totalFiles += exportSmallTable('specialty_state', 'provider_count DESC', '04_specialty_state');

// Large providers table — rowid cursor
totalFiles += exportLargeTable('providers', '05_providers');

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
