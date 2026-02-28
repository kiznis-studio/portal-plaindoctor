#!/usr/bin/env node
// Build PlainDoctor SQLite database from NPPES CSV + NUCC taxonomy
// Usage: node scripts/build-db.mjs
// Input: /storage/plaindoctor/raw/npidata_*.csv, /storage/plaindoctor/raw/nucc_taxonomy.csv
// Output: /storage/plaindoctor/plaindoctor.db

import Database from 'better-sqlite3';
import { createReadStream, existsSync, readdirSync, readFileSync, unlinkSync, statSync } from 'fs';
import { createInterface } from 'readline';
import { join } from 'path';

const RAW_DIR = '/storage/plaindoctor/raw';
const DB_PATH = '/storage/plaindoctor/plaindoctor.db';

// --- Slug helper ---
function slugify(str) {
  return str.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

// --- Load NUCC taxonomy ---
function loadTaxonomy() {
  const taxPath = join(RAW_DIR, 'nucc_taxonomy.csv');
  if (!existsSync(taxPath)) {
    console.error('NUCC taxonomy file not found:', taxPath);
    process.exit(1);
  }
  const data = readFileSync(taxPath, 'utf8');
  const lines = data.split('\n');
  const taxonomy = new Map();

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;
    const fields = parseCSVLine(line);
    const code = fields[0]?.trim();
    const classification = fields[2]?.trim() || '';
    const specialization = fields[3]?.trim() || '';
    const displayName = fields[6]?.trim() || '';
    const section = fields[7]?.trim() || '';

    if (code && section === 'Individual') {
      const name = specialization
        ? `${classification} - ${specialization}`
        : classification;
      taxonomy.set(code, {
        name: displayName || name,
        category: classification,
      });
    }
  }
  console.log(`Loaded ${taxonomy.size} individual taxonomy codes`);
  return taxonomy;
}

// --- Simple CSV line parser (handles quoted fields) ---
function parseCSVLine(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && i + 1 < line.length && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      fields.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  fields.push(current);
  return fields;
}

// --- Find NPPES CSV file ---
function findNppesFile() {
  const files = readdirSync(RAW_DIR).filter(f =>
    f.startsWith('npidata_') && f.endsWith('.csv') && !f.includes('fileheader')
  );
  if (files.length === 0) {
    console.error('No npidata_*.csv file found in', RAW_DIR);
    console.error('Available files:', readdirSync(RAW_DIR));
    process.exit(1);
  }
  const sorted = files.sort().reverse();
  return join(RAW_DIR, sorted[0]);
}

// --- State validation ---
const VALID_STATES = new Set([
  'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
  'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
  'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
  'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
  'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
  'DC', 'PR', 'GU', 'VI', 'AS', 'MP',
]);

const STATE_NAMES = {
  AL: 'Alabama', AK: 'Alaska', AZ: 'Arizona', AR: 'Arkansas', CA: 'California',
  CO: 'Colorado', CT: 'Connecticut', DE: 'Delaware', FL: 'Florida', GA: 'Georgia',
  HI: 'Hawaii', ID: 'Idaho', IL: 'Illinois', IN: 'Indiana', IA: 'Iowa',
  KS: 'Kansas', KY: 'Kentucky', LA: 'Louisiana', ME: 'Maine', MD: 'Maryland',
  MA: 'Massachusetts', MI: 'Michigan', MN: 'Minnesota', MS: 'Mississippi', MO: 'Missouri',
  MT: 'Montana', NE: 'Nebraska', NV: 'Nevada', NH: 'New Hampshire', NJ: 'New Jersey',
  NM: 'New Mexico', NY: 'New York', NC: 'North Carolina', ND: 'North Dakota', OH: 'Ohio',
  OK: 'Oklahoma', OR: 'Oregon', PA: 'Pennsylvania', RI: 'Rhode Island', SC: 'South Carolina',
  SD: 'South Dakota', TN: 'Tennessee', TX: 'Texas', UT: 'Utah', VT: 'Vermont',
  VA: 'Virginia', WA: 'Washington', WV: 'West Virginia', WI: 'Wisconsin', WY: 'Wyoming',
  DC: 'District of Columbia', PR: 'Puerto Rico', GU: 'Guam', VI: 'Virgin Islands',
  AS: 'American Samoa', MP: 'Northern Mariana Islands',
};

async function main() {
  const taxonomy = loadTaxonomy();
  const nppesFile = findNppesFile();
  console.log('Processing:', nppesFile);

  // Delete existing DB
  if (existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log('Deleted existing database');
  }

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = OFF');

  // Create tables (using run for individual statements)
  const createStatements = [
    `CREATE TABLE providers (
      npi TEXT PRIMARY KEY,
      first_name TEXT NOT NULL,
      last_name TEXT NOT NULL,
      credential TEXT,
      gender TEXT,
      specialty TEXT NOT NULL,
      specialty_code TEXT NOT NULL,
      city TEXT NOT NULL,
      state TEXT NOT NULL,
      zip TEXT NOT NULL,
      phone TEXT,
      address_line1 TEXT,
      enumeration_date TEXT,
      slug TEXT NOT NULL
    )`,
    `CREATE TABLE specialties (
      code TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      category TEXT,
      slug TEXT NOT NULL,
      provider_count INTEGER DEFAULT 0
    )`,
    `CREATE TABLE states (
      abbr TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      slug TEXT NOT NULL,
      provider_count INTEGER DEFAULT 0,
      specialty_count INTEGER DEFAULT 0
    )`,
    `CREATE TABLE cities (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      city TEXT NOT NULL,
      state TEXT NOT NULL,
      slug TEXT NOT NULL,
      provider_count INTEGER DEFAULT 0
    )`,
    `CREATE TABLE specialty_state (
      specialty_code TEXT NOT NULL,
      state TEXT NOT NULL,
      provider_count INTEGER DEFAULT 0,
      PRIMARY KEY (specialty_code, state)
    )`,
  ];

  for (const sql of createStatements) {
    db.prepare(sql).run();
  }

  // Prepare insert statement
  const insertProvider = db.prepare(`
    INSERT OR IGNORE INTO providers (npi, first_name, last_name, credential, gender, specialty, specialty_code, city, state, zip, phone, address_line1, enumeration_date, slug)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  // Read CSV line by line
  const rl = createInterface({
    input: createReadStream(nppesFile, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });

  let lineNum = 0;
  let headerIndices = {};
  let inserted = 0;
  let skipped = 0;
  let batchProviders = [];
  const BATCH_SIZE = 5000;
  const slugSeen = new Map();

  const insertBatch = db.transaction((providers) => {
    for (const p of providers) {
      insertProvider.run(p.npi, p.first_name, p.last_name, p.credential, p.gender,
        p.specialty, p.specialty_code, p.city, p.state, p.zip, p.phone,
        p.address_line1, p.enumeration_date, p.slug);
    }
  });

  for await (const line of rl) {
    lineNum++;

    if (lineNum === 1) {
      const headers = parseCSVLine(line);
      for (let i = 0; i < headers.length; i++) {
        const h = headers[i].trim().replace(/"/g, '');
        headerIndices[h] = i;
      }
      console.log(`Found ${headers.length} columns`);
      const needed = ['NPI', 'Entity Type Code', 'Provider First Name', 'Provider Last Name (Legal Name)'];
      for (const n of needed) {
        if (!(n in headerIndices)) {
          console.error(`Missing column: ${n}`);
          process.exit(1);
        }
      }
      continue;
    }

    const fields = parseCSVLine(line);
    const get = (col) => (fields[headerIndices[col]] || '').trim().replace(/"/g, '');

    // Filter: Entity Type 1 (Individual only)
    if (get('Entity Type Code') !== '1') { skipped++; continue; }

    // Filter: Active NPI
    const deactivDate = get('NPI Deactivation Date');
    const reactivDate = get('NPI Reactivation Date');
    if (deactivDate && !reactivDate) { skipped++; continue; }

    // Filter: US practice address
    const state = get('Provider Business Practice Location Address State Name');
    if (!VALID_STATES.has(state)) { skipped++; continue; }

    const city = get('Provider Business Practice Location Address City Name');
    if (!city) { skipped++; continue; }

    // Get taxonomy/specialty
    const taxCode = get('Healthcare Provider Taxonomy Code_1');
    const taxInfo = taxonomy.get(taxCode);
    if (!taxInfo) { skipped++; continue; }

    const npi = get('NPI');
    const firstName = get('Provider First Name');
    const lastName = get('Provider Last Name (Legal Name)');
    if (!npi || !firstName || !lastName) { skipped++; continue; }

    const credential = get('Provider Credential Text') || null;
    const gender = get('Provider Sex Code') || null;
    const zip = (get('Provider Business Practice Location Address Postal Code') || '').slice(0, 5);
    const phone = get('Provider Business Practice Location Address Telephone Number') || null;
    const address = get('Provider First Line Business Practice Location Address') || null;
    const enumDate = get('Provider Enumeration Date') || null;

    // Generate unique slug
    let baseSlug = slugify(`${firstName}-${lastName}-${npi.slice(-4)}`);
    let slug = baseSlug;
    let suffix = 1;
    while (slugSeen.has(slug)) {
      slug = `${baseSlug}-${suffix}`;
      suffix++;
    }
    slugSeen.set(slug, true);

    batchProviders.push({
      npi, first_name: firstName, last_name: lastName, credential, gender,
      specialty: taxInfo.name, specialty_code: taxCode,
      city, state, zip, phone, address_line1: address,
      enumeration_date: enumDate, slug,
    });

    if (batchProviders.length >= BATCH_SIZE) {
      insertBatch(batchProviders);
      inserted += batchProviders.length;
      batchProviders = [];
      if (inserted % 100000 === 0) {
        console.log(`  ${inserted.toLocaleString()} providers inserted, ${skipped.toLocaleString()} skipped...`);
      }
    }
  }

  if (batchProviders.length > 0) {
    insertBatch(batchProviders);
    inserted += batchProviders.length;
  }

  console.log(`\nProviders: ${inserted.toLocaleString()} inserted, ${skipped.toLocaleString()} skipped`);

  // --- Build aggregation tables ---
  console.log('\nBuilding specialties table...');
  db.prepare(`
    INSERT INTO specialties (code, name, category, slug, provider_count)
    SELECT specialty_code, specialty, NULL, '', COUNT(*) as cnt
    FROM providers
    GROUP BY specialty_code
    ORDER BY cnt DESC
  `).run();

  // Update specialty names and slugs from taxonomy
  const updateSpecialty = db.prepare('UPDATE specialties SET name = ?, category = ?, slug = ? WHERE code = ?');
  const specRows = db.prepare('SELECT code FROM specialties').all();
  const updateSpecBatch = db.transaction((rows) => {
    for (const row of rows) {
      const info = taxonomy.get(row.code);
      if (info) {
        updateSpecialty.run(info.name, info.category, slugify(info.name), row.code);
      } else {
        const prov = db.prepare('SELECT specialty FROM providers WHERE specialty_code = ? LIMIT 1').get(row.code);
        if (prov) {
          updateSpecialty.run(prov.specialty, null, slugify(prov.specialty), row.code);
        }
      }
    }
  });
  updateSpecBatch(specRows);

  // Handle duplicate specialty slugs
  const dupSlugs = db.prepare(`
    SELECT slug, COUNT(*) as cnt FROM specialties GROUP BY slug HAVING cnt > 1
  `).all();
  for (const dup of dupSlugs) {
    const specs = db.prepare('SELECT code, name FROM specialties WHERE slug = ? ORDER BY provider_count DESC').all(dup.slug);
    for (let i = 1; i < specs.length; i++) {
      const newSlug = `${dup.slug}-${specs[i].code.slice(0, 6).toLowerCase()}`;
      db.prepare('UPDATE specialties SET slug = ? WHERE code = ?').run(newSlug, specs[i].code);
    }
  }

  const specCount = db.prepare('SELECT COUNT(*) as cnt FROM specialties').get();
  console.log(`  ${specCount.cnt} specialties`);

  console.log('Building states table...');
  db.prepare(`
    INSERT INTO states (abbr, name, slug, provider_count, specialty_count)
    SELECT state, '', '', COUNT(*) as cnt, COUNT(DISTINCT specialty_code) as scnt
    FROM providers
    GROUP BY state
    ORDER BY cnt DESC
  `).run();

  const updateState = db.prepare('UPDATE states SET name = ?, slug = ? WHERE abbr = ?');
  const stateRows = db.prepare('SELECT abbr FROM states').all();
  const updateStateBatch = db.transaction((rows) => {
    for (const row of rows) {
      const name = STATE_NAMES[row.abbr] || row.abbr;
      updateState.run(name, slugify(name), row.abbr);
    }
  });
  updateStateBatch(stateRows);

  const stateCount = db.prepare('SELECT COUNT(*) as cnt FROM states').get();
  console.log(`  ${stateCount.cnt} states`);

  console.log('Building cities table...');
  db.prepare(`
    INSERT INTO cities (city, state, slug, provider_count)
    SELECT city, state, '', COUNT(*) as cnt
    FROM providers
    GROUP BY city, state
    HAVING cnt >= 10
    ORDER BY cnt DESC
  `).run();

  const updateCity = db.prepare('UPDATE cities SET slug = ? WHERE id = ?');
  const cityRows = db.prepare('SELECT id, city, state FROM cities').all();
  const citySlugSeen = new Map();
  const updateCityBatch = db.transaction((rows) => {
    for (const row of rows) {
      let base = slugify(`${row.city}-${row.state}`);
      let slug = base;
      let suffix = 1;
      while (citySlugSeen.has(slug)) {
        slug = `${base}-${suffix}`;
        suffix++;
      }
      citySlugSeen.set(slug, true);
      updateCity.run(slug, row.id);
    }
  });
  updateCityBatch(cityRows);

  const cityCount = db.prepare('SELECT COUNT(*) as cnt FROM cities').get();
  console.log(`  ${cityCount.cnt} cities (10+ providers)`);

  console.log('Building specialty_state table...');
  db.prepare(`
    INSERT INTO specialty_state (specialty_code, state, provider_count)
    SELECT specialty_code, state, COUNT(*) as cnt
    FROM providers
    GROUP BY specialty_code, state
    ORDER BY cnt DESC
  `).run();

  const ssCount = db.prepare('SELECT COUNT(*) as cnt FROM specialty_state').get();
  console.log(`  ${ssCount.cnt} specialty×state combinations`);

  // Create indices
  console.log('\nCreating indices...');
  const indices = [
    'CREATE INDEX idx_providers_state ON providers(state)',
    'CREATE INDEX idx_providers_specialty ON providers(specialty_code)',
    'CREATE INDEX idx_providers_city_state ON providers(city, state)',
    'CREATE INDEX idx_providers_last_name ON providers(last_name COLLATE NOCASE)',
    'CREATE INDEX idx_providers_slug ON providers(slug)',
    'CREATE INDEX idx_specialties_slug ON specialties(slug)',
    'CREATE INDEX idx_states_slug ON states(slug)',
    'CREATE INDEX idx_cities_state ON cities(state)',
    'CREATE INDEX idx_cities_slug ON cities(slug)',
    'CREATE INDEX idx_specialty_state_spec ON specialty_state(specialty_code)',
    'CREATE INDEX idx_specialty_state_state ON specialty_state(state)',
  ];
  for (const idx of indices) {
    db.prepare(idx).run();
  }

  // Create _stats table with pre-computed aggregate values
  // This avoids expensive COUNT(*) on the 7M-row providers table at runtime
  console.log('\nPopulating _stats table...');
  db.prepare('CREATE TABLE IF NOT EXISTS _stats (key TEXT PRIMARY KEY, value TEXT)').run();
  const statsQueries = [
    ["provider_count", "SELECT COUNT(*) FROM providers"],
    ["specialty_count", "SELECT COUNT(*) FROM specialties"],
    ["state_count", "SELECT COUNT(*) FROM states"],
    ["city_count", "SELECT COUNT(*) FROM cities"],
  ];
  const insertStat = db.prepare('INSERT OR REPLACE INTO _stats (key, value) VALUES (?, ?)');
  for (const [key, query] of statsQueries) {
    const row = db.prepare(query).get();
    const val = Object.values(row)[0];
    insertStat.run(key, String(val));
    console.log(`  ${key} = ${val}`);
  }

  db.close();

  const stats = statSync(DB_PATH);
  console.log(`\nDatabase: ${DB_PATH}`);
  console.log(`Size: ${(stats.size / 1024 / 1024).toFixed(1)} MB`);
  console.log('Done!');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
