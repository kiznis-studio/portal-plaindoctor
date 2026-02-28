import Database from 'better-sqlite3';
import { writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const dbPath = '/storage/plaindoctor/plaindoctor.db';
const outPath = join(__dirname, '..', 'src', 'data', 'precomputed.json');

const db = new Database(dbPath, { readonly: true });

// 1. All states — columns: abbr, name, slug, provider_count, specialty_count
const states = db.prepare(
  `SELECT abbr, name, slug, provider_count, specialty_count FROM states ORDER BY name COLLATE NOCASE`
).all();

// 2. All specialties sorted by provider_count DESC
//    covers both getAllSpecialties and getTopSpecialties (slice for top N)
//    columns: code, name, category, slug, provider_count
const specialties = db.prepare(
  `SELECT code, name, category, slug, provider_count FROM specialties ORDER BY provider_count DESC`
).all();

// 3. National stats from _stats table
const statsRows = db.prepare(`SELECT key, value FROM _stats`).all();
const nationalStats = {};
for (const row of statsRows) {
  nationalStats[row.key] = isNaN(Number(row.value)) ? row.value : Number(row.value);
}

const data = {
  states,
  specialties,
  nationalStats,
};

const json = JSON.stringify(data);
writeFileSync(outPath, json);
const size = (json.length / 1024).toFixed(1);
console.log(`Wrote ${outPath} (${size} KB)`);
console.log(`  states: ${states.length}`);
console.log(`  specialties: ${specialties.length}`);
console.log(`  nationalStats keys: ${Object.keys(nationalStats).join(', ')}`);

db.close();
