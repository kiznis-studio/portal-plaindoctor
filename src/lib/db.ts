// PlainDoctor D1 query library
// All functions accept D1Database as first param — NEVER at module scope

// --- Interfaces ---

export interface Provider {
  npi: string;
  first_name: string;
  last_name: string;
  credential: string | null;
  gender: string | null;
  specialty: string;
  specialty_code: string;
  city: string;
  state: string;
  zip: string;
  phone: string | null;
  address_line1: string | null;
  enumeration_date: string | null;
  slug: string;
}

export interface Specialty {
  code: string;
  name: string;
  category: string | null;
  slug: string;
  provider_count: number;
}

export interface StateInfo {
  abbr: string;
  name: string;
  slug: string;
  provider_count: number;
  specialty_count: number;
}

export interface CityInfo {
  id: number;
  city: string;
  state: string;
  slug: string;
  provider_count: number;
}

export interface SpecialtyState {
  specialty_code: string;
  state: string;
  provider_count: number;
}

// --- State Lookup ---

const STATE_NAMES: Record<string, string> = {
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

export function getStateName(abbr: string): string {
  return STATE_NAMES[abbr.toUpperCase()] || abbr;
}

// --- Helpers ---

export function formatNumber(num: number | null): string {
  if (num === null || num === undefined) return 'N/A';
  return num.toLocaleString();
}

export function formatProviderName(provider: Provider): string {
  const name = `${provider.first_name} ${provider.last_name}`;
  return provider.credential ? `${name}, ${provider.credential}` : name;
}

// --- Providers ---

export async function getProviderBySlug(db: D1Database, slug: string): Promise<Provider | null> {
  return db.prepare('SELECT * FROM providers WHERE slug = ?').bind(slug).first<Provider>();
}

export async function getProviderByNpi(db: D1Database, npi: string): Promise<Provider | null> {
  return db.prepare('SELECT * FROM providers WHERE npi = ?').bind(npi).first<Provider>();
}

export async function getProvidersBySpecialtyAndState(
  db: D1Database, specialtyCode: string, state: string, limit = 50, offset = 0
): Promise<Provider[]> {
  const { results } = await db.prepare(
    'SELECT * FROM providers WHERE specialty_code = ? AND state = ? ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE LIMIT ? OFFSET ?'
  ).bind(specialtyCode, state, limit, offset).all<Provider>();
  return results;
}

export async function getProviderCountBySpecialtyAndState(
  db: D1Database, specialtyCode: string, state: string
): Promise<number> {
  const row = await db.prepare(
    'SELECT provider_count FROM specialty_state WHERE specialty_code = ? AND state = ?'
  ).bind(specialtyCode, state).first<{ provider_count: number }>();
  return row?.provider_count || 0;
}

// --- Specialties ---

export async function getAllSpecialties(db: D1Database): Promise<Specialty[]> {
  const { results } = await db.prepare('SELECT * FROM specialties ORDER BY provider_count DESC').all<Specialty>();
  return results;
}

export async function getSpecialtyBySlug(db: D1Database, slug: string): Promise<Specialty | null> {
  return db.prepare('SELECT * FROM specialties WHERE slug = ?').bind(slug).first<Specialty>();
}

export async function getTopSpecialties(db: D1Database, limit = 20): Promise<Specialty[]> {
  const { results } = await db.prepare(
    'SELECT * FROM specialties ORDER BY provider_count DESC LIMIT ?'
  ).bind(limit).all<Specialty>();
  return results;
}

export async function getSpecialtyStates(db: D1Database, specialtyCode: string): Promise<SpecialtyState[]> {
  const { results } = await db.prepare(
    'SELECT * FROM specialty_state WHERE specialty_code = ? ORDER BY provider_count DESC'
  ).bind(specialtyCode).all<SpecialtyState>();
  return results;
}

// --- States ---

export async function getAllStates(db: D1Database): Promise<StateInfo[]> {
  const { results } = await db.prepare('SELECT * FROM states ORDER BY name COLLATE NOCASE').all<StateInfo>();
  return results;
}

export async function getStateBySlug(db: D1Database, slug: string): Promise<StateInfo | null> {
  return db.prepare('SELECT * FROM states WHERE slug = ?').bind(slug).first<StateInfo>();
}

// --- Cities ---

export async function getCitiesByState(db: D1Database, state: string, limit = 50): Promise<CityInfo[]> {
  const { results } = await db.prepare(
    'SELECT * FROM cities WHERE state = ? ORDER BY provider_count DESC LIMIT ?'
  ).bind(state, limit).all<CityInfo>();
  return results;
}

// --- Search ---

export async function searchProviders(db: D1Database, query: string, limit = 20): Promise<Provider[]> {
  const trimmed = query.trim();
  // Check if query is an NPI number (10 digits)
  if (/^\d{10}$/.test(trimmed)) {
    const result = await getProviderByNpi(db, trimmed);
    return result ? [result] : [];
  }
  const like = '%' + trimmed + '%';
  const { results } = await db.prepare(`
    SELECT * FROM providers
    WHERE last_name LIKE ? OR first_name LIKE ? OR specialty LIKE ? OR city LIKE ?
    ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE
    LIMIT ?
  `).bind(like, like, like, like, limit).all<Provider>();
  return results;
}

// --- Stats ---

export async function getNationalStats(db: D1Database) {
  return db.prepare(`
    SELECT
      (SELECT COUNT(*) FROM providers) as provider_count,
      (SELECT COUNT(*) FROM specialties) as specialty_count,
      (SELECT COUNT(*) FROM states) as state_count,
      (SELECT COUNT(*) FROM cities) as city_count
  `).first<{
    provider_count: number;
    specialty_count: number;
    state_count: number;
    city_count: number;
  }>();
}
