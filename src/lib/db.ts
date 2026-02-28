// PlainDoctor D1 query library
// All functions accept D1Database as first param — NEVER at module scope

import precomputed from '../data/precomputed.json';

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

// --- State Populations (2023 Census estimates, for per-capita calculations) ---

export const STATE_POPULATIONS: Record<string, number> = {
  AL: 5108468, AK: 733406, AZ: 7431344, AR: 3067732, CA: 38965193,
  CO: 5877610, CT: 3617176, DE: 1031890, FL: 22610726, GA: 11029227,
  HI: 1435138, ID: 1964726, IL: 12549689, IN: 6862199, IA: 3207004,
  KS: 2940546, KY: 4526154, LA: 4573749, ME: 1395722, MD: 6180253,
  MA: 7001399, MI: 10037261, MN: 5737915, MS: 2939690, MO: 6196156,
  MT: 1132812, NE: 1978379, NV: 3194176, NH: 1402054, NJ: 9290841,
  NM: 2114371, NY: 19571216, NC: 10835491, ND: 783926, OH: 11785935,
  OK: 4053824, OR: 4233358, PA: 12961683, RI: 1095962, SC: 5373555,
  SD: 919318, TN: 7126489, TX: 30503301, UT: 3417734, VT: 647464,
  VA: 8642274, WA: 7812880, WV: 1770071, WI: 5910955, WY: 584057,
  DC: 678972, PR: 3205691, GU: 153836, VI: 87146,
  AS: 43914, MP: 47329,
};

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

export async function getAllSpecialties(_db: D1Database): Promise<Specialty[]> {
  return precomputed.specialties as Specialty[];
}

export async function getSpecialtyBySlug(db: D1Database, slug: string): Promise<Specialty | null> {
  return db.prepare('SELECT * FROM specialties WHERE slug = ?').bind(slug).first<Specialty>();
}

export async function getTopSpecialties(_db: D1Database, limit = 20): Promise<Specialty[]> {
  return (precomputed.specialties as Specialty[]).slice(0, limit);
}

export async function getSpecialtyStates(db: D1Database, specialtyCode: string): Promise<SpecialtyState[]> {
  const { results } = await db.prepare(
    'SELECT * FROM specialty_state WHERE specialty_code = ? ORDER BY provider_count DESC'
  ).bind(specialtyCode).all<SpecialtyState>();
  return results;
}

// --- States ---

export async function getAllStates(_db: D1Database): Promise<StateInfo[]> {
  return precomputed.states as StateInfo[];
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
  if (!trimmed) return [];
  // Check if query is an NPI number (10 digits)
  if (/^\d{10}$/.test(trimmed)) {
    const result = await getProviderByNpi(db, trimmed);
    return result ? [result] : [];
  }
  // Search last_name only using prefix match + index (idx_providers_last_name)
  // OR with multiple columns forces a full 7M-row SCAN; single-column search
  // is dramatically cheaper and covers the primary use case
  const prefix = trimmed + '%';
  const { results } = await db.prepare(`
    SELECT * FROM providers
    WHERE last_name LIKE ?
    ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE
    LIMIT ?
  `).bind(prefix, limit).all<Provider>();
  return results;
}

// --- Comparison ---

export interface SpecialtyStateRow {
  specialty_code: string;
  state: string;
  provider_count: number;
}

export async function getSpecialtyStateDistribution(
  db: D1Database, specialtyCode: string
): Promise<Map<string, number>> {
  const { results } = await db.prepare(
    'SELECT state, provider_count FROM specialty_state WHERE specialty_code = ? ORDER BY provider_count DESC'
  ).bind(specialtyCode).all<{ state: string; provider_count: number }>();
  const map = new Map<string, number>();
  for (const row of results) {
    map.set(row.state, row.provider_count);
  }
  return map;
}

// --- Stats (pre-computed to avoid full table scans on every page load) ---

export async function getNationalStats(_db: D1Database) {
  const s = precomputed.nationalStats as Record<string, number>;
  return {
    provider_count: s.provider_count ?? 0,
    specialty_count: s.specialty_count ?? 0,
    state_count: s.state_count ?? 0,
    city_count: s.city_count ?? 0,
  };
}
