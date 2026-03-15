// PlainDoctor D1 query library
// All functions accept D1Database as first param — NEVER at module scope

import precomputed from '../data/precomputed.json';

// Targeted query cache for expensive SHARED queries only.
// Caches specialty-level, state-level, and global aggregations.
// Permanent — data is static, container restart = cache invalidation.
const queryCache = new Map<string, any>();
export function getQueryCacheSize(): number { return queryCache.size; }

const IS_CLUSTER_WORKER = process.env.WORKER_INTERNAL === '1';
const pendingIpc = new Map<string, Array<{ resolve: (v: any) => void }>>();

if (IS_CLUSTER_WORKER) {
  process.on('message', (msg: any) => {
    if (msg?.type === 'qcache-result') {
      const waiters = pendingIpc.get(msg.key);
      if (waiters) {
        pendingIpc.delete(msg.key);
        if (msg.hit) {
          queryCache.set(msg.key, msg.value);
          for (const w of waiters) w.resolve(msg.value);
        } else {
          for (const w of waiters) w.resolve(null);
        }
      }
    }
  });
}

function cached<T>(key: string, compute: () => Promise<T>): Promise<T> {
  if (queryCache.has(key)) return Promise.resolve(queryCache.get(key) as T);

  if (IS_CLUSTER_WORKER && process.send) {
    return new Promise<T>((resolve) => {
      if (pendingIpc.has(key)) {
        pendingIpc.get(key)!.push({ resolve: resolve as any });
        return;
      }
      pendingIpc.set(key, [{ resolve: resolve as any }]);
      process.send!({ type: 'qcache-get', key });
      setTimeout(() => {
        if (pendingIpc.has(key)) {
          pendingIpc.delete(key);
          compute().then(result => {
            queryCache.set(key, result);
            if (process.send) process.send({ type: 'qcache-set', key, value: result });
            resolve(result);
          });
        }
      }, 200);
    }).then(val => {
      if (val !== null) return val as T;
      return compute().then(result => {
        queryCache.set(key, result);
        if (process.send) process.send({ type: 'qcache-set', key, value: result });
        return result;
      });
    });
  }

  return compute().then(result => { queryCache.set(key, result); return result; });
}

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
): Promise<Pick<Provider, 'slug' | 'first_name' | 'last_name' | 'credential' | 'specialty' | 'city' | 'state' | 'zip' | 'phone'>[]> {
  const { results } = await db.prepare(
    'SELECT slug, first_name, last_name, credential, specialty, city, state, zip, phone FROM providers WHERE specialty_code = ? AND state = ? ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE LIMIT ? OFFSET ?'
  ).bind(specialtyCode, state, limit, offset).all();
  return results as Pick<Provider, 'slug' | 'first_name' | 'last_name' | 'credential' | 'specialty' | 'city' | 'state' | 'zip' | 'phone'>[];
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

export function getSpecialtyStates(db: D1Database, specialtyCode: string): Promise<SpecialtyState[]> {
  return cached(`spec-states:${specialtyCode}`, async () => {
    const { results } = await db.prepare(
      'SELECT * FROM specialty_state WHERE specialty_code = ? ORDER BY provider_count DESC'
    ).bind(specialtyCode).all<SpecialtyState>();
    return results;
  });
}

// --- Top Cities by Specialty ---

export function getTopCitiesBySpecialty(
  db: D1Database, specialtyCode: string, limit = 20
): Promise<{ city: string; state: string; slug: string; count: number }[]> {
  return cached(`spec-cities:${specialtyCode}:${limit}`, async () => {
    const { results } = await db.prepare(
      `SELECT city, state, city_slug as slug, provider_count as count
       FROM specialty_top_cities
       WHERE specialty_code = ?
       ORDER BY provider_count DESC
       LIMIT ?`
    ).bind(specialtyCode, limit).all();
    return results as { city: string; state: string; slug: string; count: number }[];
  });
}

// --- States ---

export async function getAllStates(_db: D1Database): Promise<StateInfo[]> {
  return precomputed.states as StateInfo[];
}

export async function getStateBySlug(db: D1Database, slug: string): Promise<StateInfo | null> {
  return db.prepare('SELECT * FROM states WHERE slug = ?').bind(slug).first<StateInfo>();
}

// --- Cities ---

export function getCitiesByState(db: D1Database, state: string, limit = 50): Promise<CityInfo[]> {
  return cached(`cities:${state}:${limit}`, async () => {
    const { results } = await db.prepare(
      'SELECT * FROM cities WHERE state = ? ORDER BY provider_count DESC LIMIT ?'
    ).bind(state, limit).all<CityInfo>();
    return results;
  });
}

export async function getCityBySlug(db: D1Database, slug: string): Promise<CityInfo | null> {
  return db.prepare('SELECT * FROM cities WHERE slug = ?').bind(slug).first<CityInfo>();
}

export async function getProvidersByCity(
  db: D1Database, city: string, state: string, limit = 50, offset = 0
): Promise<Pick<Provider, 'slug' | 'first_name' | 'last_name' | 'credential' | 'specialty' | 'phone'>[]> {
  const { results } = await db.prepare(
    'SELECT slug, first_name, last_name, credential, specialty, phone FROM providers WHERE city = ? AND state = ? ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE LIMIT ? OFFSET ?'
  ).bind(city, state, limit, offset).all();
  return results as Pick<Provider, 'slug' | 'first_name' | 'last_name' | 'credential' | 'specialty' | 'phone'>[];
}

export async function getCitySpecialties(
  db: D1Database, city: string, state: string, limit = 20
): Promise<{ specialty: string; specialty_code: string; count: number }[]> {
  // Materialized table lookup — already <2ms, no cache needed (response cache handles pages)
  const { results } = await db.prepare(
    `SELECT specialty, specialty_code, provider_count as count
     FROM city_top_specialties
     WHERE city = ? AND state = ?
     ORDER BY provider_count DESC LIMIT ?`
  ).bind(city, state, limit).all<{ specialty: string; specialty_code: string; count: number }>();
  return results;
}

// --- Sitemap Helpers ---

export async function getSitemapPageBoundary(db: D1Database, page: number): Promise<string | null> {
  const row = await db.prepare('SELECT start_npi FROM sitemap_pages WHERE page = ?').bind(page).first<{ start_npi: string }>();
  return row?.start_npi ?? null;
}

export async function getSitemapProviderSlugs(db: D1Database, startNpi: string, limit = 50000): Promise<string[]> {
  const { results } = await db.prepare(
    'SELECT slug FROM providers WHERE npi >= ? ORDER BY npi LIMIT ?'
  ).bind(startNpi, limit).all<{ slug: string }>();
  return results.map(r => r.slug);
}

export async function getSitemapPageCount(db: D1Database): Promise<number> {
  const row = await db.prepare('SELECT MAX(page) as max_page FROM sitemap_pages').first<{ max_page: number }>();
  return row?.max_page ?? 0;
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

export function getSpecialtyStateDistribution(
  db: D1Database, specialtyCode: string
): Promise<Map<string, number>> {
  return cached(`spec-dist:${specialtyCode}`, async () => {
    const { results } = await db.prepare(
      'SELECT state, provider_count FROM specialty_state WHERE specialty_code = ? ORDER BY provider_count DESC'
    ).bind(specialtyCode).all<{ state: string; provider_count: number }>();
    const map = new Map<string, number>();
    for (const row of results) {
      map.set(row.state, row.provider_count);
    }
    return map;
  });
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

// --- Part D Prescriber Data (CMS Medicare Part D) ---

export interface PrescriberSummary {
  npi: string;
  total_claims: number | null;
  total_30day_fills: number | null;
  total_drug_cost: number | null;
  total_day_supply: number | null;
  total_beneficiaries: number | null;
  brand_claims: number | null;
  brand_cost: number | null;
  generic_claims: number | null;
  generic_cost: number | null;
  opioid_claims: number | null;
  opioid_cost: number | null;
  opioid_benes: number | null;
  opioid_prescriber_rate: number | null;
  antibiotic_claims: number | null;
  antibiotic_cost: number | null;
  bene_avg_age: number | null;
  bene_age_lt65: number | null;
  bene_age_65_74: number | null;
  bene_age_75_84: number | null;
  bene_age_gt84: number | null;
  bene_female: number | null;
  bene_male: number | null;
  bene_avg_risk_score: number | null;
}

export async function getPrescriberSummary(db: D1Database, npi: string): Promise<PrescriberSummary | null> {
  return db.prepare('SELECT * FROM prescriber_summary WHERE npi = ?').bind(npi).first<PrescriberSummary>();
}

export function getTopPrescribersByCost(db: D1Database, limit = 50): Promise<(PrescriberSummary & { first_name: string; last_name: string; specialty: string; state: string; slug: string })[]> {
  return cached(`top-prescribers-cost:${limit}`, async () => {
    const { results } = await db.prepare(`
      SELECT ps.*, p.first_name, p.last_name, p.specialty, p.state, p.slug
      FROM prescriber_summary ps
      JOIN providers p ON p.npi = ps.npi
      WHERE ps.total_drug_cost IS NOT NULL
      ORDER BY ps.total_drug_cost DESC
      LIMIT ?
    `).bind(limit).all();
    return results as any[];
  });
}

export function getTopOpioidPrescribers(db: D1Database, limit = 50): Promise<(PrescriberSummary & { first_name: string; last_name: string; specialty: string; state: string; slug: string })[]> {
  return cached(`top-opioid:${limit}`, async () => {
    const { results } = await db.prepare(`
      SELECT ps.*, p.first_name, p.last_name, p.specialty, p.state, p.slug
      FROM prescriber_summary ps
      JOIN providers p ON p.npi = ps.npi
      WHERE ps.opioid_prescriber_rate IS NOT NULL AND ps.opioid_claims >= 10
      ORDER BY ps.opioid_prescriber_rate DESC
      LIMIT ?
    `).bind(limit).all();
    return results as any[];
  });
}

export function getPrescriberStatsByState(db: D1Database, stateAbbr: string): Promise<{
  prescribers: number; total_claims: number; total_cost: number;
  avg_cost_per_prescriber: number; opioid_prescribers: number; avg_opioid_rate: number;
} | null> {
  return cached(`prescriber-stats:${stateAbbr}`, async () => {
    return db.prepare(`
      SELECT prescribers, total_claims, total_cost,
        avg_cost_per_prescriber, opioid_prescribers, avg_opioid_rate
      FROM prescriber_state_stats
      WHERE state = ?
    `).bind(stateAbbr).first();
  });
}

export function getNationalPrescriberStats(db: D1Database): Promise<{
  total_prescribers: number; total_claims: number; total_cost: number;
  opioid_prescribers: number; avg_opioid_rate: number; avg_bene_age: number;
} | null> {
  return cached('national-prescriber-stats', async () => {
    return db.prepare(`
      SELECT
        COUNT(*) as total_prescribers,
        SUM(total_claims) as total_claims,
        SUM(total_drug_cost) as total_cost,
        SUM(CASE WHEN opioid_claims > 0 THEN 1 ELSE 0 END) as opioid_prescribers,
        ROUND(AVG(CASE WHEN opioid_prescriber_rate IS NOT NULL THEN opioid_prescriber_rate END), 1) as avg_opioid_rate,
        ROUND(AVG(bene_avg_age), 1) as avg_bene_age
      FROM prescriber_summary
    `).first();
  });
}

// --- Related Providers ---

export interface RelatedProvider {
  slug: string;
  first_name: string;
  last_name: string;
  credential: string | null;
  specialty: string;
  city: string;
  state: string;
}

export async function getRelatedProviders(
  db: D1Database, specialtyCode: string, state: string, excludeNpi: string, limit = 6
): Promise<RelatedProvider[]> {
  // Cache per specialty+state (fetch slightly more, filter excludeNpi in JS)
  const all = await cached(`related:${specialtyCode}:${state}`, async () => {
    const { results } = await db.prepare(
      `SELECT npi, slug, first_name, last_name, credential, specialty, city, state
       FROM providers
       WHERE specialty_code = ? AND state = ?
       ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE
       LIMIT ?`
    ).bind(specialtyCode, state, limit + 5).all<RelatedProvider & { npi: string }>();
    return results;
  });
  return all.filter(p => p.npi !== excludeNpi).slice(0, limit);
}

// --- Specialty Stats (for provider detail context) ---

export async function getSpecialtyStats(db: D1Database, specialtyCode: string): Promise<{
  total_providers: number;
  states_count: number;
  prescribers: number | null;
  avg_claims: number | null;
  avg_cost: number | null;
} | null> {
  // Materialized table — pre-computed during ETL, single PK lookup (<1ms)
  return db.prepare(
    'SELECT total_providers, states_count, prescribers, avg_claims, avg_cost FROM specialty_prescriber_stats WHERE specialty_code = ?'
  ).bind(specialtyCode).first();
}

// --- Nursing Homes (CMS Five-Star Quality Rating) ---

export interface NursingHome {
  ccn: string;
  slug: string;
  name: string;
  address: string | null;
  city: string | null;
  state: string;
  zip: string | null;
  phone: string | null;
  county: string | null;
  ownership: string | null;
  beds: number | null;
  avg_residents: number | null;
  overall_rating: number | null;
  health_rating: number | null;
  qm_rating: number | null;
  staffing_rating: number | null;
  long_stay_qm: number | null;
  short_stay_qm: number | null;
  rn_hours: number | null;
  total_deficiencies: number | null;
  standard_deficiencies: number | null;
  complaint_deficiencies: number | null;
  deficiency_score: number | null;
  cycle1_survey_date: string | null;
  cycle2_total_deficiencies: number | null;
  cycle2_standard: number | null;
  cycle2_complaint: number | null;
  cycle2_score: number | null;
  cycle2_survey_date: string | null;
  infection_citations: number | null;
  weighted_health_score: number | null;
  num_fines: number | null;
  fine_amount: number | null;
  num_penalties: number | null;
  abuse_icon: number;
  special_focus: number;
  lat: number | null;
  lng: number | null;
}

export interface NursingHomeState {
  state: string;
  home_count: number;
  total_beds: number;
  avg_rating: number | null;
}

export async function getNursingHomeBySlug(db: D1Database, slug: string): Promise<NursingHome | null> {
  return db.prepare('SELECT * FROM nursing_homes WHERE slug = ?').bind(slug).first<NursingHome>();
}

export async function getNursingHomesByState(db: D1Database, state: string, limit = 50, offset = 0): Promise<NursingHome[]> {
  const { results } = await db.prepare(
    'SELECT * FROM nursing_homes WHERE state = ? ORDER BY overall_rating DESC, beds DESC LIMIT ? OFFSET ?'
  ).bind(state, limit, offset).all<NursingHome>();
  return results;
}

export async function getNursingHomeCountByState(db: D1Database, state: string): Promise<number> {
  const row = await db.prepare('SELECT COUNT(*) as cnt FROM nursing_homes WHERE state = ?').bind(state).first<{ cnt: number }>();
  return row?.cnt ?? 0;
}

export function getAllNursingHomeStates(db: D1Database): Promise<NursingHomeState[]> {
  return cached('nh-states', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM nursing_home_states ORDER BY home_count DESC'
    ).all<NursingHomeState>();
    return results;
  });
}

export function getNursingHomeStats(db: D1Database): Promise<{ total: number; states: number; avg_beds: number }> {
  return cached('nh-stats', async () => {
    const row = await db.prepare(
      'SELECT COUNT(*) as total, COUNT(DISTINCT state) as states, ROUND(AVG(beds)) as avg_beds FROM nursing_homes'
    ).first<{ total: number; states: number; avg_beds: number }>();
    return row ?? { total: 0, states: 0, avg_beds: 0 };
  });
}

export function renderStars(rating: number | null): string {
  if (rating == null) return 'N/A';
  return '★'.repeat(rating) + '☆'.repeat(5 - rating);
}

// --- Nursing Home Staffing Rankings ---

export function getNursingHomesByStaffing(
  db: D1Database, limit = 100, offset = 0
): Promise<NursingHome[]> {
  if (offset === 0) return cached(`nh-staffing:${limit}`, async () => {
    const { results } = await db.prepare(
      `SELECT * FROM nursing_homes
       WHERE rn_hours IS NOT NULL AND rn_hours > 0
       ORDER BY rn_hours DESC
       LIMIT ? OFFSET ?`
    ).bind(limit, offset).all<NursingHome>();
    return results;
  });
  return (async () => {
    const { results } = await db.prepare(
      `SELECT * FROM nursing_homes
       WHERE rn_hours IS NOT NULL AND rn_hours > 0
       ORDER BY rn_hours DESC
       LIMIT ? OFFSET ?`
    ).bind(limit, offset).all<NursingHome>();
    return results;
  })();
}

export async function getNursingHomeStaffingByState(
  db: D1Database, state: string, limit = 50, offset = 0
): Promise<NursingHome[]> {
  const { results } = await db.prepare(
    `SELECT * FROM nursing_homes
     WHERE state = ? AND rn_hours IS NOT NULL AND rn_hours > 0
     ORDER BY rn_hours DESC
     LIMIT ? OFFSET ?`
  ).bind(state, limit, offset).all<NursingHome>();
  return results;
}

export interface StaffingSummary {
  state: string;
  home_count: number;
  avg_rn_hours: number;
  avg_staffing_rating: number;
  pct_4plus: number;
}

export function getNursingHomeStaffingSummaryByState(
  db: D1Database
): Promise<StaffingSummary[]> {
  return cached('nh-staffing-summary', async () => {
    const { results } = await db.prepare(
      `SELECT
         state,
         COUNT(*) as home_count,
         ROUND(AVG(rn_hours), 3) as avg_rn_hours,
         ROUND(AVG(staffing_rating), 1) as avg_staffing_rating,
         ROUND(100.0 * SUM(CASE WHEN staffing_rating >= 4 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_4plus
       FROM nursing_homes
       WHERE rn_hours IS NOT NULL
       GROUP BY state
       ORDER BY avg_rn_hours DESC`
    ).all<StaffingSummary>();
    return results;
  });
}

export function getNationalStaffingAvg(db: D1Database): Promise<{ avg_rn: number; avg_rating: number }> {
  return cached('nh-national-staffing', async () => {
    const row = await db.prepare(
      `SELECT ROUND(AVG(rn_hours), 3) as avg_rn, ROUND(AVG(staffing_rating), 1) as avg_rating
       FROM nursing_homes WHERE rn_hours IS NOT NULL`
    ).first<{ avg_rn: number; avg_rating: number }>();
    return row ?? { avg_rn: 0, avg_rating: 0 };
  });
}

// --- Nursing Home Deficiency Inspector ---

export function getNursingHomesByDeficiencies(
  db: D1Database, limit = 100, offset = 0
): Promise<NursingHome[]> {
  if (offset === 0) return cached(`nh-deficiencies:${limit}`, async () => {
    const { results } = await db.prepare(
      `SELECT * FROM nursing_homes
       WHERE total_deficiencies IS NOT NULL
       ORDER BY total_deficiencies DESC
       LIMIT ? OFFSET ?`
    ).bind(limit, offset).all<NursingHome>();
    return results;
  });
  return (async () => {
    const { results } = await db.prepare(
      `SELECT * FROM nursing_homes
       WHERE total_deficiencies IS NOT NULL
       ORDER BY total_deficiencies DESC
       LIMIT ? OFFSET ?`
    ).bind(limit, offset).all<NursingHome>();
    return results;
  })();
}

export async function getNursingHomeDeficienciesByState(
  db: D1Database, state: string, limit = 50, offset = 0
): Promise<NursingHome[]> {
  const { results } = await db.prepare(
    `SELECT * FROM nursing_homes
     WHERE state = ? AND total_deficiencies IS NOT NULL
     ORDER BY total_deficiencies DESC
     LIMIT ? OFFSET ?`
  ).bind(state, limit, offset).all<NursingHome>();
  return results;
}

export interface DeficiencySummary {
  state: string;
  home_count: number;
  avg_deficiencies: number;
  avg_health_rating: number;
  total_fines: number;
  pct_with_complaints: number;
  avg_infection_citations: number;
}

export function getDeficiencySummaryByState(
  db: D1Database
): Promise<DeficiencySummary[]> {
  return cached('nh-deficiency-summary', async () => {
    const { results } = await db.prepare(
      `SELECT
         state,
         COUNT(*) as home_count,
         ROUND(AVG(total_deficiencies), 1) as avg_deficiencies,
         ROUND(AVG(health_rating), 1) as avg_health_rating,
         ROUND(SUM(COALESCE(fine_amount, 0))) as total_fines,
         ROUND(100.0 * SUM(CASE WHEN complaint_deficiencies > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as pct_with_complaints,
         ROUND(AVG(COALESCE(infection_citations, 0)), 1) as avg_infection_citations
       FROM nursing_homes
       WHERE total_deficiencies IS NOT NULL
       GROUP BY state
       ORDER BY avg_deficiencies DESC`
    ).all<DeficiencySummary>();
    return results;
  });
}

export function getNationalDeficiencyAvg(
  db: D1Database
): Promise<{ avg_deficiencies: number; avg_score: number; avg_fines: number; total_with_abuse: number }> {
  return cached('nh-national-deficiency', async () => {
    const row = await db.prepare(
      `SELECT
         ROUND(AVG(total_deficiencies), 1) as avg_deficiencies,
         ROUND(AVG(deficiency_score), 0) as avg_score,
         ROUND(AVG(COALESCE(fine_amount, 0)), 0) as avg_fines,
         SUM(CASE WHEN abuse_icon = 1 THEN 1 ELSE 0 END) as total_with_abuse
       FROM nursing_homes WHERE total_deficiencies IS NOT NULL`
    ).first<{ avg_deficiencies: number; avg_score: number; avg_fines: number; total_with_abuse: number }>();
    return row ?? { avg_deficiencies: 0, avg_score: 0, avg_fines: 0, total_with_abuse: 0 };
  });
}

export async function warmQueryCache(db: D1Database): Promise<number> {
  const start = Date.now();
  const states = await getAllStates(db);
  // Warm only top 100 specialties by provider count (already sorted DESC).
  // The other 590 specialties cache on-demand (<2ms from materialized tables).
  const topSpecialties = (await getAllSpecialties(db)).slice(0, 100);
  await Promise.all([
    getNationalPrescriberStats(db),
    getTopPrescribersByCost(db),
    getTopOpioidPrescribers(db),
    getAllNursingHomeStates(db),
    getNursingHomeStats(db),
    getNursingHomeStaffingSummaryByState(db),
    getNationalStaffingAvg(db),
    getDeficiencySummaryByState(db),
    getNationalDeficiencyAvg(db),
    getNursingHomesByStaffing(db),
    getNursingHomesByDeficiencies(db),
    ...states.map(s => Promise.all([
      getCitiesByState(db, s.abbr),
      getPrescriberStatsByState(db, s.abbr),
    ])),
    ...topSpecialties.map(sp => Promise.all([
      getSpecialtyStates(db, sp.code),
      getTopCitiesBySpecialty(db, sp.code),
    ])),
  ]);
  console.log(`[cache] Warmed ${queryCache.size} queries in ${Date.now() - start}ms`);
  return queryCache.size;
}
