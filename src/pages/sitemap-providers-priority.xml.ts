import type { APIRoute } from 'astro';

const BASE = 'https://plaindoctor.com';

// Top specialties that people actually search for by name
const PRIORITY_SPECIALTIES = [
  'Family Medicine',
  'Internal Medicine',
  'Pediatrics',
  'Obstetrics & Gynecology',
  'Cardiology',
  'Dermatology',
  'Orthopedic Surgery',
  'Psychiatry & Neurology',
  'Ophthalmology',
  'General Surgery',
  'Emergency Medicine',
  'Anesthesiology',
  'Radiology',
  'Nurse Practitioner',
  'Dentist',
  'Optometry',
  'Physical Therapy',
  'Chiropractor',
  'Podiatry',
  'Physician Assistant',
];

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;

  // Get specialty codes for priority specialties
  const placeholders = PRIORITY_SPECIALTIES.map(() => '?').join(',');
  const { results: specs } = await db
    .prepare(`SELECT specialty_code FROM specialties WHERE name IN (${placeholders})`)
    .bind(...PRIORITY_SPECIALTIES)
    .all<{ specialty_code: string }>();

  if (specs.length === 0) {
    return new Response('<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>', {
      headers: { 'Content-Type': 'application/xml' },
    });
  }

  // Get providers from priority specialties in cities with 50+ providers (large cities)
  // Limit to 49,000 to stay under sitemap 50K limit
  const specCodes = specs.map(s => s.specialty_code);
  const specPlaceholders = specCodes.map(() => '?').join(',');

  const { results } = await db
    .prepare(`
      SELECT p.slug FROM providers p
      INNER JOIN cities c ON p.city = c.city AND p.state = c.state
      WHERE p.specialty_code IN (${specPlaceholders})
        AND c.provider_count >= 50
      ORDER BY c.provider_count DESC, p.last_name
      LIMIT 49000
    `)
    .bind(...specCodes)
    .all<{ slug: string }>();

  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...results.map(r => `  <url><loc>${BASE}/provider/${r.slug}</loc></url>`),
    '</urlset>',
  ].join('\n');

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml', 'Cache-Control': 'public, max-age=86400' },
  });
};
