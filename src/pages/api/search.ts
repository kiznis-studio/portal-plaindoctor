import type { APIRoute } from 'astro';

const CACHE_HEADERS = {
  'Content-Type': 'application/json',
  'Cache-Control': 'public, max-age=300, s-maxage=3600',
};

export const GET: APIRoute = async ({ request, locals }) => {
  const url = new URL(request.url);
  const query = url.searchParams.get('q') || '';
  const trimmed = query.trim();
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '15'), 15);

  if (trimmed.length < 2) {
    return new Response(JSON.stringify({ results: [], query: '' }), {
      headers: CACHE_HEADERS,
    });
  }

  const db = (locals as any).runtime.env.DB;

  // Check if query is an NPI number (10 digits)
  if (/^\d{10}$/.test(trimmed)) {
    const result = await db.prepare(
      'SELECT npi, first_name, last_name, credential, specialty, city, state, zip, phone, slug FROM providers WHERE npi = ?'
    ).bind(trimmed).first();
    return new Response(JSON.stringify({ results: result ? [result] : [], query: trimmed }), {
      headers: CACHE_HEADERS,
    });
  }

  // Use prefix search only (query%) to leverage indexes, not %query% which scans 7M rows
  const prefix = trimmed + '%';
  const { results } = await db.prepare(`
    SELECT npi, first_name, last_name, credential, specialty, city, state, zip, phone, slug
    FROM providers
    WHERE last_name LIKE ?1 OR first_name LIKE ?1 OR city LIKE ?1
    ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE
    LIMIT ?2
  `).bind(prefix, limit).all();

  return new Response(JSON.stringify({ results, query: trimmed }), {
    headers: CACHE_HEADERS,
  });
};
