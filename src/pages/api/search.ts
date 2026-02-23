import type { APIRoute } from 'astro';

export const GET: APIRoute = async ({ request, locals }) => {
  const url = new URL(request.url);
  const query = url.searchParams.get('q') || '';
  const limit = Math.min(parseInt(url.searchParams.get('limit') || '20'), 50);

  if (!query.trim()) {
    return new Response(JSON.stringify({ results: [], query: '' }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const db = (locals as any).runtime.env.DB;
  const trimmed = query.trim();

  // Check if query is an NPI number (10 digits)
  if (/^\d{10}$/.test(trimmed)) {
    const result = await db.prepare('SELECT * FROM providers WHERE npi = ?').bind(trimmed).first();
    return new Response(JSON.stringify({ results: result ? [result] : [], query: trimmed }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const like = '%' + trimmed + '%';
  const { results } = await db.prepare(`
    SELECT npi, first_name, last_name, credential, specialty, city, state, zip, phone, slug
    FROM providers
    WHERE last_name LIKE ? OR first_name LIKE ? OR specialty LIKE ? OR city LIKE ?
    ORDER BY last_name COLLATE NOCASE, first_name COLLATE NOCASE
    LIMIT ?
  `).bind(like, like, like, like, limit).all();

  return new Response(JSON.stringify({ results, query: trimmed }), {
    headers: { 'Content-Type': 'application/json' },
  });
};
