import type { APIRoute } from 'astro';

const BASE = 'https://plaindoctor.com';

export const GET: APIRoute = async ({ params, locals }) => {
  const page = parseInt(params.page || '1');
  if (isNaN(page) || page < 1) {
    return new Response('Not found', { status: 404 });
  }

  const limit = 50000;
  const offset = (page - 1) * limit;
  const db = (locals as any).runtime.env.DB;

  const { results } = await db
    .prepare('SELECT slug FROM providers ORDER BY npi LIMIT ? OFFSET ?')
    .bind(limit, offset)
    .all<{ slug: string }>();

  if (results.length === 0) {
    return new Response('Not found', { status: 404 });
  }

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
