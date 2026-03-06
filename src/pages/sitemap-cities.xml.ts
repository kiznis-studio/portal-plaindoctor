import type { APIRoute } from 'astro';

const BASE = 'https://plaindoctor.com';

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;
  const { results } = await db.prepare(
    'SELECT slug FROM cities ORDER BY provider_count DESC'
  ).all<{ slug: string }>();

  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...results.map(c => `  <url><loc>${BASE}/city/${c.slug}</loc><changefreq>monthly</changefreq></url>`),
    '</urlset>',
  ].join('\n');

  return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
};
