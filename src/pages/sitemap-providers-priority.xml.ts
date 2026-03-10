import type { APIRoute } from 'astro';

const BASE = 'https://plaindoctor.com';

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;

  // Pre-computed priority provider slugs (top specialties in large cities)
  const { results } = await db
    .prepare('SELECT slug FROM sitemap_priority')
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
