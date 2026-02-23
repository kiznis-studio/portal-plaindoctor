import type { APIRoute } from 'astro';

export const GET: APIRoute = async ({ locals }) => {
  const base = 'https://plaindoctor.com';
  const db = (locals as any).runtime.env.DB;
  const { results } = await db.prepare('SELECT slug FROM states ORDER BY name COLLATE NOCASE').all<{ slug: string }>();

  const urls = results.map(s => `  <url><loc>${base}/state/${s.slug}</loc><changefreq>monthly</changefreq><priority>0.8</priority></url>`).join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

  return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
};
