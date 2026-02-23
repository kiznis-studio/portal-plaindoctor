import type { APIRoute } from 'astro';

export const GET: APIRoute = async ({ locals }) => {
  const base = 'https://plaindoctor.com';
  const db = (locals as any).runtime.env.DB;

  // Get specialty slugs joined with state combos
  const { results } = await db.prepare(`
    SELECT s.slug as spec_slug, LOWER(ss.state) as state_lower
    FROM specialty_state ss
    JOIN specialties s ON s.code = ss.specialty_code
    WHERE ss.provider_count >= 10
    ORDER BY ss.provider_count DESC
  `).all<{ spec_slug: string; state_lower: string }>();

  const urls = results.map(r => `  <url><loc>${base}/specialty/${r.spec_slug}/${r.state_lower}</loc><changefreq>monthly</changefreq><priority>0.5</priority></url>`).join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

  return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
};
