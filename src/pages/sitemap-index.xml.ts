import type { APIRoute } from 'astro';

const BASE = 'https://plaindoctor.com';

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;

  // Count total providers for pagination
  const row = await db.prepare('SELECT COUNT(*) as count FROM providers').first<{ count: number }>();
  const totalProviders = row?.count || 0;
  const providerPages = Math.ceil(totalProviders / 50000);

  const sitemaps = [
    `${BASE}/sitemap-static.xml`,
    `${BASE}/sitemap-states.xml`,
    `${BASE}/sitemap-specialties.xml`,
    `${BASE}/sitemap-specialty-states.xml`,
    `${BASE}/sitemap-compare.xml`,
  ];

  for (let i = 1; i <= providerPages; i++) {
    sitemaps.push(`${BASE}/sitemap-providers-${i}.xml`);
  }

  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...sitemaps.map(loc => `  <sitemap><loc>${loc}</loc></sitemap>`),
    '</sitemapindex>',
  ].join('\n');

  return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
};
