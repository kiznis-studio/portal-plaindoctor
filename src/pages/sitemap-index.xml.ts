import type { APIRoute } from 'astro';
import { getSitemapPageCount } from '../lib/db';

const BASE = 'https://plaindoctor.com';

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;

  // Get page count from pre-computed sitemap_pages table
  const providerPages = await getSitemapPageCount(db);

  const sitemaps = [
    `${BASE}/sitemap-providers-priority.xml`,
    `${BASE}/sitemap-static.xml`,
    `${BASE}/sitemap-states.xml`,
    `${BASE}/sitemap-specialties.xml`,
    `${BASE}/sitemap-specialty-states.xml`,
    `${BASE}/sitemap-compare.xml`,
    `${BASE}/sitemap-cities.xml`,
    `${BASE}/sitemap-nursing-staffing.xml`,
    `${BASE}/sitemap-nursing-deficiencies.xml`,
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
