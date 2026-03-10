import type { APIRoute } from 'astro';
import { getSitemapPageBoundary, getSitemapProviderSlugs } from '../lib/db';

const BASE = 'https://plaindoctor.com';

export const GET: APIRoute = async ({ params, locals }) => {
  const page = parseInt(params.page || '1');
  if (isNaN(page) || page < 1) {
    return new Response('Not found', { status: 404 });
  }

  const db = (locals as any).runtime.env.DB;

  // Keyset pagination: look up the boundary NPI for this page, then range scan
  const startNpi = await getSitemapPageBoundary(db, page);
  if (!startNpi) {
    return new Response('Not found', { status: 404 });
  }

  const slugs = await getSitemapProviderSlugs(db, startNpi);
  if (slugs.length === 0) {
    return new Response('Not found', { status: 404 });
  }

  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...slugs.map(s => `  <url><loc>${BASE}/provider/${s}</loc></url>`),
    '</urlset>',
  ].join('\n');

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml', 'Cache-Control': 'public, max-age=86400' },
  });
};
