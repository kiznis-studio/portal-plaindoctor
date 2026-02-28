import type { APIRoute } from 'astro';

const BASE = 'https://plaindoctor.com';

export const GET: APIRoute = async () => {
  const pages = [
    '/', '/specialty', '/state', '/search', '/compare',
    '/guides', '/guides/how-to-find-a-doctor', '/guides/medical-specialties-explained',
    '/guides/in-network-vs-out-of-network', '/guides/telehealth-guide',
    '/guides/credential-verification-guide',
    '/about', '/privacy', '/terms',
  ];

  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...pages.map(p => `  <url><loc>${BASE}${p}</loc><changefreq>weekly</changefreq></url>`),
    '</urlset>',
  ].join('\n');

  return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
};
