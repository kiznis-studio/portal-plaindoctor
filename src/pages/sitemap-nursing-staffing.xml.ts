import type { APIRoute } from 'astro';

const BASE = 'https://plaindoctor.com';

const STATES = [
  'al','ak','az','ar','ca','co','ct','de','dc','fl','ga','hi',
  'id','il','in','ia','ks','ky','la','me','md','ma','mi','mn',
  'ms','mo','mt','ne','nv','nh','nj','nm','ny','nc','nd','oh',
  'ok','or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa',
  'wv','wi','wy',
];

export const GET: APIRoute = async () => {
  const urls = [
    `${BASE}/nursing-homes/staffing-rankings`,
    ...STATES.map(s => `${BASE}/nursing-homes/${s}/staffing`),
  ];

  const xml = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...urls.map(url => `  <url><loc>${url}</loc><changefreq>monthly</changefreq></url>`),
    '</urlset>',
  ].join('\n');

  return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
};
