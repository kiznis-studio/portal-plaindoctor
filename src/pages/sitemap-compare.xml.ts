import type { APIRoute } from 'astro';
import { getTopSpecialties } from '../lib/db';

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;
  const top = await getTopSpecialties(db, 20);

  // Generate comparison pairs from top 20 specialties (canonical order)
  const urls: string[] = ['https://plaindoctor.com/compare/'];
  for (let i = 0; i < top.length; i++) {
    for (let j = i + 1; j < top.length; j++) {
      const a = top[i].slug < top[j].slug ? top[i].slug : top[j].slug;
      const b = top[i].slug < top[j].slug ? top[j].slug : top[i].slug;
      urls.push('https://plaindoctor.com/compare/' + a + '-vs-' + b);
    }
  }

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map(u => '  <url><loc>' + u + '</loc><changefreq>monthly</changefreq><priority>0.6</priority></url>').join('\n')}
</urlset>`;

  return new Response(xml, { headers: { 'Content-Type': 'application/xml' } });
};
