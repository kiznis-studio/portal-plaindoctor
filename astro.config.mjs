import { defineConfig } from 'astro/config';
import node from '@astrojs/node';
import tailwindcss from '@tailwindcss/vite';
import sentry from '@sentry/astro';

export default defineConfig({
  output: 'server',
  adapter: node({ mode: 'standalone' }),
  site: 'https://plaindoctor.com',
  vite: {
    plugins: [tailwindcss()],
  },
  integrations: [
    sentry({
      dsn: 'https://8b81a49731da05f92bec9b10a8e826e0@o4510827630231552.ingest.de.sentry.io/4511031098802256',
      sourceMapsUploadOptions: {
        enabled: false,
      },
    }),
  ],
});
