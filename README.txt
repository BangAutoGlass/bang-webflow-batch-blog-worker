Bang Webflow Batch Blog Worker - CSV-safe publish version

Deploy this folder as the Render worker repo.

Files:
- src/index.ts: main worker code
- package.json: npm scripts and dependencies
- package-lock.json: lockfile
- tsconfig.json: TypeScript config
- render.yaml: Render worker config
- index.ts: compatibility entrypoint for older manual commands

Main update in this version:
- The worker now strips unsupported Webflow fieldData keys before publishing.
- It only sends CSV-confirmed field slugs: name, slug, article-author, author-name, publication-date, category, post-summary, make, model, service-label, city, state.
- It defensively removes rejected fields such as post-body, post-body-2, meta_title, meta_description, faqHtml, ctaText, imagePrompt, notes, and tags if a stale edge payload still contains them.

Validation performed:
- npm ci
- npm run build
- WORKER_ENABLED=false npm start
