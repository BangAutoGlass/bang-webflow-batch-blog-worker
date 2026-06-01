# Bang Webflow Batch Blog Worker

This Render worker is aligned with the `webflow-blog-batch-creation` Supabase Edge Function.

## Deploy files

Deploy this folder as the Render worker repo.

Required files:
- `src/index.ts`
- `package.json`
- `package-lock.json`
- `tsconfig.json`
- `render.yaml`
- `index.ts` is only a compatibility entrypoint.

## Required Render environment variables

- `EDGE_FUNCTION_URL`
- `WORKER_SECRET`
- `WEBFLOW_API_TOKEN`
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`

Optional:
- `BATCH_JOB_ID` or `JOB_ID` to run one specific job instead of auto-discovering jobs.

## Render commands

Build command:

```bash
npm ci && npm run build
```

Start command:

```bash
npm start
```
