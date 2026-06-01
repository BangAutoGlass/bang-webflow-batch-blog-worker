const DEFAULT_EDGE_REQUEST_TIMEOUT_SECONDS = 180
const DEFAULT_WEBFLOW_REQUEST_TIMEOUT_SECONDS = 120
const DEFAULT_BATCH_MAX_ROWS = 500
const DEFAULT_PUBLISH_BATCH_SIZE = 10
const DEFAULT_PUBLISH_CONCURRENCY = 5
const DEFAULT_POLL_INTERVAL_MS = 60_000
const DEFAULT_IDLE_POLL_INTERVAL_MS = 120_000
const DEFAULT_ERROR_SLEEP_MS = 30_000
const DEFAULT_OPENAI_BATCH_MIN_POLL_INTERVAL_MS = 60_000
const DEFAULT_WEBFLOW_API_BASE = "https://api.webflow.com/v2"

// Confirmed Webflow publish field allowlist.
// Webflow response payload confirmed these API slugs for the Blog Posts collection:
// - Post Body => rich-text
// - meta title => meta-title
// - meta description => meta-description
// The generated-only fields below are still stripped because Webflow rejected them:
// faqHtml, ctaText, imagePrompt, notes, tags, post-body, post-body-2, meta_title, meta_description.
const CSV_SAFE_WEBFLOW_FIELD_SLUGS = new Set([
  "name",
  "slug",
  "article-author",
  "author-name",
  "publication-date",
  "category",
  "rich-text",
  "post-summary",
  "meta-title",
  "meta-description",
  "make",
  "model",
  "service-label",
  "city",
  "state",
])


type JsonRecord = Record<string, unknown>
type BatchPhase = "body_generation" | "title_and_field_generation"
type JobStatus =
  | "draft"
  | "queued"
  | "body_batch_preparing"
  | "body_batch_submitted"
  | "body_batch_running"
  | "body_batch_completed"
  | "field_batch_preparing"
  | "field_batch_submitted"
  | "field_batch_running"
  | "field_batch_completed"
  | "publishing"
  | "completed"
  | "completed_with_errors"
  | "cancelled"
  | "error"

type BatchJob = {
  id: string
  status: JobStatus
  updated_at?: string
  body_batch_id?: string | null
  field_batch_id?: string | null
}

type EdgeResponse = JsonRecord & {
  success?: boolean
  jobId?: string
  job?: JsonRecord
  batches?: JsonRecord[]
  rows?: JsonRecord[]
  action?: string
  publishItems?: PublishItem[]
  claimedTotal?: number
  message?: string
  error?: string
}

type PublishItem = {
  row?: JsonRecord
  publishAttempt?: JsonRecord
  request?: JsonRecord
  workerId?: string
}

type PublishResult = {
  ok: boolean
  rowId: string
  webflowItemId?: string
  skipped?: boolean
  error?: string
}

const shutdownState = {
  shuttingDown: false,
}

const lastBatchPollAtByJobPhase = new Map<string, number>()

function envValue(name: string) {
  const value = process.env[name]
  return value && value.trim() ? value.trim() : ""
}

function getRequiredEnv(name: string) {
  const value = envValue(name)
  if (!value) throw new Error(`Missing required environment variable: ${name}`)
  return value
}

function boolEnv(name: string, fallback: boolean) {
  const value = envValue(name).toLowerCase()
  if (["true", "1", "yes", "y", "on"].includes(value)) return true
  if (["false", "0", "no", "n", "off"].includes(value)) return false
  return fallback
}

function numberEnv(name: string, fallback: number, min: number, max: number) {
  const parsed = Number(process.env[name])
  if (!Number.isFinite(parsed)) return fallback
  return Math.max(min, Math.min(max, Math.floor(parsed)))
}

function firstString(...values: unknown[]) {
  for (const value of values) {
    const text = stringifyValue(value).trim()
    if (text) return text
  }
  return ""
}

function stringifyValue(value: unknown) {
  if (value === null || value === undefined) return ""
  if (typeof value === "string") return value
  if (typeof value === "number" || typeof value === "boolean") return String(value)
  try {
    return JSON.stringify(value)
  } catch {
    return ""
  }
}

function isRecord(value: unknown): value is JsonRecord {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value)
}

function asRecord(value: unknown, fallback: JsonRecord = {}) {
  return isRecord(value) ? value : fallback
}

function compactObject(value: JsonRecord) {
  const output: JsonRecord = {}
  for (const [key, innerValue] of Object.entries(value)) {
    if (innerValue === undefined || innerValue === null) continue
    if (typeof innerValue === "string" && innerValue.trim() === "") continue
    if (Array.isArray(innerValue) && innerValue.length === 0) continue
    output[key] = innerValue
  }
  return output
}

function sanitizeWebflowFieldDataForConfirmedPublish(fieldData: JsonRecord) {
  const safeFieldData: JsonRecord = {}

  for (const [key, value] of Object.entries(fieldData)) {
    if (!CSV_SAFE_WEBFLOW_FIELD_SLUGS.has(key)) continue
    if (value === undefined || value === null) continue
    if (typeof value === "string" && value.trim() === "") continue
    if (Array.isArray(value) && value.length === 0) continue
    safeFieldData[key] = value
  }

  return safeFieldData
}

function getDroppedWebflowFieldSlugs(originalFieldData: JsonRecord, safeFieldData: JsonRecord) {
  return Object.keys(originalFieldData).filter((key) => !(key in safeFieldData))
}

function sanitizePublishRequestForConfirmedWebflow(request: JsonRecord): { request: JsonRecord; droppedFieldSlugs: string[] } {
  const originalBody = asRecord(request.body)
  const originalFieldData = asRecord(originalBody.fieldData || request.fieldData || request.finalFieldData)
  const safeFieldData = sanitizeWebflowFieldDataForConfirmedPublish(originalFieldData)
  const droppedFieldSlugs = getDroppedWebflowFieldSlugs(originalFieldData, safeFieldData)

  return {
    request: {
      ...request,
      body: compactObject({
        ...originalBody,
        fieldData: safeFieldData,
      }),
      fieldData: safeFieldData,
      finalFieldData: safeFieldData,
    },
    droppedFieldSlugs,
  }
}

function sanitizePublishItemForConfirmedWebflow(item: PublishItem): { item: PublishItem; droppedFieldSlugs: string[] } {
  const sanitized = sanitizePublishRequestForConfirmedWebflow(asRecord(item.request))
  return {
    item: {
      ...item,
      request: sanitized.request,
    } satisfies PublishItem,
    droppedFieldSlugs: sanitized.droppedFieldSlugs,
  }
}

function safeErrorMessage(error: unknown, maxLength = 5000) {
  let raw = "Unknown error"
  if (error instanceof Error) raw = error.message
  else if (typeof error === "string") raw = error
  else if (isRecord(error)) {
    const parts = [
      error.message ? `message=${String(error.message)}` : "",
      error.details ? `details=${String(error.details)}` : "",
      error.hint ? `hint=${String(error.hint)}` : "",
      error.code ? `code=${String(error.code)}` : "",
    ].filter(Boolean)

    if (parts.length) raw = parts.join(" | ")
    else {
      try {
        raw = JSON.stringify(error)
      } catch {
        raw = Object.prototype.toString.call(error)
      }
    }
  } else if (error !== undefined && error !== null) raw = String(error)

  return raw.length > maxLength ? `${raw.slice(0, maxLength)}... [truncated]` : raw
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function logJson(payload: JsonRecord, level: "debug" | "info" | "warn" | "error" = "info") {
  const configured = envValue("LOG_LEVEL") || "info"
  const order = { debug: 10, info: 20, warn: 30, error: 40 }
  if (order[level] < (order as Record<string, number>)[configured] && configured !== "debug") return
  console.log(JSON.stringify({ time: new Date().toISOString(), level, ...payload }, null, 2))
}

function buildQuery(params: unknown) {
  const search = new URLSearchParams()
  const record = asRecord(params)

  for (const [key, value] of Object.entries(record)) {
    if (value === undefined || value === null || value === "") continue
    if (Array.isArray(value)) {
      for (const item of value) search.append(key, String(item))
    } else {
      search.set(key, String(value))
    }
  }

  const query = search.toString()
  return query ? `?${query}` : ""
}

function normalizeUrl(base: string, path: string, query?: unknown) {
  const cleanBase = String(base || "").replace(/\/+$/, "")
  const cleanPath = String(path || "").startsWith("/") ? String(path || "") : `/${String(path || "")}`
  return `${cleanBase}${cleanPath}${buildQuery(query)}`
}

async function fetchWithTimeout(url: string, init: RequestInit = {}, label = "request", timeoutMs = 30_000) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), timeoutMs)

  try {
    return await fetch(url, { ...init, signal: controller.signal })
  } catch (error) {
    if (controller.signal.aborted || /abort|timeout|timed out/i.test(safeErrorMessage(error))) {
      throw new Error(`${label} timed out after ${Math.round(timeoutMs / 1000)}s.`)
    }
    throw error
  } finally {
    clearTimeout(timeout)
  }
}

async function retry<T>(label: string, maxAttempts: number, fn: (attempt: number) => Promise<T>) {
  let lastError: unknown = null

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn(attempt)
    } catch (error) {
      lastError = error
      const message = safeErrorMessage(error)
      const retryable = /timeout|timed out|429|rate|temporar|502|503|504|network|fetch failed|ECONNRESET/i.test(message)

      logJson({ event: "retryable_failure", label, attempt, maxAttempts, retryable, error: message }, retryable && attempt < maxAttempts ? "warn" : "error")

      if (!retryable || attempt >= maxAttempts) break
      await sleep(Math.min(30_000, 1000 * 2 ** (attempt - 1)) + Math.floor(Math.random() * 750))
    }
  }

  throw lastError instanceof Error ? lastError : new Error(safeErrorMessage(lastError))
}

async function readJsonResponse(response: Response) {
  const text = await response.text()
  if (!text.trim()) return {}
  try {
    return JSON.parse(text)
  } catch {
    return { raw: text }
  }
}

function edgeHeaders() {
  const workerSecret = getRequiredEnv("WORKER_SECRET")
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${workerSecret}`,
    "x-worker-secret": workerSecret,
  }
}

async function callEdge<T extends EdgeResponse = EdgeResponse>(payload: JsonRecord) {
  const url = getRequiredEnv("EDGE_FUNCTION_URL")
  const timeoutMs = numberEnv("EDGE_REQUEST_TIMEOUT_SECONDS", DEFAULT_EDGE_REQUEST_TIMEOUT_SECONDS, 10, 900) * 1000
  const maxRetries = numberEnv("EDGE_MAX_RETRIES", 3, 1, 10)

  return await retry(`edge:${firstString(payload.action) || "unknown"}`, maxRetries, async () => {
    const response = await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: edgeHeaders(),
        body: JSON.stringify(payload),
      },
      `Edge action ${firstString(payload.action) || "unknown"}`,
      timeoutMs
    )

    const data = await readJsonResponse(response) as T

    if (!response.ok || data.success === false) {
      throw new Error(`${firstString(data.error, data.message, response.statusText)}\nStatus: ${response.status}\nResponse: ${JSON.stringify(data).slice(0, 3000)}`)
    }

    return data
  })
}

function supabaseRestHeaders() {
  const key = getRequiredEnv("SUPABASE_SERVICE_ROLE_KEY")
  return {
    apikey: key,
    Authorization: `Bearer ${key}`,
    "Content-Type": "application/json",
  }
}

async function supabaseRest<T = unknown>(pathAndQuery: string, init: RequestInit = {}) {
  const supabaseUrl = getRequiredEnv("SUPABASE_URL").replace(/\/+$/, "")
  const url = `${supabaseUrl}/rest/v1/${pathAndQuery.replace(/^\/+/, "")}`
  const timeoutMs = numberEnv("EDGE_REQUEST_TIMEOUT_SECONDS", DEFAULT_EDGE_REQUEST_TIMEOUT_SECONDS, 10, 900) * 1000

  const response = await fetchWithTimeout(
    url,
    {
      ...init,
      headers: {
        ...supabaseRestHeaders(),
        Prefer: "return=representation",
        ...(init.headers || {}),
      },
    },
    "Supabase REST",
    timeoutMs
  )

  const data = await readJsonResponse(response)
  if (!response.ok) throw new Error(`Supabase REST failed: ${response.status} ${JSON.stringify(data).slice(0, 3000)}`)
  return data as T
}

function normalizeJobRecord(value: unknown): BatchJob | null {
  const record = asRecord(value)
  const id = firstString(record.id)
  const status = firstString(record.status) as JobStatus
  if (!id || !status) return null

  return {
    id,
    status,
    updated_at: firstString(record.updated_at),
    body_batch_id: firstString(record.body_batch_id) || null,
    field_batch_id: firstString(record.field_batch_id) || null,
  }
}

async function discoverJobs() {
  const explicitJobIds = [envValue("BATCH_JOB_ID"), envValue("JOB_ID")]
    .flatMap((value) => value.split(","))
    .map((value) => value.trim())
    .filter(Boolean)

  if (explicitJobIds.length) return explicitJobIds.map((id) => ({ id, status: "queued" as JobStatus }))

  if (!boolEnv("AUTO_DISCOVER_JOBS", true)) {
    logJson({ event: "no_job_discovery", message: "Set BATCH_JOB_ID or enable AUTO_DISCOVER_JOBS with SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY." }, "warn")
    return []
  }

  if (!envValue("SUPABASE_URL") || !envValue("SUPABASE_SERVICE_ROLE_KEY")) {
    logJson({ event: "missing_discovery_env", message: "AUTO_DISCOVER_JOBS is enabled, but SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY is missing. Set BATCH_JOB_ID for manual mode." }, "warn")
    return []
  }

  const statuses = [
    "queued",
    "body_batch_preparing",
    "body_batch_submitted",
    "body_batch_running",
    "body_batch_completed",
    "field_batch_preparing",
    "field_batch_submitted",
    "field_batch_running",
    "field_batch_completed",
    "publishing",
  ].join(",")

  const rows = await supabaseRest<JsonRecord[]>(
    `webflow_batch_blog_creation_jobs?select=id,status,updated_at,body_batch_id,field_batch_id&status=in.(${statuses})&order=updated_at.asc&limit=5`
  )

  return rows.map(normalizeJobRecord).filter((job): job is BatchJob => Boolean(job))
}

async function getJobDirect(jobId: string) {
  if (!envValue("SUPABASE_URL") || !envValue("SUPABASE_SERVICE_ROLE_KEY")) return null
  const rows = await supabaseRest<JsonRecord[]>(
    `webflow_batch_blog_creation_jobs?select=id,status,updated_at,body_batch_id,field_batch_id&id=eq.${encodeURIComponent(jobId)}&limit=1`
  )
  return normalizeJobRecord(rows[0])
}

function edgeJobStatus(response: EdgeResponse): JobStatus | "" {
  return firstString(asRecord(response.job).status) as JobStatus | ""
}

function shouldThrottleBatchPoll(jobId: string, phase: BatchPhase) {
  const minPollMs = numberEnv("OPENAI_BATCH_MIN_POLL_INTERVAL_MS", DEFAULT_OPENAI_BATCH_MIN_POLL_INTERVAL_MS, 10_000, 3_600_000)
  const key = `${jobId}:${phase}`
  const now = Date.now()
  const last = lastBatchPollAtByJobPhase.get(key) || 0
  if (now - last < minPollMs) return true
  lastBatchPollAtByJobPhase.set(key, now)
  return false
}

async function submitOpenAIBatch(jobId: string, phase: BatchPhase) {
  const maxRows = numberEnv("BATCH_MAX_ROWS", DEFAULT_BATCH_MAX_ROWS, 1, 50_000)
  logJson({ event: "submit_openai_batch", jobId, phase, maxRows })
  return await callEdge({
    action: "submit_openai_batch",
    jobId,
    phase,
    maxRows,
    includeRows: false,
  })
}

async function pollOpenAIBatch(jobId: string, phase: BatchPhase) {
  if (shouldThrottleBatchPoll(jobId, phase)) {
    logJson({ event: "poll_throttled", jobId, phase }, "debug")
    return null
  }

  logJson({ event: "poll_openai_batch", jobId, phase })
  return await callEdge({
    action: "poll_openai_batch",
    jobId,
    phase,
    autoParse: true,
    includeRows: false,
  })
}

async function claimPublishRows(jobId: string) {
  const maxRows = numberEnv("PUBLISH_BATCH_SIZE", DEFAULT_PUBLISH_BATCH_SIZE, 1, 100)
  logJson({ event: "claim_publish_rows", jobId, maxRows })
  return await callEdge({
    action: "claim_publish_rows",
    jobId,
    workerId: envValue("WORKER_ID") || "render-batch-blog-worker",
    maxRows,
    includeRows: false,
  })
}

function shouldSkipWebflowPublish(item: PublishItem) {
  const request = asRecord(item.request)
  return request.execute === false || request.allowPublish === false
}

function extractWebflowItemId(value: unknown) {
  const record = asRecord(value)
  const data = asRecord(record.data)
  const items = Array.isArray(record.items) ? record.items : []
  const firstItem = asRecord(items[0])
  return firstString(record.id, record._id, data.id, data._id, firstItem.id, firstItem._id)
}

async function postToWebflow(item: PublishItem) {
  const sanitized = sanitizePublishRequestForConfirmedWebflow(asRecord(item.request))
  const request = sanitized.request
  const apiBase = firstString(request.apiBase, DEFAULT_WEBFLOW_API_BASE)
  const path = firstString(request.path)
  const method = firstString(request.method, "POST")
  const query = asRecord(request.query)
  const body = asRecord(request.body)

  if (!path) throw new Error("Publish item is missing request.path.")

  const url = normalizeUrl(apiBase, path, query)
  const timeoutMs = numberEnv("WEBFLOW_REQUEST_TIMEOUT_SECONDS", DEFAULT_WEBFLOW_REQUEST_TIMEOUT_SECONDS, 10, 600) * 1000
  const maxRetries = numberEnv("WEBFLOW_MAX_RETRIES", 4, 1, 10)
  const token = getRequiredEnv("WEBFLOW_API_TOKEN")

  return await retry("webflow_publish", maxRetries, async () => {
    const response = await fetchWithTimeout(
      url,
      {
        method,
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      },
      "Webflow publish",
      timeoutMs
    )

    const data = await readJsonResponse(response)
    if (!response.ok) {
      const message = firstString(asRecord(data).message, asRecord(data).msg, response.statusText)
      throw new Error(`Webflow publish failed: ${response.status} ${message}\n${JSON.stringify(data).slice(0, 3000)}`)
    }

    return data as JsonRecord
  })
}

async function completePublish(item: PublishItem, webflowResponse: JsonRecord, skipped = false) {
  const row = asRecord(item.row)
  const publishAttempt = asRecord(item.publishAttempt)
  const rowId = firstString(row.id)
  if (!rowId) throw new Error("Cannot complete publish because row.id is missing.")

  const finalFieldData = asRecord(item.request).fieldData || asRecord(item.request).finalFieldData || {}
  const webflowItemId = extractWebflowItemId(webflowResponse)

  await callEdge({
    action: "complete_publish",
    rowId,
    publishAttemptId: firstString(publishAttempt.id),
    finalFieldData,
    webflowItemId,
    webflowResponse: skipped
      ? { skipped: true, reason: "execute_or_allow_publish_false", request: item.request }
      : webflowResponse,
    includeRows: false,
  })

  return { ok: true, rowId, webflowItemId, skipped } satisfies PublishResult
}

async function failPublish(item: PublishItem, error: unknown) {
  const row = asRecord(item.row)
  const publishAttempt = asRecord(item.publishAttempt)
  const rowId = firstString(row.id)
  const message = safeErrorMessage(error)

  if (!rowId) {
    logJson({ event: "publish_fail_missing_row", error: message }, "error")
    return { ok: false, rowId: "", error: message } satisfies PublishResult
  }

  await callEdge({
    action: "fail_publish",
    rowId,
    publishAttemptId: firstString(publishAttempt.id),
    error: message,
    errorPayload: isRecord(error) ? error : { message },
    includeRows: false,
  })

  return { ok: false, rowId, error: message } satisfies PublishResult
}

async function processPublishItem(item: PublishItem) {
  const rowId = firstString(asRecord(item.row).id)
  const title = firstString(asRecord(item.row).webflow_name, asRecord(item.row).name_seed)
  const sanitized = sanitizePublishItemForConfirmedWebflow(item)
  const safeItem = sanitized.item

  if (sanitized.droppedFieldSlugs.length) {
    logJson({
      event: "webflow_fielddata_confirmed_sanitized",
      rowId,
      title,
      droppedFieldSlugs: sanitized.droppedFieldSlugs,
      allowedFieldSlugs: Array.from(CSV_SAFE_WEBFLOW_FIELD_SLUGS),
    }, "warn")
  }

  try {
    if (shouldSkipWebflowPublish(safeItem)) {
      logJson({ event: "publish_skipped", rowId, title })
      return await completePublish(safeItem, { skipped: true, title }, true)
    }

    logJson({ event: "webflow_publish_start", rowId, title })
    const webflowResponse = await postToWebflow(safeItem)
    const result = await completePublish(safeItem, webflowResponse, false)
    logJson({ event: "webflow_publish_success", rowId, webflowItemId: result.webflowItemId })
    return result
  } catch (error) {
    const result = await failPublish(safeItem, error)
    logJson({ event: "webflow_publish_error", rowId, error: result.error }, "error")
    return result
  }
}

async function runLimited<T, R>(items: T[], limit: number, handler: (item: T) => Promise<R>) {
  const results: R[] = []
  let nextIndex = 0

  async function worker() {
    while (nextIndex < items.length && !shutdownState.shuttingDown) {
      const currentIndex = nextIndex++
      results[currentIndex] = await handler(items[currentIndex])
    }
  }

  const workers = Array.from({ length: Math.max(1, Math.min(limit, items.length)) }, () => worker())
  await Promise.all(workers)
  return results
}

async function publishReadyRows(jobId: string) {
  const response = await claimPublishRows(jobId)
  const items = Array.isArray(response.publishItems) ? response.publishItems : []
  if (!items.length) {
    logJson({ event: "no_publish_rows", jobId })
    return { claimed: 0, results: [] as PublishResult[] }
  }

  const concurrency = numberEnv("PUBLISH_CONCURRENCY", DEFAULT_PUBLISH_CONCURRENCY, 1, 25)
  const results = await runLimited(items, concurrency, processPublishItem)
  return { claimed: items.length, results }
}

async function handleJob(job: BatchJob) {
  const jobId = job.id
  let status = job.status

  if (envValue("BATCH_JOB_ID") || envValue("JOB_ID")) {
    const direct = await getJobDirect(jobId)
    if (direct?.status) status = direct.status
  }

  logJson({ event: "job_tick", jobId, status })

  if (["completed", "completed_with_errors", "cancelled", "error", "draft"].includes(status)) {
    logJson({ event: "job_terminal_or_not_runnable", jobId, status })
    return { didWork: false, status }
  }

  if (status === "queued" || status === "body_batch_preparing") {
    const response = await submitOpenAIBatch(jobId, "body_generation")
    return { didWork: true, status: edgeJobStatus(response) || "body_batch_submitted" }
  }

  if (status === "body_batch_submitted" || status === "body_batch_running") {
    const response = await pollOpenAIBatch(jobId, "body_generation")
    return { didWork: Boolean(response), status: response ? edgeJobStatus(response) || status : status }
  }

  if (status === "body_batch_completed" || status === "field_batch_preparing") {
    const response = await submitOpenAIBatch(jobId, "title_and_field_generation")
    return { didWork: true, status: edgeJobStatus(response) || "field_batch_submitted" }
  }

  if (status === "field_batch_submitted" || status === "field_batch_running") {
    const response = await pollOpenAIBatch(jobId, "title_and_field_generation")
    return { didWork: Boolean(response), status: response ? edgeJobStatus(response) || status : status }
  }

  if (status === "field_batch_completed" || status === "publishing") {
    const publish = await publishReadyRows(jobId)
    return { didWork: publish.claimed > 0, status: "publishing", publish }
  }

  logJson({ event: "unhandled_job_status", jobId, status }, "warn")
  return { didWork: false, status }
}

async function tick() {
  const jobs = await discoverJobs()
  if (!jobs.length) {
    logJson({ event: "idle", message: "No runnable batch blog jobs found." })
    return { didWork: false }
  }

  let didAnyWork = false
  for (const job of jobs) {
    if (shutdownState.shuttingDown) break
    const result = await handleJob(job)
    didAnyWork = didAnyWork || Boolean(result.didWork)
  }

  return { didWork: didAnyWork }
}

function installShutdownHandlers() {
  const shutdown = (signal: string) => {
    if (shutdownState.shuttingDown) return
    shutdownState.shuttingDown = true
    logJson({ event: "shutdown_requested", signal }, "warn")
  }

  process.on("SIGINT", () => shutdown("SIGINT"))
  process.on("SIGTERM", () => shutdown("SIGTERM"))
  process.on("uncaughtException", (error) => {
    logJson({ event: "uncaught_exception", error: safeErrorMessage(error) }, "error")
    shutdownState.shuttingDown = true
    process.exitCode = 1
  })
  process.on("unhandledRejection", (error) => {
    logJson({ event: "unhandled_rejection", error: safeErrorMessage(error) }, "error")
    shutdownState.shuttingDown = true
    process.exitCode = 1
  })
}

async function mainLoop() {
  installShutdownHandlers()

  if (!boolEnv("WORKER_ENABLED", true)) {
    logJson({ event: "worker_disabled" }, "warn")
    return
  }

  getRequiredEnv("EDGE_FUNCTION_URL")
  getRequiredEnv("WORKER_SECRET")
  getRequiredEnv("WEBFLOW_API_TOKEN")

  logJson({
    event: "worker_started",
    workerId: envValue("WORKER_ID") || "render-batch-blog-worker",
    mode: envValue("BATCH_JOB_ID") || envValue("JOB_ID") ? "specific_job" : "auto_discovery",
    edgeFunctionUrl: getRequiredEnv("EDGE_FUNCTION_URL"),
    autoDiscoverJobs: boolEnv("AUTO_DISCOVER_JOBS", true),
    batchMaxRows: numberEnv("BATCH_MAX_ROWS", DEFAULT_BATCH_MAX_ROWS, 1, 50_000),
    publishBatchSize: numberEnv("PUBLISH_BATCH_SIZE", DEFAULT_PUBLISH_BATCH_SIZE, 1, 100),
    publishConcurrency: numberEnv("PUBLISH_CONCURRENCY", DEFAULT_PUBLISH_CONCURRENCY, 1, 25),
  })

  while (!shutdownState.shuttingDown) {
    try {
      const result = await tick()
      const sleepMs = result.didWork
        ? numberEnv("WORKER_POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL_MS, 5_000, 3_600_000)
        : numberEnv("WORKER_IDLE_POLL_INTERVAL_MS", DEFAULT_IDLE_POLL_INTERVAL_MS, 10_000, 3_600_000)

      if (!boolEnv("KEEP_ALIVE_WHEN_DONE", true) && !result.didWork) {
        logJson({ event: "no_work_exit", message: "KEEP_ALIVE_WHEN_DONE is false and no work was found." })
        return
      }

      await sleep(sleepMs)
    } catch (error) {
      logJson({ event: "tick_error", error: safeErrorMessage(error) }, "error")
      await sleep(numberEnv("WORKER_ERROR_SLEEP_MS", DEFAULT_ERROR_SLEEP_MS, 5_000, 600_000))
    }
  }

  logJson({ event: "worker_stopped" }, "warn")
}

void mainLoop()
