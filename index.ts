type JsonRecord = Record<string, unknown>

type BatchPhase = "body_generation" | "title_and_field_generation"

type PublishItem = {
  row?: JsonRecord
  publishAttempt?: JsonRecord
  request?: JsonRecord
  workerId?: string
}

const DEFAULT_EDGE_REQUEST_TIMEOUT_SECONDS = 120
const DEFAULT_WEBFLOW_REQUEST_TIMEOUT_SECONDS = 120
const DEFAULT_LOOP_SLEEP_MS = 5000
const DEFAULT_IDLE_SLEEP_MS = 60000
const DEFAULT_ERROR_SLEEP_MS = 30000
const DEFAULT_PUBLISH_BATCH_SIZE = 25
const DEFAULT_PUBLISH_CONCURRENCY = 5
const MAX_PUBLISH_BATCH_SIZE = 100
const MAX_PUBLISH_CONCURRENCY = 25
const DEFAULT_WEBFLOW_API_BASE = "https://api.webflow.com/v2"

const TERMINAL_JOB_STATUSES = new Set(["completed", "completed_with_errors", "cancelled", "error"])

let shuttingDown = false

function envValue(name: string) {
  const value = process.env[name]
  return value && String(value).trim() ? String(value).trim() : ""
}

function getRequiredEnv(name: string) {
  const value = envValue(name)
  if (!value) throw new Error(`Missing required environment variable: ${name}`)
  return value
}

function getRequiredAnyEnv(names: string[]) {
  for (const name of names) {
    const value = envValue(name)
    if (value) return value
  }
  throw new Error(`Missing required environment variable. Set one of: ${names.join(", ")}`)
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

function listEnv(name: string) {
  return envValue(name)
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean)
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

function firstString(...values: unknown[]) {
  for (const value of values) {
    const text = stringifyValue(value).trim()
    if (text) return text
  }
  return ""
}

function isRecord(value: unknown): value is JsonRecord {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value)
}

function asRecord(value: unknown, fallback: JsonRecord = {}) {
  return isRecord(value) ? value : fallback
}

function cleanPlainObject(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(cleanPlainObject)
  if (!isRecord(value)) return value

  const output: JsonRecord = {}
  for (const [key, innerValue] of Object.entries(value)) {
    if (["__proto__", "constructor", "prototype"].includes(key)) continue
    output[key] = cleanPlainObject(innerValue)
  }
  return output
}

function safeErrorMessage(error: unknown, maxLength = 5000) {
  let raw = "Unknown error."

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
  console.log(JSON.stringify({ time: new Date().toISOString(), level, ...payload }))
}

function buildQuery(params: unknown) {
  const record = asRecord(params)
  const search = new URLSearchParams()

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

async function fetchWithTimeout(url: string, init: RequestInit = {}, label = "request", timeoutMs = 30000) {
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

async function callJson(
  url: string,
  init: RequestInit = {},
  options: {
    label?: string
    timeoutMs?: number
    retries?: number
    retryStatuses?: number[]
  } = {}
) {
  const {
    label = "request",
    timeoutMs = 30000,
    retries = 1,
    retryStatuses = [408, 409, 425, 429, 500, 502, 503, 504],
  } = options

  let lastError: unknown = null

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      const response = await fetchWithTimeout(url, init, label, timeoutMs)
      const text = await response.text()
      let data: unknown = null

      try {
        data = text ? JSON.parse(text) : null
      } catch {
        data = { raw: text.slice(0, 4000) }
      }

      if (!response.ok) {
        const shouldRetry = attempt < retries - 1 && retryStatuses.includes(response.status)
        if (shouldRetry) {
          const retryAfter = Number(response.headers.get("retry-after") || 0)
          const waitMs = retryAfter
            ? Math.min(retryAfter * 1000, 30000)
            : Math.min(1000 * Math.pow(2, attempt), 15000) + Math.floor(Math.random() * 250)
          await sleep(waitMs)
          continue
        }

        throw new Error(`${label} failed with HTTP ${response.status}: ${JSON.stringify(data).slice(0, 5000)}`)
      }

      return data
    } catch (error) {
      lastError = error
      if (attempt < retries - 1) {
        await sleep(Math.min(1000 * Math.pow(2, attempt), 15000) + Math.floor(Math.random() * 250))
        continue
      }
      throw lastError
    }
  }

  throw lastError || new Error(`${label} failed.`)
}

const config = {
  edgeFunctionUrl: getRequiredEnv("EDGE_FUNCTION_URL"),
  workerSecret: getRequiredAnyEnv([
    "WORKER_SECRET",
    "WEBFLOW_BLOG_BATCH_CREATION_WORKER_SECRET",
    "WEBFLOW_BLOG_CREATION_WORKER_SECRET",
  ]),
  workerId: envValue("WORKER_ID") || `webflow-batch-blog-worker-${process.pid}`,
  jobIds: [...listEnv("JOB_IDS"), ...listEnv("BATCH_JOB_IDS"), envValue("JOB_ID"), envValue("BATCH_JOB_ID")].filter(Boolean),
  webflowToken: envValue("WEBFLOW_API_TOKEN") || envValue("WEBFLOW_OAUTH_TOKEN"),
  webflowApiBase: envValue("WEBFLOW_API_BASE") || DEFAULT_WEBFLOW_API_BASE,
  workerEnabled: boolEnv("WORKER_ENABLED", true),
  autoSubmitBodyBatch: boolEnv("AUTO_SUBMIT_BODY_BATCH", true),
  autoSubmitFieldBatch: boolEnv("AUTO_SUBMIT_FIELD_BATCH", true),
  autoPublish: boolEnv("AUTO_PUBLISH", true),
  keepAliveWhenDone: boolEnv("KEEP_ALIVE_WHEN_DONE", true),
  stopOnFatalError: boolEnv("STOP_ON_FATAL_ERROR", false),
  publishBatchSize: numberEnv("PUBLISH_BATCH_SIZE", DEFAULT_PUBLISH_BATCH_SIZE, 1, MAX_PUBLISH_BATCH_SIZE),
  publishConcurrency: numberEnv("PUBLISH_CONCURRENCY", DEFAULT_PUBLISH_CONCURRENCY, 1, MAX_PUBLISH_CONCURRENCY),
  loopSleepMs: numberEnv("WORKER_LOOP_SLEEP_MS", DEFAULT_LOOP_SLEEP_MS, 250, 300000),
  idleSleepMs: numberEnv("WORKER_IDLE_SLEEP_MS", DEFAULT_IDLE_SLEEP_MS, 1000, 900000),
  errorSleepMs: numberEnv("WORKER_ERROR_SLEEP_MS", DEFAULT_ERROR_SLEEP_MS, 1000, 900000),
  edgeRequestTimeoutMs: numberEnv("EDGE_REQUEST_TIMEOUT_SECONDS", DEFAULT_EDGE_REQUEST_TIMEOUT_SECONDS, 10, 900) * 1000,
  webflowRequestTimeoutMs: numberEnv("WEBFLOW_REQUEST_TIMEOUT_SECONDS", DEFAULT_WEBFLOW_REQUEST_TIMEOUT_SECONDS, 10, 300) * 1000,
  edgeMaxRetries: numberEnv("EDGE_MAX_RETRIES", 3, 1, 8),
  webflowMaxRetries: numberEnv("WEBFLOW_MAX_RETRIES", 4, 1, 10),
}

async function callEdge(action: string, extra: JsonRecord = {}) {
  const body = cleanPlainObject({
    action,
    workerId: config.workerId,
    maxRows: config.publishBatchSize,
    includeRows: false,
    ...extra,
  })

  const data = await callJson(
    config.edgeFunctionUrl,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-worker-secret": config.workerSecret,
      },
      body: JSON.stringify(body),
    },
    {
      label: `Edge Function ${action}`,
      timeoutMs: config.edgeRequestTimeoutMs,
      retries: config.edgeMaxRetries,
    }
  )

  const record = asRecord(data)
  if (record.success === false) {
    throw new Error(`Edge Function ${action} failed: ${JSON.stringify(record).slice(0, 5000)}`)
  }

  return record
}

function getJobStatus(data: JsonRecord) {
  return firstString(asRecord(data.job).status, data.status)
}

function getJobId(data: JsonRecord) {
  return firstString(data.jobId, asRecord(data.job).id)
}

function getBatches(data: JsonRecord) {
  return Array.isArray(data.batches) ? data.batches.map((batch) => asRecord(batch)) : []
}

function latestBatchForPhase(data: JsonRecord, phase: BatchPhase) {
  return getBatches(data).find((batch) => firstString(batch.phase) === phase) || null
}

function batchNeedsSubmit(batch: JsonRecord | null) {
  if (!batch) return true
  return ["draft", "input_file_created"].includes(firstString(batch.status).toLowerCase())
}

async function getWorkerJob(jobId = "") {
  return await callEdge(jobId ? "get_worker_job" : "get_next_worker_job", jobId ? { jobId } : {})
}

async function submitBatch(jobId: string, phase: BatchPhase, batch: JsonRecord | null) {
  const payload: JsonRecord = { jobId, phase, includeRows: false }
  if (batch && batchNeedsSubmit(batch)) payload.batchId = firstString(batch.id)

  logJson({ event: "submit_openai_batch_start", jobId, phase, batchId: payload.batchId || null })
  const result = await callEdge("submit_openai_batch", payload)
  logJson({ event: "submit_openai_batch_done", jobId, phase, status: getJobStatus(result) })
  return result
}

async function pollBatch(jobId: string, phase: BatchPhase, batch: JsonRecord | null) {
  const payload: JsonRecord = { jobId, phase, autoParse: true, includeRows: false }
  const openaiBatchId = firstString(batch?.openaiBatchId, batch?.openai_batch_id)
  const batchId = firstString(batch?.id)
  if (openaiBatchId) payload.openaiBatchId = openaiBatchId
  else if (batchId) payload.batchId = batchId

  logJson({ event: "poll_openai_batch_start", jobId, phase, batchId: batchId || null, openaiBatchId: openaiBatchId || null })
  const result = await callEdge("poll_openai_batch", payload)
  logJson({ event: "poll_openai_batch_done", jobId, phase, action: result.action || null, status: getJobStatus(result) })
  return result
}

function getWebflowItemId(data: unknown) {
  const record = asRecord(data)
  const responseData = asRecord(record.data)
  const result = asRecord(record.result)
  const resultData = asRecord(result.data)
  const items = Array.isArray(record.items) ? record.items : []
  const dataItems = Array.isArray(responseData.items) ? responseData.items : []

  return firstString(
    responseData.id,
    responseData._id,
    record.id,
    record._id,
    resultData.id,
    resultData._id,
    result.id,
    result._id,
    asRecord(items[0]).id,
    asRecord(items[0])._id,
    asRecord(dataItems[0]).id,
    asRecord(dataItems[0])._id
  )
}

async function webflowRequest(request: JsonRecord) {
  if (!config.webflowToken) throw new Error("Missing WEBFLOW_API_TOKEN or WEBFLOW_OAUTH_TOKEN for Webflow publish phase.")

  const method = firstString(request.method, "POST")
  const path = firstString(request.path)
  if (!path) throw new Error("Webflow publish request is missing path.")

  const apiBase = firstString(request.apiBase, request.api_base, config.webflowApiBase)
  const url = normalizeUrl(apiBase, path, request.query)
  const body = request.body === undefined ? undefined : cleanPlainObject(request.body)

  return await callJson(
    url,
    {
      method,
      headers: {
        Authorization: `Bearer ${config.webflowToken}`,
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    },
    {
      label: `Webflow ${method} ${path}`,
      timeoutMs: config.webflowRequestTimeoutMs,
      retries: config.webflowMaxRetries,
    }
  )
}

async function processPublishItem(item: PublishItem) {
  const row = asRecord(item.row)
  const publishAttempt = asRecord(item.publishAttempt)
  const request = asRecord(item.request)
  const rowId = firstString(row.id)
  const publishAttemptId = firstString(publishAttempt.id)
  const fieldData = asRecord(request.fieldData, asRecord(asRecord(request.body).fieldData))

  if (!rowId) throw new Error("Publish item is missing row.id.")

  try {
    if (request.execute === false) {
      await callEdge("complete_publish", {
        rowId,
        publishAttemptId,
        finalFieldData: fieldData,
        webflowResponse: { dryRun: true, message: "Dry run enabled. Webflow item was not created." },
        includeRows: false,
      })

      return { ok: true, rowId, publishAttemptId, dryRun: true }
    }

    if (request.publishMode === "live" && request.allowPublish === false) {
      throw new Error("Publishing is disabled for this job, but publishMode is live.")
    }

    const webflowResponse = await webflowRequest(request)
    const webflowItemId = getWebflowItemId(webflowResponse)

    await callEdge("complete_publish", {
      rowId,
      publishAttemptId,
      finalFieldData: fieldData,
      webflowItemId,
      webflowResponse: webflowResponse as JsonRecord,
      includeRows: false,
    })

    return { ok: true, rowId, publishAttemptId, webflowItemId }
  } catch (error) {
    const message = safeErrorMessage(error)
    await callEdge("fail_publish", {
      rowId,
      publishAttemptId,
      error: message,
      errorPayload: { message, request: cleanPlainObject(request) },
      includeRows: false,
    }).catch((failError) => {
      logJson({ event: "fail_publish_report_failed", rowId, publishAttemptId, error: safeErrorMessage(failError) }, "error")
    })

    return { ok: false, rowId, publishAttemptId, error: message }
  }
}

async function processWithConcurrency<T, R>(items: T[], concurrency: number, worker: (item: T) => Promise<R>) {
  const results: R[] = []
  let index = 0

  async function run() {
    while (index < items.length) {
      const current = index++
      results[current] = await worker(items[current])
    }
  }

  await Promise.all(Array.from({ length: Math.min(Math.max(1, concurrency), items.length) }, () => run()))
  return results
}

async function claimAndPublish(jobId: string) {
  if (!config.autoPublish) {
    logJson({ event: "publish_skipped_disabled", jobId }, "warn")
    return { worked: false, done: false }
  }

  const claim = await callEdge("claim_publish_rows", {
    jobId,
    maxRows: config.publishBatchSize,
    includeRows: false,
  })

  const publishItems = Array.isArray(claim.publishItems) ? claim.publishItems.map((item) => asRecord(item)) as PublishItem[] : []
  const claimedTotal = Number(claim.claimedTotal || publishItems.length || 0)
  logJson({ event: "claim_publish_rows_done", jobId, claimedTotal, publishItems: publishItems.length })

  if (!publishItems.length) return { worked: false, done: false }

  const results = await processWithConcurrency(publishItems, config.publishConcurrency, processPublishItem)
  const successCount = results.filter((result: any) => result.ok).length
  const errorCount = results.length - successCount

  logJson({ event: "publish_batch_done", jobId, processedCount: results.length, successCount, errorCount })
  return { worked: results.length > 0, done: false }
}

async function processJob(jobData: JsonRecord) {
  const jobId = getJobId(jobData)
  const status = getJobStatus(jobData)

  if (!jobId || !status) return { worked: false, done: true }
  if (TERMINAL_JOB_STATUSES.has(status)) return { worked: false, done: true }

  logJson({ event: "job_status", jobId, status })

  if (["queued"].includes(status)) {
    if (!config.autoSubmitBodyBatch) return { worked: false, done: false }
    return { worked: Boolean(await submitBatch(jobId, "body_generation", latestBatchForPhase(jobData, "body_generation"))), done: false }
  }

  if (["body_batch_preparing"].includes(status)) {
    if (!config.autoSubmitBodyBatch) return { worked: false, done: false }
    return { worked: Boolean(await submitBatch(jobId, "body_generation", latestBatchForPhase(jobData, "body_generation"))), done: false }
  }

  if (["body_batch_submitted", "body_batch_running"].includes(status)) {
    return { worked: Boolean(await pollBatch(jobId, "body_generation", latestBatchForPhase(jobData, "body_generation"))), done: false }
  }

  if (["body_batch_completed"].includes(status)) {
    if (!config.autoSubmitFieldBatch) return { worked: false, done: false }
    return { worked: Boolean(await submitBatch(jobId, "title_and_field_generation", latestBatchForPhase(jobData, "title_and_field_generation"))), done: false }
  }

  if (["field_batch_preparing"].includes(status)) {
    if (!config.autoSubmitFieldBatch) return { worked: false, done: false }
    return { worked: Boolean(await submitBatch(jobId, "title_and_field_generation", latestBatchForPhase(jobData, "title_and_field_generation"))), done: false }
  }

  if (["field_batch_submitted", "field_batch_running"].includes(status)) {
    return { worked: Boolean(await pollBatch(jobId, "title_and_field_generation", latestBatchForPhase(jobData, "title_and_field_generation"))), done: false }
  }

  if (["field_batch_completed", "publishing"].includes(status)) {
    return await claimAndPublish(jobId)
  }

  logJson({ event: "unhandled_job_status", jobId, status }, "warn")
  return { worked: false, done: false }
}

async function runLoopOnce() {
  const jobIds = config.jobIds.length ? config.jobIds : [""]
  let anyWorked = false
  let anyActive = false

  for (const jobId of jobIds) {
    const jobData = await getWorkerJob(jobId)
    const activeJobId = getJobId(jobData)

    if (!activeJobId) {
      logJson({ event: "no_active_job", requestedJobId: jobId || null })
      continue
    }

    const result = await processJob(jobData)
    anyWorked = anyWorked || result.worked
    anyActive = anyActive || !result.done
  }

  return { worked: anyWorked, done: !anyActive }
}

function validateRuntimeConfig() {
  if (!config.edgeFunctionUrl.startsWith("http")) {
    throw new Error("EDGE_FUNCTION_URL must be a full Supabase Edge Function URL.")
  }

  logJson({
    event: "worker_config_validated",
    workerId: config.workerId,
    edgeFunctionUrl: config.edgeFunctionUrl,
    explicitJobIds: config.jobIds,
    publishBatchSize: config.publishBatchSize,
    publishConcurrency: config.publishConcurrency,
    autoSubmitBodyBatch: config.autoSubmitBodyBatch,
    autoSubmitFieldBatch: config.autoSubmitFieldBatch,
    autoPublish: config.autoPublish,
    webflowTokenPresent: Boolean(config.webflowToken),
  })
}

async function main() {
  validateRuntimeConfig()

  process.on("SIGTERM", () => {
    shuttingDown = true
    logJson({ event: "shutdown_signal", signal: "SIGTERM" }, "warn")
  })

  process.on("SIGINT", () => {
    shuttingDown = true
    logJson({ event: "shutdown_signal", signal: "SIGINT" }, "warn")
  })

  logJson({ event: "worker_started", mode: "webflow_batch_blog_creation_worker", workerId: config.workerId })

  while (!shuttingDown) {
    if (!config.workerEnabled) {
      logJson({ event: "worker_disabled_sleeping", workerId: config.workerId }, "warn")
      await sleep(config.idleSleepMs)
      continue
    }

    try {
      const result = await runLoopOnce()

      if (result.done) {
        logJson({ event: "no_active_work", workerId: config.workerId })
        if (!config.keepAliveWhenDone) break
        await sleep(config.idleSleepMs)
        continue
      }

      await sleep(result.worked ? config.loopSleepMs : config.idleSleepMs)
    } catch (error) {
      logJson({ event: "worker_loop_error", workerId: config.workerId, error: safeErrorMessage(error) }, "error")
      if (config.stopOnFatalError) throw error
      await sleep(config.errorSleepMs)
    }
  }

  logJson({ event: "worker_stopped", workerId: config.workerId })
}

main().catch((error) => {
  logJson({ event: "fatal_worker_error", error: safeErrorMessage(error) }, "error")
  process.exit(1)
})
