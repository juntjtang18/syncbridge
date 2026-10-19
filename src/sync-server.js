import { createReadStream } from "node:fs"
import { pipeline } from "node:stream/promises"
import {
  asBytes,
  createAttachmentReader,
  persistedPackage,
  preparePackage,
  randomId,
  requireApplyChange,
  requirePointer,
  requireReadAttachment,
  requireSubject,
  sha256,
} from "./contracts.js"
import { writeHashed } from "./hash-stream.js"
import { createLog, trace } from "./log.js"

const ATTEMPT_LIMIT = 3

export function createServer({ storage, currentSubject, log }) {
  if (currentSubject !== undefined && typeof currentSubject !== "function") {
    throw new TypeError("currentSubject must be a function")
  }
  const callbacks = new Map()
  const waiting = new Map()
  const sink = createLog(log)
  let tail = Promise.resolve()
  let drain = Promise.resolve()
  let stopped = false
  let idle = true

  function exclusive(work) {
    const run = tail.then(work, work)
    tail = run.then(() => {}, () => {})
    return run
  }

  function wake() {
    if (stopped) return
    idle = false
    drain = drain.then(() => pump(), () => pump()).then(() => {
      idle = !storage.hasUnprocessed()
    }, () => {
      idle = !storage.hasUnprocessed()
    })
  }

  async function pump() {
    while (!stopped) {
      const row = storage.headUnprocessed()
      if (!row) return
      await applyRow(row)
    }
  }

  async function applyRow(row) {
    const registered = callbacks.get(row.subject)
    const attempt = row.attempts + 1
    trace(sink, { side: "server", subject: row.subject, step: "apply", id: row.id, attempt, detail: { position: row.position } })
    try {
      if (!registered?.applyChange) throw new Error(`subject is not initialized: ${row.subject}`)
      const origin = storage.entryOrigin(row.subject, row.id)
      const applied = await registered.applyChange(
        { id: row.id, position: row.position, package: row.package, clientId: origin.clientId, base: origin.base },
        readerFor(storage, row.package.attachments, row.subject, registered.readAttachment),
      )
      if (applied?.attachments) storage.updateAttachmentPaths(row.subject, row.id, applied.attachments)
      storage.removeUnprocessed(row.subject, row.id)
      // PUT staging is stored by content: keep it while another waiting change carries the same bytes.
      for (const manifest of row.package.attachments || []) {
        if (manifest.sha256 && !storage.unprocessedUsesBlob(manifest.sha256)) storage.dropBlob(manifest.sha256)
      }
      trace(sink, { side: "server", subject: row.subject, step: "applied", id: row.id, attempt })
      settle(row.subject, row.id, null)
    } catch (error) {
      const attempts = row.attempts + 1
      trace(sink, {
        side: "server",
        subject: row.subject,
        step: "apply-failed",
        id: row.id,
        attempt: attempts,
        error: messageOf(error),
      })
      if (attempts < ATTEMPT_LIMIT) {
        storage.setUnprocessedAttempts(row.subject, row.id, attempts)
        row.attempts = attempts
        await applyRow(row)
        return
      }
      storage.recordFailure({
        subject: row.subject,
        position: row.position,
        id: row.id,
        clientId: "",
        kind: "apply",
        error: `apply failed after 3 attempts: ${messageOf(error)}`,
        failedAt: new Date().toISOString(),
      })
      storage.removeUnprocessed(row.subject, row.id)
      settle(row.subject, row.id, new Error(`apply failed after 3 attempts: ${messageOf(error)}`))
    }
  }

  function settle(subject, id, error) {
    const key = `${subject}\n${id}`
    const waiter = waiting.get(key)
    if (!waiter) return
    waiting.delete(key)
    if (error) waiter.reject(error)
    else waiter.resolve()
  }

  function whenApplied(subject, id) {
    return new Promise((resolve, reject) => waiting.set(`${subject}\n${id}`, { resolve, reject }))
  }

  function whenIdle() {
    if (stopped) return Promise.resolve()
    wake()
    const pending = drain
    return pending.then(() => {
      if (!stopped && storage.hasUnprocessed()) return whenIdle()
    })
  }

  return {
    init(subject, applyChange, options) {
      const key = requireSubject(subject)
      requireApplyChange(applyChange)
      const readAttachment = requireReadAttachment(options?.readAttachment)
      if (callbacks.has(key)) throw new Error(`subject already initialized: ${key}`)
      callbacks.set(key, { applyChange, readAttachment })
      wake()
    },

    handle(req, res) {
      return exclusive(async () => {
        try {
          await dispatch(req, res, storage, callbacks, currentSubject, wake, sink)
        } catch (error) {
          trace(sink, { side: "server", step: "error", status: isPredicted(error) ? "failed" : "error", error: messageOf(error) })
          writeAckError(res, error)
        }
      })
    },

    // A host append is logged for clients and not applied: the host already made the change.
    // With { apply: true } the server's own apply makes it, in log order with client changes, and
    // the call resolves once applied (or rejects with the apply error). It carries no attachments.
    appendChanges(subject, input, { apply = false } = {}) {
      return exclusive(async () => {
        const key = requireSubject(subject)
        if (!callbacks.has(key)) throw new Error(`subject is not initialized: ${key}`)
        const prepared = preparePackage(input)
        if (apply && prepared.package.attachments.length > 0) {
          throw new TypeError("an applied host append carries no attachments")
        }
        const id = randomId()
        const result = storage.appendEntry(key, id, prepared.package)
        if (apply) storage.enqueueUnprocessed(key, result.position, id, prepared.package)
        trace(sink, { side: "server", subject: key, step: "append", id, detail: { position: result.position, apply } })
        return { id, position: result.position, applied: apply ? whenApplied(key, id) : null }
      }).then(async ({ id, position, applied }) => {
        if (applied) {
          wake()
          await applied
        }
        return { id, position }
      })
    },

    // The changes logged after `after` and before `before`, each with its sender's clientId:
    // what a sender whose pointer was `after` had not seen when its change at `before` was logged.
    entriesBetween(subject, after, before) {
      return storage.entriesBetween(requireSubject(subject), requirePointer(after), requirePointer(before))
        .map((row) => ({ id: row.id, position: row.position, package: row.package, clientId: row.clientId }))
    },

    failedEntries(subject) {
      return storage.failedEntries(requireSubject(subject))
    },

    unprocessed(subject) {
      return storage.listUnprocessed(requireSubject(subject))
    },

    setUnprocessed(subject, rows) {
      const key = requireSubject(subject)
      const queued = normalizeQueue(rows, "unprocessed")
      storage.replaceUnprocessed(key, queued)
      for (const row of queued) {
        trace(sink, { side: "server", subject: key, step: "enqueue", id: row.id, detail: { position: row.position } })
      }
      wake()
    },

    whenIdle,

    close() {
      for (const [key, waiter] of waiting) {
        waiting.delete(key)
        waiter.reject(new Error("sync server closed before the change was applied"))
      }
      stopped = true
      if (idle) {
        storage.close()
        return undefined
      }
      const pending = drain
      return pending.then(() => storage.close(), () => storage.close())
    },
  }
}

async function dispatch(req, res, storage, callbacks, currentSubject, wake, log) {
  const query = new URL(req.url || "/", "http://localhost").searchParams
  if (req.method === "PUT") {
    await handlePut(req, res, storage, log)
    return
  }
  if (req.method === "POST") {
    const result = await handlePost(req, storage, callbacks, currentSubject, wake, log)
    sendJson(res, result.status, result.body)
    return
  }
  if (req.method === "GET") {
    await handleGet(req, res, storage, callbacks, currentSubject, query, log)
    return
  }
  if (req.method === "HEAD") {
    await handleHead(res, storage, currentSubject, req, query)
    return
  }
  res.writeHead(405, { Allow: "GET, HEAD, POST, PUT" })
  res.end()
}

async function handlePut(req, res, storage, log) {
  const tempPath = storage.beginBlob()
  trace(log, { side: "server", step: "put" })
  try {
    const written = await writeHashed(req, tempPath)
    storage.commitBlob(written.hash, tempPath, written.size)
    trace(log, { side: "server", step: "put", status: "succeed", detail: { sha256: written.hash, size: written.size } })
    sendJson(res, 200, { sha256: written.hash, size: written.size })
  } catch (error) {
    storage.discardBlob(tempPath)
    if (res.headersSent || res.writableEnded) return
    const status = isPredicted(error) ? "failed" : "error"
    trace(log, { side: "server", step: "put", status, error: messageOf(error) })
    sendJson(res, 200, {
      status,
      error: messageOf(error),
    })
  }
}

async function handlePost(req, storage, callbacks, currentSubject, wake, log) {
  const body = await readJson(req)
  if (body === null || typeof body !== "object" || Array.isArray(body)) {
    throw new TypeError("request must be an object")
  }
  const subject = requireSubject(body.subject)
  const live = currentSubject === undefined
    ? subject
    : requireSubject(await currentSubject(req))
  if (live !== subject) {
    trace(log, { side: "server", subject: live, step: "ack", status: "switched" })
    return { status: 409, body: emptyAck("switched", live) }
  }
  if (!callbacks.has(subject)) throw new Error(`subject is not initialized: ${subject}`)
  const clientId = optionalClientId(body.clientId)
  const failures = receiveFailureList(body.receiveFailed)
  if (failures.length > 0 && !clientId) throw new TypeError("clientId is required")
  const pointer = requirePointer(body.pointer)
  trace(log, {
    side: "server",
    subject,
    step: "post",
    detail: { pointer, ids: changesOf(body).map((change) => change?.id), clientId },
  })

  const skipped = []
  for (const failure of failures) {
    const noted = noteReceiveFailure(storage, subject, clientId, failure, log)
    if (noted) skipped.push(noted)
  }

  const changes = body.changes ?? []
  if (!Array.isArray(changes)) throw new TypeError("changes must be an array")
  const acceptedIds = []
  let failure = null
  let enqueued = false
  for (const change of changes) {
    try {
      const stored = acceptChange(storage, subject, change, clientId, pointer)
      if (stored.enqueued) {
        enqueued = true
        trace(log, { side: "server", subject, step: "enqueue", id: stored.id, detail: { position: stored.position } })
      }
      acceptedIds.push(stored.id)
    } catch (error) {
      if (!isPredicted(error)) throw error
      failure = error
      break
    }
  }

  const delivery = clientId
    ? deliveryFor(storage, subject, pointer, clientId)
    : { skipped: [], entries: storage.entriesAfter(subject, pointer).map(publicEntry) }
  // A client that asks for pages (`receivePage`) gets at most that many entries, with `more` when
  // others follow; a skip past the page's end waits for the page that reaches it.
  const page = Number.isInteger(body.receivePage) && body.receivePage > 0 ? body.receivePage : null
  const remaining = delivery.entries.length
  const entries = page && remaining > page ? delivery.entries.slice(0, page) : delivery.entries
  const more = entries.length < remaining
  const reached = more ? entries[entries.length - 1].position : Infinity
  const seen = new Set(skipped.map((item) => item.id))
  for (const item of delivery.skipped) {
    if (!seen.has(item.id) && item.position <= reached) skipped.push(item)
  }
  if (enqueued) wake()

  const statusName = failure ? (acceptedIds.length > 0 ? "partial" : "failed") : "succeed"
  trace(log, {
    side: "server",
    subject,
    step: "ack",
    status: statusName,
    error: failure ? failure.message : undefined,
    detail: { acceptedIds },
  })

  return {
    status: 200,
    body: {
      ...emptyAck(failure ? (acceptedIds.length > 0 ? "partial" : "failed") : "succeed", subject),
      acceptedIds,
      entries,
      skipped,
      ...(page ? { more, remaining, applying: storage.unprocessedCount(subject) } : {}),
      ...(clientId ? { serverFailed: storage.applyFailuresFor(subject, clientId) } : {}),
      ...(failure ? { error: failure.message } : {}),
    },
  }
}

function acceptChange(storage, subject, change, clientId, base) {
  if (change === null || typeof change !== "object") throw new TypeError("change must be an object")
  if (typeof change.id !== "string" || change.id.length === 0) {
    throw new TypeError("change.id must be a non-empty string")
  }
  const packageValue = persistedPackage(change.package)
  if (storage.hasEntry(subject, change.id)) return { id: change.id, enqueued: false }
  for (const manifest of packageValue.attachments) {
    // Staging gone, but the host already stores these bytes for an earlier change: apply reads that copy.
    const stored = !storage.openBlob(manifest.sha256) && storage.findAttachment(subject, manifest.sha256)
    if (stored && stored.size === manifest.size) continue
    try {
      storage.requireBlob(manifest.sha256, manifest.size)
    } catch (error) {
      if (!isAttachmentCheck(error)) throw error
      throw attachmentFailure(error, manifest.id)
    }
  }
  const result = storage.appendEntry(subject, change.id, packageValue, clientId || null, base)
  if (result.duplicate) return { id: change.id, enqueued: false }
  storage.enqueueUnprocessed(subject, result.position, change.id, packageValue)
  return { id: change.id, position: result.position, enqueued: true }
}

function noteReceiveFailure(storage, subject, clientId, failure, log) {
  const existing = storage.sendFailure(subject, clientId, failure.id)
  if (existing) return { id: existing.id, position: existing.position }
  const entry = storage.entryById(subject, failure.id)
  if (!entry) return null
  const attempts = storage.incrementSendAttempts(subject, clientId, failure.id)
  trace(log, {
    side: "server",
    subject,
    step: attempts < ATTEMPT_LIMIT ? "send" : "send-failed",
    id: failure.id,
    attempt: attempts,
    error: failure.error,
    detail: { clientId },
  })
  if (attempts < ATTEMPT_LIMIT) return null
  storage.recordFailure({
    subject,
    position: entry.position,
    id: entry.id,
    clientId,
    kind: "send",
    error: `send failed after 3 attempts: ${failure.error}`,
    failedAt: new Date().toISOString(),
  })
  return { id: entry.id, position: entry.position }
}

function deliveryFor(storage, subject, pointer, clientId) {
  const rows = storage.entriesAfter(subject, pointer)
  const skipped = []
  const entries = []
  let retry = null
  for (const row of rows) {
    // This client's own change comes back in log order so every copy replays the same log, but
    // without its attachments: the bytes are already on that client.
    if (row.clientId && row.clientId === clientId) {
      entries.push({ ...publicEntry(row), package: { ...row.package, attachments: [] }, own: true })
      continue
    }
    if (storage.sendFailure(subject, clientId, row.id)) {
      skipped.push({ id: row.id, position: row.position })
      continue
    }
    const attempts = storage.sendAttempts(subject, clientId, row.id)
    if (attempts > 0 && attempts < ATTEMPT_LIMIT) {
      retry = row
      break
    }
    entries.push(publicEntry(row))
  }
  if (retry) return { skipped, entries: [...entries.filter((entry) => entry.own), publicEntry(retry)] }
  return { skipped, entries }
}

function publicEntry(row) {
  return { id: row.id, position: row.position, package: row.package }
}

function changesOf(body) {
  return Array.isArray(body.changes) ? body.changes : []
}

// HEAD /sync?sha256=: 200 when the host already holds these bytes, as a PUT staging or as the stored
// copy of an applied change in the live subject's log, so a client need not upload them again.
async function handleHead(res, storage, currentSubject, req, query) {
  const hash = requireSha256(query)
  let held = Boolean(storage.openBlob(hash))
  if (!held && currentSubject !== undefined) held = Boolean(storage.findAttachment(requireSubject(await currentSubject(req)), hash))
  res.writeHead(held ? 200 : 404)
  res.end()
}

async function handleGet(req, res, storage, callbacks, currentSubject, query, log) {
  const expected = requireSha256(query)
  const blob = storage.openBlob(expected)
  if (blob) {
    res.writeHead(200, {
      "Content-Type": "application/octet-stream",
      "Content-Length": blob.size,
    })
    trace(log, { side: "server", step: "get", status: "succeed", detail: { sha256: expected, size: blob.size } })
    await pipeline(createReadStream(blob.file), res)
    return
  }
  if (currentSubject === undefined) {
    trace(log, { side: "server", step: "get", status: "failed", detail: { sha256: expected } })
    res.writeHead(404)
    res.end()
    return
  }
  const live = requireSubject(await currentSubject(req))
  const registered = callbacks.get(live)
  const manifest = storage.findAttachment(live, expected)
  if (!registered?.readAttachment || !manifest) {
    trace(log, { side: "server", subject: live, step: "get", status: "failed", detail: { sha256: expected } })
    res.writeHead(404)
    res.end()
    return
  }
  let bytes
  try {
    bytes = asBytes(await registered.readAttachment(manifest), "attachment")
  } catch (error) {
    if (isMissing(error)) {
      trace(log, { side: "server", subject: live, step: "get", status: "failed", error: messageOf(error), detail: { sha256: expected } })
      res.writeHead(404)
      res.end()
      return
    }
    throw error
  }
  const actualHash = sha256(bytes)
  if (actualHash !== manifest.sha256) {
    throw attachmentError(
      "ATTACHMENT_HASH_MISMATCH",
      `ATTACHMENT_HASH_MISMATCH sha256 ${manifest.sha256} actual ${actualHash} attachment ${manifest.id}`,
    )
  }
  if (bytes.byteLength !== manifest.size) {
    throw attachmentError(
      "ATTACHMENT_SIZE_MISMATCH",
      `ATTACHMENT_SIZE_MISMATCH sha256 ${manifest.sha256} expected ${manifest.size} actual ${bytes.byteLength} attachment ${manifest.id}`,
    )
  }
  trace(log, { side: "server", subject: live, step: "get", status: "succeed", detail: { sha256: expected, size: bytes.byteLength } })
  res.writeHead(200, {
    "Content-Type": "application/octet-stream",
    "Content-Length": bytes.byteLength,
  })
  res.end(Buffer.from(bytes))
}

function normalizeQueue(rows, label) {
  if (!Array.isArray(rows)) throw new TypeError(`${label} must be an array`)
  return rows.map((row) => {
    if (row === null || typeof row !== "object" || Array.isArray(row)) {
      throw new TypeError(`${label} entry must be an object`)
    }
    if (typeof row.id !== "string" || row.id.length === 0) throw new TypeError(`${label} id is invalid`)
    if (!Number.isInteger(row.position) || row.position < 1) throw new TypeError(`${label} position is invalid`)
    const attempts = row.attempts ?? 0
    if (!Number.isInteger(attempts) || attempts < 0) throw new TypeError(`${label} attempts is invalid`)
    return {
      id: row.id,
      position: row.position,
      package: queuePackage(row.package),
      attempts,
    }
  })
}

function queuePackage(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("package must be an object")
  }
  if (!Object.hasOwn(value, "data")) throw new TypeError("package.data is required")
  const attachments = value.attachments ?? []
  if (!Array.isArray(attachments)) throw new TypeError("package.attachments must be an array")
  return { data: value.data, attachments }
}

function emptyAck(status, subject) {
  return { status, subject, acceptedIds: [], entries: [], attachmentHashes: [], skipped: [] }
}

function optionalClientId(value) {
  if (value == null || value === "") return ""
  if (typeof value !== "string") throw new TypeError("clientId must be a string")
  return value
}

function receiveFailureList(value) {
  if (value == null) return []
  if (!Array.isArray(value)) throw new TypeError("receiveFailed must be an array")
  return value.map((item) => {
    if (typeof item === "string" && item.length > 0) {
      return { id: item, error: "receiver could not store the log entry" }
    }
    if (item && typeof item.id === "string" && item.id.length > 0) {
      const error = typeof item.error === "string" && item.error.length > 0
        ? item.error
        : "receiver could not store the log entry"
      return { id: item.id, error }
    }
    throw new TypeError("receiveFailed id is invalid")
  })
}

function isMissing(error) {
  return error instanceof Error && /missing/i.test(error.message)
}

function isAttachmentCheck(error) {
  return error instanceof Error && (
    error.code === "ATTACHMENT_NOT_FOUND"
    || error.code === "ATTACHMENT_SIZE_MISMATCH"
    || error.code === "ATTACHMENT_HASH_MISMATCH"
  )
}

function attachmentFailure(error, id) {
  return attachmentError(error.code, `${error.message} attachment ${id}`)
}

function attachmentError(code, message) {
  const error = new Error(message)
  error.code = code
  return error
}

function isPredicted(error) {
  if (error instanceof TypeError) return true
  if (!(error instanceof Error)) return false
  return /attachment verification failed|attachment hash|attachment cache|ATTACHMENT_NOT_FOUND|ATTACHMENT_SIZE_MISMATCH|ATTACHMENT_HASH_MISMATCH|must be|invalid|required/.test(error.message)
}

function messageOf(error) {
  return error instanceof Error && error.message ? error.message : "request failed"
}

function requireSha256(query) {
  const value = query.get("sha256")
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    throw new TypeError("sha256 is invalid")
  }
  return value
}

async function readJson(req) {
  const chunks = []
  let size = 0
  for await (const chunk of req) {
    size += chunk.length
    if (size > 1_000_000) throw new TypeError("request body is too large")
    chunks.push(chunk)
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"))
  } catch {
    throw new TypeError("request body must be JSON")
  }
}

function sendJson(res, status, value) {
  const payload = Buffer.from(JSON.stringify(value))
  res.writeHead(status, {
    "Content-Type": "application/json",
    "Content-Length": payload.length,
  })
  res.end(payload)
}

function writeAckError(res, error) {
  if (res.headersSent || res.writableEnded) return
  sendJson(res, 200, {
    status: isPredicted(error) ? "failed" : "error",
    error: messageOf(error),
    acceptedIds: [],
    entries: [],
    attachmentHashes: [],
    skipped: [],
  })
}

// Reads an upload's PUT staging. When the staging is gone (another change with the same bytes was
// applied first and it was deleted, as when two clients upload one photo), the same bytes are read
// from the host's stored copy, found through the log by hash, as GET /sync does.
function readerFor(storage, manifests, subject, readAttachment) {
  return createAttachmentReader(manifests, async (manifest) => {
    if (storage.openBlob(manifest.sha256)) return storage.readBlob(manifest.sha256)
    const stored = storage.findAttachment(subject, manifest.sha256)
    if (!stored || !readAttachment) throw attachmentError("ATTACHMENT_NOT_FOUND", `ATTACHMENT_NOT_FOUND sha256 ${manifest.sha256}`)
    return asBytes(await readAttachment(stored), "attachment")
  }, sha256)
}
