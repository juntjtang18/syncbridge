import { createReadStream } from "node:fs"
import { pipeline } from "node:stream/promises"
import { asBytes, attachmentReader, loggedPackage, messageOf, requireFunction, requireSubject, tryThrice } from "./client-contracts.js"
import { randomId, sha256 } from "./contracts.js"
import { writeHashed } from "./hash-stream.js"
import { SYNC_LIMITS } from "./limits.js"
import { createLog, trace } from "./log.js"

// How far down the waiting queue the scheduler looks for changes it may start.
const APPLY_WINDOW = 200

// The server: one ordered log per subject, in SQLite. HTTP on /sync:
//   PUT   bytes → { upload, sha256, size }: one upload, kept until its change is applied
//   POST  { subject, pointer, clientId, changes, page } → { acceptedIds, refused?, entries, more, remaining }
//   GET   ?subject=&entry=&attachment= → one attachment of one logged change
// Accepted changes are applied through the host's applyChange. Each subject is scheduled on its own,
// so one user's backlog never holds up another's: up to `applyConcurrency` of a subject's changes at
// once, the same applyKey one at a time in log order, different keys side by side, no key alone
// (within that subject). An apply that fails three times goes on the failure list.
export function createServer({ storage, currentSubject, log, applyConcurrency = SYNC_LIMITS.applyConcurrency }) {
  const callbacks = new Map()
  const waiting = new Map()
  const sink = createLog(log)
  let drain = Promise.resolve()
  let stopped = false

  function wake() {
    if (!stopped) drain = drain.then(pump, pump)
  }

  async function pump() {
    const running = new Map()
    while (!stopped) {
      for (const subject of storage.pendingSubjects()) {
        if (!callbacks.has(subject)) continue
        const mine = running.get(subject) ?? new Map()
        for (const row of startable(storage.unprocessedHead(subject, APPLY_WINDOW), mine)) {
          mine.set(row.id, { key: keyOf(row), done: applyRow(row).finally(() => mine.delete(row.id)) })
        }
        running.set(subject, mine)
      }
      const all = [...running.values()].flatMap((mine) => [...mine.values()])
      if (all.length === 0) return
      await Promise.race(all.map((item) => item.done))
    }
    await Promise.allSettled([...running.values()].flatMap((mine) => [...mine.values()].map((item) => item.done)))
  }

  function keyOf(row) {
    const key = callbacks.get(row.subject)?.applyKey?.(row)
    return typeof key === "string" && key ? `${row.subject}\n${key}` : null
  }

  // One subject's rows to start now, oldest first: a keyed row no earlier row holds the key of, or a
  // keyless row with nothing before it. Nothing passes a keyless row.
  function startable(rows, running) {
    const chosen = []
    const held = new Set([...running.values()].map((item) => item.key))
    for (const row of rows) {
      if (running.size + chosen.length >= applyConcurrency) break
      if (running.has(row.id)) continue
      const key = keyOf(row)
      if (key === null) {
        if (held.size === 0 && chosen.length === 0) chosen.push(row)
        break
      }
      if (!held.has(key) && !held.has(null)) chosen.push(row)
      held.add(key)
    }
    return chosen
  }

  async function applyRow(row) {
    const { applyChange } = callbacks.get(row.subject)
    let moved = null
    const failure = await tryThrice(async () => {
      const applied = await applyChange(
        { id: row.id, position: row.position, package: row.package },
        attachmentReader(row.package.attachments, (manifest) => storage.readUpload(manifest.upload)),
      )
      moved = applied?.attachments
    })
    if (moved) storage.setAttachmentPaths(row.subject, row.id, moved)
    if (failure) storage.recordFailure(row.subject, row.position, row.id, `apply failed after 3 attempts: ${messageOf(failure)}`)
    for (const manifest of row.package.attachments) storage.dropUpload(manifest.upload)
    storage.dequeue(row.subject, row.id)
    trace(sink, { side: "server", subject: row.subject, step: failure ? "apply-failed" : "applied", id: row.id, error: failure ? messageOf(failure) : undefined })
    const waiter = waiting.get(`${row.subject}\n${row.id}`)
    waiting.delete(`${row.subject}\n${row.id}`)
    if (waiter) (failure ? waiter.reject(failure) : waiter.resolve())
  }

  // A change from a client: logged once (a resend is a no-op), each attachment's upload present.
  function accept(subject, change, clientId) {
    if (typeof change?.id !== "string" || !change.id) throw new TypeError("change.id is required")
    if (storage.entryById(subject, change.id)) return
    const packageValue = loggedPackage(change.package)
    for (const manifest of packageValue.attachments) {
      if (storage.uploadSize(manifest.upload) !== manifest.size) throw new Error(`upload of attachment ${manifest.id} is missing or incomplete`)
    }
    const stored = storage.appendEntry(subject, change.id, packageValue, clientId)
    storage.enqueue(subject, stored.position, change.id, packageValue)
  }

  async function handlePut(req, res) {
    const temp = storage.uploadTemp()
    try {
      const written = await writeHashed(req, temp)
      const upload = randomId()
      storage.commitUpload(upload, temp)
      trace(sink, { side: "server", step: "put", detail: { upload, size: written.size } })
      sendJson(res, 200, { upload, sha256: written.hash, size: written.size })
    } finally {
      storage.discard(temp)
    }
  }

  async function handlePost(req, res) {
    const body = await readJson(req)
    const subject = requireSubject(body.subject)
    if (currentSubject && subject !== await currentSubject(req)) {
      sendJson(res, 409, { error: "the sync account changed; sign in again" })
      return
    }
    if (!callbacks.has(subject)) throw new Error(`subject is not initialized: ${subject}`)
    const pointer = Number.isInteger(body.pointer) && body.pointer >= 0 ? body.pointer : 0
    const page = Number.isInteger(body.page) && body.page >= 0 ? body.page : SYNC_LIMITS.receivePage
    const acceptedIds = []
    let refused
    for (const change of Array.isArray(body.changes) ? body.changes : []) {
      try {
        accept(subject, change, body.clientId)
        acceptedIds.push(change.id)
      } catch (error) {
        refused = { id: change?.id, error: messageOf(error) }
        break
      }
    }
    wake()
    // This client's own changes come back in log order without their attachments: it has them.
    const entries = storage.page(subject, pointer, page).map((entry) => entry.clientId && entry.clientId === body.clientId
      ? { id: entry.id, position: entry.position, package: { data: entry.package.data, attachments: [] }, own: true }
      : { id: entry.id, position: entry.position, package: entry.package })
    const remaining = storage.countAfter(subject, pointer)
    trace(sink, { side: "server", subject, step: "ack", detail: { pointer, accepted: acceptedIds.length, refused, entries: entries.length } })
    sendJson(res, 200, { acceptedIds, ...(refused ? { refused } : {}), entries, more: remaining > entries.length, remaining })
  }

  // The change's own upload while it waits to be applied; the host's stored copy after.
  async function handleGet(req, res, query) {
    const subject = currentSubject ? await currentSubject(req) : query.get("subject")
    const entry = subject ? storage.entryById(subject, query.get("entry")) : null
    const manifest = entry?.package.attachments.find((item) => item.id === query.get("attachment"))
    const size = manifest?.upload ? storage.uploadSize(manifest.upload) : null
    if (size !== null) {
      res.writeHead(200, { "Content-Type": "application/octet-stream", "Content-Length": size })
      await pipeline(createReadStream(storage.uploadPath(manifest.upload)), res)
      return
    }
    const readAttachment = callbacks.get(subject)?.readAttachment
    // readAttachment answers null for a file that is not there (404: a try for the receiver) and
    // throws on a failure to read (500: the receiver stops and comes back).
    const found = manifest && readAttachment ? await readAttachment(manifest) : null
    const bytes = found ? asBytes(found, "attachment") : null
    if (!bytes || sha256(bytes) !== manifest.sha256) {
      sendJson(res, 404, { error: "attachment not found" })
      return
    }
    res.writeHead(200, { "Content-Type": "application/octet-stream", "Content-Length": bytes.byteLength })
    res.end(Buffer.from(bytes))
  }

  return {
    init(subject, applyChange, options = {}) {
      requireSubject(subject)
      requireFunction(applyChange, "applyChange")
      if (callbacks.has(subject)) throw new Error(`subject already initialized: ${subject}`)
      callbacks.set(subject, { applyChange, readAttachment: options.readAttachment, applyKey: options.applyKey })
      wake()
    },

    async handle(req, res) {
      try {
        const query = new URL(req.url || "/", "http://localhost").searchParams
        if (req.method === "PUT") await handlePut(req, res)
        else if (req.method === "POST") await handlePost(req, res)
        else if (req.method === "GET") await handleGet(req, res, query)
        else sendJson(res, 405, { error: "method not allowed" })
      } catch (error) {
        trace(sink, { side: "server", step: "error", error: messageOf(error) })
        if (res.headersSent) res.destroy()
        else sendJson(res, error instanceof TypeError || error instanceof SyntaxError ? 400 : 500, { error: messageOf(error) })
      }
    },

    // A change made by the host itself, logged for every client. With `apply` it also goes through
    // the host's applyChange, in log order with the clients' changes, and resolves once applied.
    async appendChanges(subject, input, { apply = false } = {}) {
      requireSubject(subject)
      const id = randomId()
      const packageValue = { data: input.data, attachments: [] }
      const stored = storage.appendEntry(subject, id, packageValue, null)
      if (!apply) return { id, position: stored.position }
      const applied = new Promise((resolve, reject) => waiting.set(`${subject}\n${id}`, { resolve, reject }))
      storage.enqueue(subject, stored.position, id, packageValue)
      wake()
      await applied
      return { id, position: stored.position }
    },

    entriesBetween: (subject, after, before) => storage.entriesBetween(subject, after, before),
    failedEntries: (subject) => storage.failedEntries(subject),
    // Subjects with changes still waiting, so a restarted host can init them and let them apply.
    pendingSubjects: () => storage.pendingSubjects(),
    whenIdle: () => drain,

    async close() {
      stopped = true
      await drain
      storage.close()
    },
  }
}

async function readJson(req) {
  const chunks = []
  for await (const chunk of req) chunks.push(chunk)
  const body = JSON.parse(Buffer.concat(chunks).toString("utf8"))
  if (body === null || typeof body !== "object" || Array.isArray(body)) throw new TypeError("request must be an object")
  return body
}

function sendJson(res, status, body) {
  const text = JSON.stringify(body)
  res.writeHead(status, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(text) })
  res.end(text)
}
