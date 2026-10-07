import { createReadStream } from "node:fs"
import { pipeline } from "node:stream/promises"
import {
  createAttachmentReader,
  persistedPackage,
  preparePackage,
  randomId,
  requireApplyChange,
  requirePointer,
  requireSubject,
} from "./contracts.js"
import { writeHashed } from "./hash-stream.js"

export function createServer({ storage, currentSubject }) {
  if (currentSubject !== undefined && typeof currentSubject !== "function") {
    throw new TypeError("currentSubject must be a function")
  }
  const callbacks = new Map()
  let tail = Promise.resolve()

  function exclusive(work) {
    const run = tail.then(work, work)
    tail = run.then(() => {}, () => {})
    return run
  }

  return {
    init(subject, applyChange) {
      const key = requireSubject(subject)
      requireApplyChange(applyChange)
      if (callbacks.has(key)) throw new Error(`subject already initialized: ${key}`)
      callbacks.set(key, applyChange)
    },

    handle(req, res) {
      return exclusive(async () => {
        try {
          await dispatch(req, res, storage, callbacks, currentSubject)
        } catch (error) {
          writeError(res, error)
        }
      })
    },

    appendChanges(subject, input) {
      return exclusive(async () => {
        const key = requireSubject(subject)
        if (!callbacks.has(key)) throw new Error(`subject is not initialized: ${key}`)
        const prepared = preparePackage(input)
        const id = randomId()
        const result = storage.appendEntry(key, id, prepared.package)
        return { id, position: result.position }
      })
    },

    close() {
      storage.close()
    },
  }
}

async function dispatch(req, res, storage, callbacks, currentSubject) {
  const query = new URL(req.url || "/", "http://localhost").searchParams
  if (req.method === "PUT") {
    await handlePut(req, res, storage)
    return
  }
  if (req.method === "POST") {
    const result = await handlePost(req, storage, callbacks, currentSubject)
    sendJson(res, result.status, result.body)
    return
  }
  if (req.method === "GET") {
    await handleGet(res, storage, query)
    return
  }
  res.writeHead(405, { Allow: "GET, POST, PUT" })
  res.end()
}

async function handlePut(req, res, storage) {
  const tempPath = storage.beginBlob()
  try {
    const written = await writeHashed(req, tempPath)
    storage.commitBlob(written.hash, tempPath, written.size)
    sendJson(res, 200, { sha256: written.hash, size: written.size })
  } catch (error) {
    storage.discardBlob(tempPath)
    throw error
  }
}

async function handlePost(req, storage, callbacks, currentSubject) {
  const body = await readJson(req)
  if (body === null || typeof body !== "object" || Array.isArray(body)) {
    throw new TypeError("request must be an object")
  }
  const subject = requireSubject(body.subject)
  const live = currentSubject === undefined
    ? subject
    : requireSubject(await currentSubject(req))
  if (live !== subject) {
    return {
      status: 409,
      body: { subject: live, acceptedIds: [], entries: [], attachmentHashes: [] },
    }
  }
  if (!callbacks.has(subject)) throw new Error(`subject is not initialized: ${subject}`)
  const result = await receiveLocked(storage, callbacks, body)
  return { status: 200, body: { ...result, subject } }
}

async function handleGet(res, storage, query) {
  const expected = requireSha256(query)
  const blob = storage.openBlob(expected)
  if (!blob) {
    res.writeHead(404)
    res.end()
    return
  }
  res.writeHead(200, {
    "Content-Type": "application/octet-stream",
    "Content-Length": blob.size,
  })
  await pipeline(createReadStream(blob.file), res)
}

async function receiveLocked(storage, callbacks, request) {
  if (request === null || typeof request !== "object") throw new TypeError("request must be an object")
  const subject = requireSubject(request.subject)
  const applyChange = callbacks.get(subject)
  if (!applyChange) throw new Error(`subject is not initialized: ${subject}`)
  const pointer = requirePointer(request.pointer)
  const changes = request.changes ?? []
  if (!Array.isArray(changes)) throw new TypeError("changes must be an array")

  const acceptedIds = []
  for (const change of changes) {
    if (change === null || typeof change !== "object") throw new TypeError("change must be an object")
    if (typeof change.id !== "string" || change.id.length === 0) {
      throw new TypeError("change.id must be a non-empty string")
    }
    const packageValue = persistedPackage(change.package)
    const existing = storage.hasEntry(subject, change.id)
    if (existing) {
      acceptedIds.push(change.id)
      continue
    }
    for (const manifest of packageValue.attachments) {
      try {
        storage.requireBlob(manifest.sha256, manifest.size)
      } catch {
        throw new Error(`attachment verification failed for ${manifest.id}`)
      }
    }
    const result = storage.appendEntry(subject, change.id, packageValue)
    const entry = { id: change.id, position: result.position, package: packageValue }
    try {
      const applied = await applyChange(entry, readerFor(storage, packageValue.attachments))
      if (applied?.attachments) {
        storage.updateAttachmentPaths(subject, change.id, applied.attachments)
      }
      for (const manifest of packageValue.attachments) {
        storage.dropBlob(manifest.sha256)
      }
    } catch (error) {
      storage.removeTip(subject, change.id, result.position)
      throw error
    }
    acceptedIds.push(change.id)
  }

  return {
    acceptedIds,
    entries: storage.entriesAfter(subject, pointer),
    attachmentHashes: [],
  }
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

function writeError(res, error) {
  if (res.headersSent || res.writableEnded) return
  const status = error instanceof TypeError ? 400 : 500
  sendJson(res, status, { error: error.message || "request failed" })
}

function readerFor(storage, manifests) {
  return createAttachmentReader(manifests, async (manifest) => storage.readBlob(manifest.sha256))
}
