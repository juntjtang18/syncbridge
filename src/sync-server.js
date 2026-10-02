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
  sha256,
} from "./contracts.js"
import { writeHashed } from "./hash-stream.js"

export function createServer({ storage }) {
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
          await dispatch(req, res, storage, callbacks)
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
        for (const blob of prepared.blobs) storage.putAttachment(key, blob.sha256, blob.bytes)
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

async function dispatch(req, res, storage, callbacks) {
  const query = new URL(req.url || "/", "http://localhost").searchParams
  if (req.method === "PUT") {
    await handlePut(req, res, storage, query)
    return
  }
  if (req.method === "POST") {
    const result = await handlePost(req, storage, callbacks)
    sendJson(res, 200, result)
    return
  }
  if (req.method === "GET") {
    await handleGet(res, storage, query)
    return
  }
  res.writeHead(405, { Allow: "GET, POST, PUT" })
  res.end()
}

async function handlePut(req, res, storage, query) {
  const expected = requireSha256(query)
  const tempPath = storage.beginBlob()
  try {
    const written = await writeHashed(req, tempPath)
    if (written.hash !== expected) throw new TypeError("attachment hash mismatch")
    storage.commitBlob(expected, tempPath, written.size)
  } catch (error) {
    storage.discardBlob(tempPath)
    throw error
  }
  res.writeHead(204)
  res.end()
}

async function handlePost(req, storage, callbacks) {
  const body = await readJson(req)
  if (body === null || typeof body !== "object" || Array.isArray(body)) {
    throw new TypeError("request must be an object")
  }
  const subject = requireSubject(body.subject)
  if (!callbacks.has(subject)) throw new Error(`subject is not initialized: ${subject}`)
  const changes = body.changes ?? []
  if (!Array.isArray(changes)) throw new TypeError("changes must be an array")
  for (const change of changes) {
    if (change === null || typeof change !== "object") throw new TypeError("change must be an object")
    const packageValue = persistedPackage(change.package)
    for (const manifest of packageValue.attachments) {
      try {
        await storage.installAttachment(subject, manifest.sha256, manifest.size)
      } catch (error) {
        if (error.message === "attachment verification failed") {
          throw new Error(`attachment verification failed for ${manifest.id}`)
        }
        throw error
      }
    }
  }
  return receiveLocked(storage, callbacks, body)
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
    for (const manifest of packageValue.attachments) {
      const stored = storage.readAttachment(subject, manifest.sha256)
      if (sha256(stored) !== manifest.sha256 || stored.length !== manifest.size) {
        throw new Error(`attachment verification failed for ${manifest.id}`)
      }
    }
    const result = storage.appendEntry(subject, change.id, packageValue)
    if (!result.duplicate) {
      const entry = { id: change.id, position: result.position, package: packageValue }
      try {
        await applyChange(entry, readerFor(storage, subject, packageValue.attachments))
      } catch (error) {
        storage.removeTip(subject, change.id, result.position)
        throw error
      }
    }
    acceptedIds.push(change.id)
  }

  const entries = storage.entriesAfter(subject, pointer)
  const hashes = new Set()
  for (const entry of entries) {
    for (const manifest of entry.package.attachments) hashes.add(manifest.sha256)
  }
  return { acceptedIds, entries, attachmentHashes: [...hashes] }
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

function readerFor(storage, subject, manifests) {
  return createAttachmentReader(manifests, async (digest) => storage.readAttachment(subject, digest))
}
