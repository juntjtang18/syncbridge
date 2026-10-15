import {
  createAttachmentReader,
  preparePackage,
  requireApplyChange,
  requireReadAttachment,
  requireSubject,
} from "./client-contracts.js"
import { createLog, trace } from "./log.js"

const ATTEMPT_LIMIT = 3

export function createClient({ storage, transport, sha256, randomId, log }) {
  if (typeof sha256 !== "function") throw new TypeError("sha256 must be a function")
  if (typeof randomId !== "function") throw new TypeError("randomId must be a function")

  const applyBySubject = new Map()
  const readBySubject = new Map()
  const subjectByDigest = new Map()
  const sink = createLog(log)
  let subject
  let digest
  let applyChange
  let readAttachment
  let tail = Promise.resolve()
  let drain = Promise.resolve()
  let queueLock = Promise.resolve()
  const draining = new Set()
  const seqByDigest = new Map()

  function withQueue(work) {
    const run = queueLock.then(work, work)
    queueLock = run.then(() => {}, () => {})
    return run
  }

  function exclusive(work) {
    const run = tail.then(work, work)
    tail = run.then(() => {}, () => {})
    return run
  }

  function wake(targetDigest, callback) {
    if (!targetDigest || typeof callback !== "function") return
    drain = drain.then(() => pump(targetDigest, callback), () => pump(targetDigest, callback))
  }

  async function pump(targetDigest, callback) {
    if (draining.has(targetDigest)) return
    draining.add(targetDigest)
    try {
      for (;;) {
        const rows = await storage.readUnprocessed(targetDigest)
        const row = rows[0]
        if (!row) return
        await applyUnprocessed(targetDigest, row, callback)
      }
    } finally {
      draining.delete(targetDigest)
    }
  }

  async function applyUnprocessed(targetDigest, row, callback) {
    const loggedSubject = subjectByDigest.get(targetDigest)
    const attempt = (row.attempts || 0) + 1
    trace(sink, { side: "client", subject: loggedSubject, step: "apply", id: row.id, attempt, detail: { position: row.position } })
    try {
      const applied = await callback(
        { id: row.id, position: row.position, package: row.package },
        createAttachmentReader(row.package.attachments || [], async (manifest) => {
          return storage.readBlob(targetDigest, manifest.sha256)
        }, sha256),
      )
      if (applied?.attachments) {
        await withQueue(async () => {
          const rows = await storage.readUnprocessed(targetDigest)
          const current = rows.find((item) => item.id === row.id)
          if (!current) return
          const byId = new Map(applied.attachments.map((item) => [item.id, item]))
          for (const manifest of current.package.attachments || []) {
            const next = byId.get(manifest.id)
            if (next && typeof next.path === "string" && next.path.length > 0) manifest.path = next.path
          }
          await storage.writeUnprocessed(targetDigest, rows)
        })
      }
      for (const manifest of row.package.attachments || []) {
        if (manifest.sha256) await storage.dropBlob(targetDigest, manifest.sha256)
      }
      await removeUnprocessed(targetDigest, row.id)
      trace(sink, { side: "client", subject: loggedSubject, step: "applied", id: row.id, attempt })
    } catch (error) {
      const attempts = (row.attempts || 0) + 1
      trace(sink, {
        side: "client",
        subject: loggedSubject,
        step: "apply-failed",
        id: row.id,
        attempt: attempts,
        error: messageOf(error),
      })
      if (attempts < ATTEMPT_LIMIT) {
        const current = await withQueue(async () => {
          const rows = await storage.readUnprocessed(targetDigest)
          const found = rows.find((item) => item.id === row.id)
          if (!found) return null
          found.attempts = attempts
          await storage.writeUnprocessed(targetDigest, rows)
          return found
        })
        if (!current) return
        await applyUnprocessed(targetDigest, current, callback)
        return
      }
      await storage.writeFailures(targetDigest, [
        ...await storage.readFailures(targetDigest),
        {
          id: row.id,
          position: row.position,
          error: `apply failed after 3 attempts: ${messageOf(error)}`,
          failedAt: new Date().toISOString(),
        },
      ])
      await removeUnprocessed(targetDigest, row.id)
    }
  }

  async function removeUnprocessed(targetDigest, id) {
    await withQueue(async () => {
      const rows = await storage.readUnprocessed(targetDigest)
      await storage.writeUnprocessed(targetDigest, rows.filter((item) => item.id !== id))
    })
  }

  function whenIdle() {
    const targetDigest = digest
    const callback = applyChange
    wake(targetDigest, callback)
    const pending = drain
    return pending.then(async () => {
      if (!targetDigest) return
      const rows = await storage.readUnprocessed(targetDigest)
      if (rows.length > 0) return whenIdle()
    })
  }

  async function use(nextSubject, nextApplyChange, options) {
    const key = requireSubject(nextSubject)
    const callback = requireApplyChange(nextApplyChange)
    const reader = requireReadAttachment(options?.readAttachment)
    const nextDigest = await storage.initSubject(key)
    subjectByDigest.set(nextDigest, key)
    applyBySubject.set(key, callback)
    if (reader) readBySubject.set(key, reader)
    else readBySubject.delete(key)
    subject = key
    digest = nextDigest
    applyChange = callback
    readAttachment = reader
    await storage.writeCurrent(key)
    wake(nextDigest, callback)
  }

  return {
    use,
    init: use,

    async current() {
      if (subject) return subject
      return storage.readCurrent()
    },

    async subjects() {
      return storage.listSubjects()
    },

    async failedEntries() {
      ready()
      return storage.readFailures(digest)
    },

    outbox() {
      ready()
      return storage.readOutbox(digest)
    },

    setOutbox(records) {
      return exclusive(async () => {
        ready()
        await storage.replaceOutbox(digest, normalizeOutbox(records))
        seqByDigest.delete(digest)
      })
    },

    unprocessed() {
      return exclusive(async () => {
        ready()
        return storage.readUnprocessed(digest)
      })
    },

    setUnprocessed(records) {
      return exclusive(async () => {
        ready()
        const rows = normalizeUnprocessed(records)
        await withQueue(async () => {
          await storage.writeUnprocessed(digest, rows)
        })
        wake(digest, applyChange)
      })
    },

    whenIdle,

    appendChanges(input) {
      try {
        ready()
        const targetDigest = digest
        const targetSubject = subject
        const prepared = preparePackage(input)
        const record = { id: randomId(), package: prepared.package, attempts: 0 }
        return takeSeq(targetDigest).then(async (seq) => {
          await storage.appendIncoming(targetDigest, record, seq)
          trace(sink, { side: "client", subject: targetSubject, step: "append", id: record.id })
          return { id: record.id }
        })
      } catch (error) {
        return Promise.reject(error)
      }
    },

    sync() {
      return exclusive(async () => {
        ready()
        const clientId = await storage.ensureClientId()
        let pulled = false
        for (;;) {
          const inflight = await storage.claimInflight(digest)
          const batch = selectBatch(inflight)
          if (batch.length === 0 && pulled) return { status: "succeed", subject }
          if (batch.length > 0) {
            try {
              await fillHashes(batch, readAttachment, transport, sink, subject)
              for (const row of batch) await storage.writeInflight(digest, row)
            } catch (error) {
              trace(sink, { side: "client", subject, step: "put", id: batch[0]?.id, status: "failed", error: messageOf(error) })
              const capped = await failSend(storage, digest, batch[0], messageOf(error), sink, subject)
              if (!capped) return { status: "failed", subject, error: messageOf(error) }
              continue
            }
          }
          const pointer = await storage.readPointer(digest)
          trace(sink, {
            side: "client",
            subject,
            step: "post",
            detail: { pointer, ids: batch.map((row) => row.id) },
          })
          let response
          try {
            response = await transport.post({
              subject,
              pointer,
              changes: batch,
              clientId,
            })
          } catch (error) {
            trace(sink, { side: "client", subject, step: "error", error: messageOf(error) })
            throw error
          }
          pulled = true
          trace(sink, {
            side: "client",
            subject: response?.subject,
            step: "ack",
            status: response?.status,
            error: response?.error,
            detail: { acceptedIds: response?.acceptedIds },
          })
          if ((response?.status === "failed" || response?.status === "error") && !response.subject) {
            if (batch[0]) {
              const capped = await failSend(storage, digest, batch[0], response.error || response.status, sink, subject)
              if (!capped) return { status: response.status, error: response.error }
              continue
            }
            return { status: response.status, error: response.error }
          }
          const switched = await adoptSwitch(response, storage, {
            subject, digest, applyBySubject, readBySubject,
            assign(next) {
              subject = next.subject
              digest = next.digest
              applyChange = next.applyChange
              readAttachment = next.readAttachment
            },
          })
          if (switched) return switched
          if (response?.status === "failed" || response?.status === "error") {
            if (!response.subject) return { status: response.status, error: response.error }
          }
          if (!response || typeof response.subject !== "string" || response.subject.length === 0) {
            throw new TypeError("sync response is invalid")
          }
          if (!Array.isArray(response.acceptedIds) || !Array.isArray(response.entries)) {
            throw new TypeError("sync response is invalid")
          }
          await storage.removeInflight(digest, response.acceptedIds)
          await ingest(response, {
            storage, transport, digest, clientId, subject, withQueue, log: sink,
            wake: () => wake(digest, applyChange),
            post: (body) => transport.post(body),
          })
          if (response.status === "failed" || response.status === "partial" || response.status === "error") {
            const left = await storage.readOutbox(digest)
            const rejected = left.find((row) => batch.some((item) => item.id === row.id))
            if (!rejected) {
              if (left.length === 0) return { status: "succeed", subject }
              return { status: response.status, subject, error: response.error }
            }
            const capped = await failSend(storage, digest, rejected, response.error || response.status, sink, subject)
            if (!capped) return { status: response.status, subject, error: response.error }
            continue
          }
          const left = await storage.readOutbox(digest)
          if (left.length === 0) return { status: "succeed", subject }
        }
      })
    },
  }

  function ready() {
    if (!subject) throw new Error("client is not initialized")
  }

  function takeSeq(targetDigest) {
    let box = seqByDigest.get(targetDigest)
    if (!box) {
      let open
      const gate = new Promise((resolve) => { open = resolve })
      box = { next: 0, failed: null, chain: gate }
      seqByDigest.set(targetDigest, box)
      storage.highestSeq(targetDigest).then((highest) => {
        box.next = highest + 1
        open()
      }, (error) => {
        box.failed = error
        seqByDigest.delete(targetDigest)
        open()
      })
    }
    const run = box.chain.then(() => {
      if (box.failed) throw box.failed
      const seq = box.next
      box.next += 1
      return seq
    })
    box.chain = run.then(() => {}, () => {})
    return run
  }
}

function normalizeOutbox(records) {
  if (!Array.isArray(records)) throw new TypeError("outbox must be an array")
  return records.map((record) => {
    if (record === null || typeof record !== "object" || Array.isArray(record)) {
      throw new TypeError("outbox entry must be an object")
    }
    if (typeof record.id !== "string" || record.id.length === 0) throw new TypeError("outbox id is invalid")
    const attempts = record.attempts ?? 0
    if (!Number.isInteger(attempts) || attempts < 0) throw new TypeError("outbox attempts is invalid")
    return { id: record.id, package: queuePackage(record.package), attempts }
  })
}

function normalizeUnprocessed(records) {
  if (!Array.isArray(records)) throw new TypeError("unprocessed must be an array")
  return records.map((record) => {
    if (record === null || typeof record !== "object" || Array.isArray(record)) {
      throw new TypeError("unprocessed entry must be an object")
    }
    if (typeof record.id !== "string" || record.id.length === 0) throw new TypeError("unprocessed id is invalid")
    if (!Number.isInteger(record.position) || record.position < 1) throw new TypeError("unprocessed position is invalid")
    const attempts = record.attempts ?? 0
    if (!Number.isInteger(attempts) || attempts < 0) throw new TypeError("unprocessed attempts is invalid")
    return { id: record.id, position: record.position, package: queuePackage(record.package), attempts }
  })
}

function queuePackage(value) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("package must be an object")
  }
  if (!Object.hasOwn(value, "data")) throw new TypeError("package.data is required")
  const attachments = value.attachments ?? []
  if (!Array.isArray(attachments)) throw new TypeError("package.attachments must be an array")
  return {
    data: value.data,
    attachments: attachments.map((attachment) => {
      if (attachment === null || typeof attachment !== "object" || Array.isArray(attachment)) {
        throw new TypeError("attachment must be an object")
      }
      if (typeof attachment.id !== "string" || attachment.id.length === 0) {
        throw new TypeError("attachment.id must be a non-empty string")
      }
      if (typeof attachment.path !== "string" || attachment.path.length === 0) {
        throw new TypeError("attachment.path must be a non-empty string")
      }
      const manifest = { id: attachment.id, path: attachment.path }
      if (attachment.contentType !== undefined) manifest.contentType = attachment.contentType
      if (attachment.sha256 !== undefined) manifest.sha256 = attachment.sha256
      if (attachment.size !== undefined) manifest.size = attachment.size
      return manifest
    }),
  }
}

function selectBatch(records) {
  const heldIndex = records.findIndex((row) => (row.attempts || 0) > 0)
  if (heldIndex === -1) return records
  return records.slice(0, heldIndex + 1)
}

async function failSend(storage, digest, record, detail, log, subject) {
  if (!record) return false
  const inflight = await storage.readInflight(digest)
  const found = inflight.find((row) => row.id === record.id)
  if (!found) return false
  const attempts = (found.attempts || 0) + 1
  if (attempts < ATTEMPT_LIMIT) {
    await storage.writeInflight(digest, { ...found, attempts })
    trace(log, { side: "client", subject, step: "send", id: record.id, attempt: attempts, error: detail })
    return false
  }
  const failures = await storage.readFailures(digest)
  failures.push({
    id: record.id,
    position: null,
    error: `send failed after 3 attempts: ${detail}`,
    failedAt: new Date().toISOString(),
  })
  await storage.writeFailures(digest, failures)
  await storage.removeInflight(digest, [record.id])
  trace(log, {
    side: "client",
    subject,
    step: "send-failed",
    id: record.id,
    attempt: attempts,
    error: `send failed after 3 attempts: ${detail}`,
  })
  return true
}

async function ingest(response, context) {
  await advanceSkipped(context, response.skipped)
  let entries = Array.isArray(response.entries) ? response.entries : []
  let guard = 0
  while (entries.length > 0) {
    const entry = entries[0]
    const pointer = await context.storage.readPointer(context.digest)
    if (!entry || !Number.isInteger(entry.position) || entry.position <= pointer) {
      entries = entries.slice(1)
      continue
    }
    try {
      await saveReceived(context, entry)
      await context.storage.writePointer(context.digest, entry.position)
      trace(context.log, {
        side: "client",
        subject: context.subject,
        step: "pointer",
        id: entry.id,
        detail: { position: entry.position },
      })
      context.wake()
      entries = entries.slice(1)
      guard = 0
    } catch (error) {
      guard += 1
      if (guard > 12) throw new Error("receive retry did not advance")
      trace(context.log, {
        side: "client",
        subject: context.subject,
        step: "receive-failed",
        id: entry.id,
        error: messageOf(error),
      })
      const ack = await context.post({
        subject: context.subject,
        pointer,
        changes: [],
        clientId: context.clientId,
        receiveFailed: [{ id: entry.id, error: messageOf(error) }],
      })
      if (!ack || typeof ack.subject !== "string") throw new TypeError("sync response is invalid")
      if (ack.status === "switched" || ack.subject !== context.subject) {
        throw new Error("subject changed during receive")
      }
      await advanceSkipped(context, ack.skipped)
      const skip = Array.isArray(ack.skipped) ? ack.skipped.find((item) => item.id === entry.id) : null
      if (skip && Number.isInteger(skip.position)) {
        const current = await context.storage.readPointer(context.digest)
        if (skip.position > current) {
          await context.storage.writePointer(context.digest, skip.position)
          trace(context.log, {
            side: "client",
            subject: context.subject,
            step: "skip",
            id: entry.id,
            detail: { position: skip.position },
          })
        }
      }
      entries = Array.isArray(ack.entries) ? ack.entries : []
    }
  }
  await advanceEcho(context, response.echoed)
}

async function advanceEcho(context, echoed) {
  if (!Array.isArray(echoed) || echoed.length === 0) return
  const positions = echoed
    .filter((item) => item && Number.isInteger(item.position))
    .map((item) => item.position)
    .sort((left, right) => left - right)
  let pointer = await context.storage.readPointer(context.digest)
  for (const position of positions) {
    if (position <= pointer) continue
    if (position !== pointer + 1) break
    pointer = position
  }
  const current = await context.storage.readPointer(context.digest)
  if (pointer === current) return
  await context.storage.writePointer(context.digest, pointer)
  trace(context.log, {
    side: "client",
    subject: context.subject,
    step: "pointer",
    detail: { position: pointer, echoed: true },
  })
}

async function saveReceived(context, entry) {
  const attachments = entry.package?.attachments || []
  for (const manifest of attachments) {
    if (!manifest?.sha256) throw new Error(`attachment for ${manifest?.id || "unknown"} is missing`)
    const bytes = await context.transport.get(manifest.sha256)
    await context.storage.putBlob(context.digest, manifest.sha256, bytes)
  }
  await context.withQueue(async () => {
    const rows = await context.storage.readUnprocessed(context.digest)
    if (rows.some((row) => row.id === entry.id)) return
    rows.push({ id: entry.id, position: entry.position, package: entry.package, attempts: 0 })
    await context.storage.writeUnprocessed(context.digest, rows)
    trace(context.log, {
      side: "client",
      subject: context.subject,
      step: "enqueue",
      id: entry.id,
      detail: { position: entry.position },
    })
  })
  trace(context.log, { side: "client", subject: context.subject, step: "save", id: entry.id })
}

async function advanceSkipped(context, skipped) {
  if (!Array.isArray(skipped) || skipped.length === 0) return
  let pointer = await context.storage.readPointer(context.digest)
  for (const item of skipped) {
    if (item && Number.isInteger(item.position) && item.position > pointer) pointer = item.position
  }
  const current = await context.storage.readPointer(context.digest)
  if (pointer !== current) {
    await context.storage.writePointer(context.digest, pointer)
    trace(context.log, {
      side: "client",
      subject: context.subject,
      step: "pointer",
      detail: { position: pointer },
    })
  }
}

async function adoptSwitch(response, storage, state) {
  if (!response || typeof response.subject !== "string" || response.subject.length === 0) return null
  if (response.status !== "switched" && response.subject === state.subject) return null
  if (response.subject === state.subject) return null
  const live = requireSubject(response.subject)
  const nextDigest = await storage.initSubject(live)
  await storage.writeCurrent(live)
  state.assign({
    subject: live,
    digest: nextDigest,
    applyChange: state.applyBySubject.get(live),
    readAttachment: state.readBySubject.get(live),
  })
  return { status: "switched", switched: true, subject: live }
}

async function fillHashes(changes, readAttachment, transport, log, subject) {
  for (const change of changes) {
    for (const manifest of change.package.attachments) {
      if (manifest.sha256 && Number.isInteger(manifest.size)) continue
      if (!readAttachment) throw new Error("readAttachment is required")
      trace(log, {
        side: "client",
        subject,
        step: "put",
        id: change.id,
        detail: { attachmentId: manifest.id, path: manifest.path },
      })
      const uploaded = await transport.put(await readAttachment(manifest))
      if (uploaded?.status === "failed" || uploaded?.status === "error") {
        throw new Error(uploaded.error || "attachment upload failed")
      }
      if (!uploaded || !/^[0-9a-f]{64}$/.test(uploaded.sha256) || !Number.isInteger(uploaded.size)) {
        throw new TypeError("put response is invalid")
      }
      manifest.sha256 = uploaded.sha256
      manifest.size = uploaded.size
      trace(log, {
        side: "client",
        subject,
        step: "put",
        id: change.id,
        status: "succeed",
        detail: { attachmentId: manifest.id, sha256: uploaded.sha256, size: uploaded.size },
      })
    }
  }
}

function messageOf(error) {
  return error instanceof Error && error.message ? error.message : "request failed"
}
