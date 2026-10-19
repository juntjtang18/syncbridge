import { ATTEMPTS, attachmentReader, messageOf, preparePackage, requireFunction, requireSubject, tryThrice } from "./client-contracts.js"
import { SYNC_LIMITS } from "./limits.js"
import { createLog, trace } from "./log.js"

// One install's side of one subject's log. appendChanges queues a change on disk. sync() sends the
// queue in batches (uploading each attachment, checked by hash, before its change), and applies
// every logged change after this install's pointer, in log order, moving the pointer as it goes.
// A change that fails three times (refused by the server, or not applied here) goes on the failure
// list and the sync moves on. A network failure ends the sync; the next one resumes where it stopped.
export function createClient({ storage, transport, sha256, randomId, log, sendBatch = SYNC_LIMITS.sendBatch, receivePage = SYNC_LIMITS.receivePage }) {
  const sink = createLog(log)
  const listeners = new Set()
  let progress = { sent: 0, uploaded: 0, toSend: 0, received: 0, toReceive: 0 }
  let subject = null
  let applyChange = null
  let readAttachment = null
  let tail = Promise.resolve()

  function report(next) {
    progress = { ...progress, ...next }
    for (const listener of listeners) listener(progress)
  }

  function serial(work) {
    const run = tail.then(work, work)
    tail = run.catch(() => {})
    return run
  }

  function ready() {
    if (!subject) throw new Error("client is not initialized")
  }

  // Uploads the change's attachments not yet up, checks the server received the same bytes, and
  // keeps the upload ids on its outbox file, so a stop part way re-uploads nothing already sent.
  async function upload({ name, row }) {
    for (const manifest of row.package.attachments.filter((item) => !item.upload)) {
      const bytes = await readAttachment(manifest)
      const hash = await sha256(bytes)
      const put = await transport.put(bytes)
      if (put.sha256 !== hash || put.size !== bytes.byteLength) throw new Error(`attachment ${manifest.id} arrived changed`)
      Object.assign(manifest, { upload: put.upload, sha256: hash, size: bytes.byteLength })
      trace(sink, { side: "client", subject, step: "put", id: row.id, detail: { attachmentId: manifest.id, upload: put.upload } })
      await storage.save(name, row)
    }
    report({ uploaded: progress.uploaded + 1 })
  }

  // The server refused a change: try it again (its attachments uploaded afresh) up to three times.
  async function refused({ name, row }, error) {
    const attempts = (row.attempts || 0) + 1
    trace(sink, { side: "client", subject, step: "refused", id: row.id, attempt: attempts, error })
    if (attempts < ATTEMPTS) {
      const attachments = row.package.attachments.map(({ id, path, contentType }) => ({ id, path, ...(contentType ? { contentType } : {}) }))
      await storage.save(name, { ...row, attempts, package: { ...row.package, attachments } })
      return
    }
    await storage.addFailure({ id: row.id, position: null, error: `send failed after ${ATTEMPTS} attempts: ${error}`, failedAt: new Date().toISOString() })
    await storage.remove(name)
  }

  // Downloads, checks and applies each entry, then moves the pointer past it.
  async function receive(entries) {
    for (const entry of entries) {
      const failure = await tryThrice(async () => {
        const files = new Map()
        for (const manifest of entry.package.attachments) {
          const bytes = await transport.get(subject, entry.id, manifest.id)
          if (await sha256(bytes) !== manifest.sha256 || bytes.byteLength !== manifest.size) throw new Error(`attachment ${manifest.id} arrived changed`)
          files.set(manifest.id, bytes)
        }
        await applyChange(entry, attachmentReader(entry.package.attachments, (manifest) => files.get(manifest.id)))
      })
      if (failure) {
        trace(sink, { side: "client", subject, step: "apply-failed", id: entry.id, error: messageOf(failure) })
        await storage.addFailure({ id: entry.id, position: entry.position, error: `apply failed after ${ATTEMPTS} attempts: ${messageOf(failure)}`, failedAt: new Date().toISOString() })
      } else {
        trace(sink, { side: "client", subject, step: "applied", id: entry.id, detail: { position: entry.position } })
      }
      await storage.setPointer(entry.position)
      report({ received: progress.received + 1 })
    }
  }

  return {
    async init(nextSubject, nextApplyChange, options = {}) {
      requireSubject(nextSubject)
      requireFunction(nextApplyChange, "applyChange")
      if (options.readAttachment !== undefined) requireFunction(options.readAttachment, "readAttachment")
      await storage.open(nextSubject)
      subject = nextSubject
      applyChange = nextApplyChange
      readAttachment = options.readAttachment
    },

    async appendChanges(input) {
      ready()
      const row = { id: randomId(), package: preparePackage(input) }
      await storage.append(row)
      trace(sink, { side: "client", subject, step: "append", id: row.id })
      return { id: row.id }
    },

    sync() {
      return serial(async () => {
        ready()
        const clientId = await storage.clientId()
        report({ sent: 0, uploaded: 0, toSend: await storage.queued(), received: 0, toReceive: 0 })
        for (;;) {
          const batch = await storage.outbox(sendBatch)
          for (const item of batch) await upload(item)
          const pointer = await storage.pointer()
          trace(sink, { side: "client", subject, step: "post", detail: { pointer, ids: batch.map((item) => item.row.id) } })
          const answer = await transport.post({ subject, pointer, clientId, changes: batch.map((item) => item.row), page: receivePage })
          const accepted = new Set(answer.acceptedIds)
          for (const item of batch) if (accepted.has(item.row.id)) await storage.remove(item.name)
          if (answer.refused) await refused(batch.find((item) => item.row.id === answer.refused.id), answer.refused.error)
          trace(sink, { side: "client", subject, step: "ack", detail: { acceptedIds: answer.acceptedIds, refused: answer.refused, entries: answer.entries.length } })
          report({ sent: progress.sent + accepted.size, toReceive: progress.received + answer.remaining })
          await receive(answer.entries)
          if (batch.length === 0 && !answer.more) return { status: "succeed" }
        }
      })
    },

    failedEntries() {
      ready()
      return storage.failures()
    },

    /** Calls `listener({ sent, uploaded, toSend, received, toReceive })` as a sync() moves. */
    onProgress(listener) {
      listeners.add(listener)
      return () => { listeners.delete(listener) }
    },
  }
}
