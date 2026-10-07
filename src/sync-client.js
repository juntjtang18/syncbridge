import {
  createAttachmentReader,
  preparePackage,
  requireApplyChange,
  requireReadAttachment,
  requireSubject,
} from "./client-contracts.js"

export function createClient({ storage, transport, sha256, randomId }) {
  if (typeof sha256 !== "function") throw new TypeError("sha256 must be a function")
  if (typeof randomId !== "function") throw new TypeError("randomId must be a function")

  const applyBySubject = new Map()
  const readBySubject = new Map()
  let subject
  let digest
  let applyChange
  let readAttachment
  let tail = Promise.resolve()

  function exclusive(work) {
    const run = tail.then(work, work)
    tail = run.then(() => {}, () => {})
    return run
  }

  function ready() {
    if (!subject) throw new Error("client is not initialized")
  }

  async function use(nextSubject, nextApplyChange, options) {
    const key = requireSubject(nextSubject)
    const callback = requireApplyChange(nextApplyChange)
    const reader = requireReadAttachment(options?.readAttachment)
    const nextDigest = await storage.initSubject(key)
    applyBySubject.set(key, callback)
    if (reader) readBySubject.set(key, reader)
    else readBySubject.delete(key)
    subject = key
    digest = nextDigest
    applyChange = callback
    readAttachment = reader
    await storage.writeCurrent(key)
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

    appendChanges(input) {
      return exclusive(async () => {
        ready()
        const prepared = preparePackage(input)
        const record = { id: randomId(), package: prepared.package }
        await storage.appendOutbox(digest, record)
        return { id: record.id }
      })
    },

    sync() {
      return exclusive(async () => {
        ready()
        const pointer = await storage.readPointer(digest)
        const changes = await storage.readOutbox(digest)
        await fillHashes(changes, readAttachment, transport)
        await storage.writeOutbox(digest, changes)
        const response = await transport.post({ subject, pointer, changes })
        if (!response || typeof response.subject !== "string" || response.subject.length === 0) {
          throw new TypeError("sync response is invalid")
        }
        if (response.subject !== subject) {
          const live = requireSubject(response.subject)
          const nextDigest = await storage.initSubject(live)
          await storage.writeCurrent(live)
          subject = live
          digest = nextDigest
          applyChange = applyBySubject.get(live)
          readAttachment = readBySubject.get(live)
          return { switched: true, subject: live }
        }
        if (!Array.isArray(response.acceptedIds) || !Array.isArray(response.entries)) {
          throw new TypeError("sync response is invalid")
        }
        await storage.removeOutbox(digest, response.acceptedIds)
        const entries = [...response.entries].sort((left, right) => left.position - right.position)
        for (const entry of entries) {
          const current = await storage.readPointer(digest)
          if (entry.position <= current) continue
          await applyChange(entry, createAttachmentReader(entry.package.attachments, async (manifest) => {
            if (!readAttachment) throw new Error("readAttachment is required")
            return readAttachment(manifest)
          }, sha256))
          await storage.writePointer(digest, entry.position)
        }
        return { subject }
      })
    },
  }
}

async function fillHashes(changes, readAttachment, transport) {
  for (const change of changes) {
    for (const manifest of change.package.attachments) {
      if (manifest.sha256 && Number.isInteger(manifest.size)) continue
      if (!readAttachment) throw new Error("readAttachment is required")
      const uploaded = await transport.put(await readAttachment(manifest))
      if (!uploaded || !/^[0-9a-f]{64}$/.test(uploaded.sha256) || !Number.isInteger(uploaded.size)) {
        throw new TypeError("put response is invalid")
      }
      manifest.sha256 = uploaded.sha256
      manifest.size = uploaded.size
    }
  }
}
