import {
  createAttachmentReader,
  preparePackage,
  randomId,
  requireApplyChange,
  requireSubject,
  sha256,
} from "./contracts.js"

export function createClient({ storage, transport }) {
  const applyBySubject = new Map()
  let subject
  let digest
  let applyChange
  let tail = Promise.resolve()

  function exclusive(work) {
    const run = tail.then(work, work)
    tail = run.then(() => {}, () => {})
    return run
  }

  function ready() {
    if (!subject) throw new Error("client is not initialized")
  }

  async function use(nextSubject, nextApplyChange) {
    const key = requireSubject(nextSubject)
    const callback = requireApplyChange(nextApplyChange)
    const nextDigest = await storage.initSubject(key)
    applyBySubject.set(key, callback)
    subject = key
    digest = nextDigest
    applyChange = callback
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
        for (const blob of prepared.blobs) await storage.putBlob(digest, blob.sha256, blob.bytes)
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
        for (const hash of attachmentHashes(changes.map((change) => change.package))) {
          await transport.put(hash, storage.blobFile(digest, hash))
        }
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
          return { switched: true, subject: live }
        }
        if (!Array.isArray(response.acceptedIds) || !Array.isArray(response.entries) || !Array.isArray(response.attachmentHashes)) {
          throw new TypeError("sync response is invalid")
        }
        await storage.removeOutbox(digest, response.acceptedIds)
        for (const hash of response.attachmentHashes) {
          if (!/^[0-9a-f]{64}$/.test(hash)) throw new TypeError("sha256 is invalid")
          if (await storage.hasBlob(digest, hash)) continue
          const temporary = storage.beginDownload(digest)
          try {
            await transport.get(hash, temporary)
            await storage.commitDownload(digest, hash, temporary)
          } catch (error) {
            await storage.discardDownload(temporary)
            throw error
          }
        }
        const entries = [...response.entries].sort((left, right) => left.position - right.position)
        for (const entry of entries) {
          const current = await storage.readPointer(digest)
          if (entry.position <= current) continue
          for (const manifest of entry.package.attachments) {
            const bytes = await storage.readBlob(digest, manifest.sha256)
            if (sha256(bytes) !== manifest.sha256 || bytes.length !== manifest.size) {
              throw new Error(`attachment verification failed for ${manifest.id}`)
            }
          }
          await applyChange(entry, createAttachmentReader(
            entry.package.attachments,
            (hash) => storage.readBlob(digest, hash),
          ))
          await storage.writePointer(digest, entry.position)
        }
        return { subject }
      })
    },
  }
}

function attachmentHashes(packages) {
  const hashes = new Set()
  for (const packageValue of packages) {
    for (const manifest of packageValue.attachments) hashes.add(manifest.sha256)
  }
  return hashes
}
