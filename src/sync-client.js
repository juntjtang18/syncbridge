import {
  createAttachmentReader,
  preparePackage,
  randomId,
  requireApplyChange,
  requireSubject,
  sha256,
} from "./contracts.js"

export function createClient({ storage, transport }) {
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

  return {
    async init(nextSubject, nextApplyChange) {
      if (subject) throw new Error("client already initialized")
      const key = requireSubject(nextSubject)
      const callback = requireApplyChange(nextApplyChange)
      const nextDigest = await storage.initSubject(key)
      subject = key
      digest = nextDigest
      applyChange = callback
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
        if (!response || !Array.isArray(response.acceptedIds) || !Array.isArray(response.entries) || !Array.isArray(response.attachmentHashes)) {
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
