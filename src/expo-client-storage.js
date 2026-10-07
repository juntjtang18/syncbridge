import { Directory, File } from "expo-file-system"

export function createExpoClientStorage(dataDir, { sha256, subjectDigest, randomId }) {
  const root = dataDir.replace(/\/+$/, "")

  return {
    async initSubject(subject) {
      const digest = await subjectDigest(subject)
      directory(subjectDirectory(digest)).create({ intermediates: true, idempotent: true })
      directory(attachmentsDirectory(digest)).create({ intermediates: true, idempotent: true })
      await writeIfMissing(pointerPath(digest), "0")
      await writeIfMissing(outboxPath(digest), "")
      await writeIfMissing(`${subjectDirectory(digest)}/subject`, subject)
      return digest
    },

    async readCurrent() {
      const file = new File(currentPath())
      if (!file.exists) return null
      const text = (await file.text()).trim()
      return text || null
    },

    async writeCurrent(subject) {
      await writeAtomic(currentPath(), subject)
    },

    async listSubjects() {
      const dir = directory(`${root}/subjects`)
      if (!dir.exists) return []
      const subjects = []
      for (const entry of dir.list()) {
        const file = new File(`${String(entry.uri).replace(/\/+$/, "")}/subject`)
        if (!file.exists) continue
        const text = (await file.text()).trim()
        if (text) subjects.push(text)
      }
      return subjects
    },

    async readPointer(digest) {
      const pointer = Number(await new File(pointerPath(digest)).text())
      if (!Number.isInteger(pointer) || pointer < 0) throw new Error("pointer file is invalid")
      return pointer
    },

    async writePointer(digest, position) {
      await writeAtomic(pointerPath(digest), String(position))
    },

    async readOutbox(digest) {
      const text = await new File(outboxPath(digest)).text()
      if (text.length === 0) return []
      return text.trim().split("\n").map((line) => JSON.parse(line))
    },

    async appendOutbox(digest, record) {
      const file = outboxPath(digest)
      const current = await new File(file).text()
      const next = current.length === 0 || current.endsWith("\n") ? current : `${current}\n`
      await writeAtomic(file, `${next}${JSON.stringify(record)}\n`)
    },

    async removeOutbox(digest, ids) {
      const drop = new Set(ids)
      const records = await this.readOutbox(digest)
      const kept = records.filter((record) => !drop.has(record.id))
      const text = kept.length === 0 ? "" : `${kept.map((record) => JSON.stringify(record)).join("\n")}\n`
      await writeAtomic(outboxPath(digest), text)
    },

    async putBlob(digest, expectedHash, bytes) {
      if (await sha256(bytes) !== expectedHash) throw new Error("attachment hash mismatch")
      const destination = blobPath(digest, expectedHash)
      const existing = new File(destination)
      if (existing.exists) {
        const saved = await existing.bytes()
        if (await sha256(saved) !== expectedHash || saved.length !== bytes.length) {
          throw new Error("attachment cache mismatch")
        }
        return
      }
      await writeAtomic(destination, bytes)
    },

    blobFile(digest, expectedHash) {
      return blobPath(digest, expectedHash)
    },

    async hasBlob(digest, expectedHash) {
      return new File(blobPath(digest, expectedHash)).exists
    },

    beginDownload(digest) {
      const path = `${attachmentsDirectory(digest)}/.${randomId()}.download`
      const temporary = new File(path)
      temporary.create({ intermediates: true, overwrite: true })
      return path
    },

    async commitDownload(digest, expectedHash, temporaryPath) {
      const temporary = new File(temporaryPath)
      const bytes = await temporary.bytes()
      if (await sha256(bytes) !== expectedHash) {
        temporary.delete()
        throw new Error("attachment hash mismatch")
      }
      const destination = new File(blobPath(digest, expectedHash))
      if (destination.exists) {
        const saved = await destination.bytes()
        if (saved.length !== bytes.length) throw new Error("attachment cache mismatch")
        temporary.delete()
        return
      }
      temporary.move(destination)
    },

    async discardDownload(path) {
      const file = new File(path)
      if (file.exists) file.delete()
    },

    async readBlob(digest, expectedHash) {
      return new File(blobPath(digest, expectedHash)).bytes()
    },
  }

  function currentPath() { return `${root}/current` }
  function subjectDirectory(digest) { return `${root}/subjects/${digest}` }
  function attachmentsDirectory(digest) { return `${subjectDirectory(digest)}/attachments` }
  function pointerPath(digest) { return `${subjectDirectory(digest)}/pointer` }
  function outboxPath(digest) { return `${subjectDirectory(digest)}/outbox.jsonl` }
  function blobPath(digest, hash) { return `${attachmentsDirectory(digest)}/${hash}` }

  function directory(uri) { return new Directory(uri) }

  async function writeIfMissing(path, contents) {
    const file = new File(path)
    if (file.exists) return
    file.create({ intermediates: true, overwrite: true })
    file.write(contents)
  }

  async function writeAtomic(path, contents) {
    const temporary = new File(`${path}.${randomId()}.tmp`)
    temporary.create({ intermediates: true, overwrite: true })
    temporary.write(contents)
    const destination = new File(path)
    if (destination.exists) destination.delete()
    temporary.move(destination)
  }
}
