import { Directory, File } from "expo-file-system"

export function createExpoClientStorage(dataDir, { sha256, subjectDigest, randomId }) {
  const root = dataDir.replace(/\/+$/, "")

  return {
    async initSubject(subject) {
      const digest = await subjectDigest(subject)
      directory(subjectDirectory(digest)).create({ intermediates: true, idempotent: true })
      directory(attachmentsDirectory(digest)).create({ intermediates: true, idempotent: true })
      directory(queueDirectory(digest, "incoming")).create({ intermediates: true, idempotent: true })
      directory(queueDirectory(digest, "inflight")).create({ intermediates: true, idempotent: true })
      await writeIfMissing(pointerPath(digest), "0")
      await writeIfMissing(unprocessedPath(digest), "[]")
      await writeIfMissing(failuresPath(digest), "[]")
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

    async highestSeq(digest) {
      await migrateLegacyOutbox(digest)
      const names = [
        ...listQueueNames(queueDirectory(digest, "inflight")),
        ...listQueueNames(queueDirectory(digest, "incoming")),
      ]
      let highest = 0
      for (const name of names) highest = Math.max(highest, seqOf(name))
      return highest
    },

    async appendIncoming(digest, record, seq) {
      await migrateLegacyOutbox(digest)
      await writeQueueFile(queueDirectory(digest, "incoming"), seq, record)
    },

    async claimInflight(digest) {
      await migrateLegacyOutbox(digest)
      const inflightDir = queueDirectory(digest, "inflight")
      const pending = await readQueue(inflightDir)
      if (pending.length > 0) return pending
      const incomingDir = queueDirectory(digest, "incoming")
      directory(inflightDir).create({ intermediates: true, idempotent: true })
      for (const name of listQueueNames(incomingDir)) {
        new File(`${incomingDir}/${name}`).move(new File(`${inflightDir}/${name}`))
      }
      return readQueue(inflightDir)
    },

    async readInflight(digest) {
      await migrateLegacyOutbox(digest)
      return readQueue(queueDirectory(digest, "inflight"))
    },

    async readOutbox(digest) {
      await migrateLegacyOutbox(digest)
      return [
        ...await readQueue(queueDirectory(digest, "inflight")),
        ...await readQueue(queueDirectory(digest, "incoming")),
      ]
    },

    async writeInflight(digest, record) {
      const dir = queueDirectory(digest, "inflight")
      const name = await findQueueName(dir, record.id)
      if (!name) throw new Error(`inflight entry is missing: ${record.id}`)
      await writeAtomic(`${dir}/${name}`, JSON.stringify(record))
    },

    async removeInflight(digest, ids) {
      const drop = new Set(ids)
      const dir = queueDirectory(digest, "inflight")
      for (const name of listQueueNames(dir)) {
        const record = JSON.parse(await new File(`${dir}/${name}`).text())
        if (drop.has(record.id)) new File(`${dir}/${name}`).delete()
      }
    },

    async replaceOutbox(digest, records) {
      const legacy = new File(legacyOutboxPath(digest))
      if (legacy.exists) legacy.delete()
      clearQueue(queueDirectory(digest, "inflight"))
      clearQueue(queueDirectory(digest, "incoming"))
      for (let index = 0; index < records.length; index += 1) {
        await writeQueueFile(queueDirectory(digest, "incoming"), index + 1, records[index])
      }
    },

    // A received attachment, checked against its hash and saved under `name` (its change's own).
    async putBlob(digest, name, expectedHash, bytes) {
      if (await sha256(bytes) !== expectedHash) throw new Error("attachment hash mismatch")
      await writeAtomic(blobPath(digest, name), bytes)
    },

    async readBlob(digest, name) {
      return new File(blobPath(digest, name)).bytes()
    },

    async dropBlob(digest, name) {
      const file = new File(blobPath(digest, name))
      if (file.exists) file.delete()
    },

    async ensureClientId() {
      const file = new File(clientIdPath())
      if (file.exists) {
        const text = (await file.text()).trim()
        if (text) return text
      }
      const id = randomId()
      await writeAtomic(clientIdPath(), id)
      return id
    },

    async readUnprocessed(digest) {
      return readJsonArray(unprocessedPath(digest))
    },

    async writeUnprocessed(digest, rows) {
      await writeAtomic(unprocessedPath(digest), JSON.stringify(rows))
    },

    async readFailures(digest) {
      return readJsonArray(failuresPath(digest))
    },

    async writeFailures(digest, rows) {
      await writeAtomic(failuresPath(digest), JSON.stringify(rows))
    },
  }

  function currentPath() { return `${root}/current` }
  function clientIdPath() { return `${root}/client-id` }
  function subjectDirectory(digest) { return `${root}/subjects/${digest}` }
  function attachmentsDirectory(digest) { return `${subjectDirectory(digest)}/attachments` }
  function pointerPath(digest) { return `${subjectDirectory(digest)}/pointer` }
  function queueDirectory(digest, name) { return `${subjectDirectory(digest)}/${name}` }
  function legacyOutboxPath(digest) { return `${subjectDirectory(digest)}/outbox.jsonl` }
  function queueFileName(seq, id) { return `${String(seq).padStart(10, "0")}-${id}.json` }
  function seqOf(name) {
    const seq = Number(name.slice(0, name.indexOf("-")))
    return Number.isInteger(seq) ? seq : 0
  }

  function listQueueNames(dirPath) {
    const dir = directory(dirPath)
    if (!dir.exists) return []
    const names = []
    for (const entry of dir.list()) {
      const name = String(entry.uri).split("/").pop()
      if (name.endsWith(".json") && !name.startsWith(".")) names.push(name)
    }
    return names.sort()
  }

  async function readQueue(dirPath) {
    const records = []
    for (const name of listQueueNames(dirPath)) {
      records.push(JSON.parse(await new File(`${dirPath}/${name}`).text()))
    }
    return records
  }

  async function findQueueName(dirPath, id) {
    for (const name of listQueueNames(dirPath)) {
      const record = JSON.parse(await new File(`${dirPath}/${name}`).text())
      if (record.id === id) return name
    }
    return null
  }

  async function writeQueueFile(dirPath, seq, record) {
    directory(dirPath).create({ intermediates: true, idempotent: true })
    await writeAtomic(`${dirPath}/${queueFileName(seq, record.id)}`, JSON.stringify(record))
  }

  function clearQueue(dirPath) {
    directory(dirPath).create({ intermediates: true, idempotent: true })
    for (const name of listQueueNames(dirPath)) new File(`${dirPath}/${name}`).delete()
  }

  async function migrateLegacyOutbox(digest) {
    const file = new File(legacyOutboxPath(digest))
    if (!file.exists) return
    const text = await file.text()
    const lines = text.trim() ? text.trim().split("\n") : []
    for (let index = 0; index < lines.length; index += 1) {
      await writeQueueFile(queueDirectory(digest, "inflight"), index + 1, JSON.parse(lines[index]))
    }
    file.delete()
  }
  function unprocessedPath(digest) { return `${subjectDirectory(digest)}/unprocessed.json` }
  function failuresPath(digest) { return `${subjectDirectory(digest)}/failures.json` }
  function blobPath(digest, name) {
    if (!/^[A-Za-z0-9-]+\.[0-9]+$/.test(name)) throw new TypeError("attachment name is invalid")
    return `${attachmentsDirectory(digest)}/${name}`
  }

  async function readJsonArray(path) {
    const file = new File(path)
    if (!file.exists) return []
    const text = await file.text()
    const parsed = text ? JSON.parse(text) : []
    return Array.isArray(parsed) ? parsed : []
  }

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
