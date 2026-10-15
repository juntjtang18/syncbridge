import { randomUUID } from "node:crypto"
import { mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises"
import path from "node:path"
import { sha256, subjectDigest } from "./contracts.js"
import { hashFile } from "./hash-stream.js"

export function createNodeClientStorage(dataDir) {
  return {
    async initSubject(subject) {
      const digest = subjectDigest(subject)
      const directory = subjectDirectory(dataDir, digest)
      await mkdir(directory, { recursive: true })
      await mkdir(queueDirectory(dataDir, digest, "incoming"), { recursive: true })
      await mkdir(queueDirectory(dataDir, digest, "inflight"), { recursive: true })
      await writeIfMissing(path.join(directory, "pointer"), "0")
      await writeIfMissing(path.join(directory, "unprocessed.json"), "[]")
      await writeIfMissing(path.join(directory, "failures.json"), "[]")
      await writeIfMissing(path.join(directory, "subject"), subject)
      return digest
    },

    async readCurrent() {
      try {
        const text = (await readFile(currentPath(dataDir), "utf8")).trim()
        return text || null
      } catch (error) {
        if (error.code === "ENOENT") return null
        throw error
      }
    },

    async writeCurrent(subject) {
      await mkdir(dataDir, { recursive: true })
      await writeAtomic(currentPath(dataDir), subject)
    },

    async listSubjects() {
      let names
      try {
        names = await readdir(path.join(dataDir, "subjects"))
      } catch (error) {
        if (error.code === "ENOENT") return []
        throw error
      }
      const subjects = []
      for (const name of names) {
        try {
          const text = (await readFile(path.join(dataDir, "subjects", name, "subject"), "utf8")).trim()
          if (text) subjects.push(text)
        } catch (error) {
          if (error.code !== "ENOENT") throw error
        }
      }
      return subjects
    },

    async readPointer(digest) {
      const text = await readFile(pointerPath(dataDir, digest), "utf8")
      const pointer = Number(text)
      if (!Number.isInteger(pointer) || pointer < 0) throw new Error("pointer file is invalid")
      return pointer
    },

    async writePointer(digest, position) {
      await writeAtomic(pointerPath(dataDir, digest), String(position))
    },

    async highestSeq(digest) {
      await migrateLegacyOutbox(dataDir, digest)
      const names = [
        ...await listQueueNames(queueDirectory(dataDir, digest, "inflight")),
        ...await listQueueNames(queueDirectory(dataDir, digest, "incoming")),
      ]
      let highest = 0
      for (const name of names) highest = Math.max(highest, seqOf(name))
      return highest
    },

    async appendIncoming(digest, record, seq) {
      await migrateLegacyOutbox(dataDir, digest)
      await writeQueueFile(queueDirectory(dataDir, digest, "incoming"), seq, record)
    },

    async claimInflight(digest) {
      await migrateLegacyOutbox(dataDir, digest)
      const inflightDir = queueDirectory(dataDir, digest, "inflight")
      const pending = await readQueue(inflightDir)
      if (pending.length > 0) return pending
      const incomingDir = queueDirectory(dataDir, digest, "incoming")
      await mkdir(inflightDir, { recursive: true })
      for (const name of await listQueueNames(incomingDir)) {
        await rename(path.join(incomingDir, name), path.join(inflightDir, name))
      }
      return readQueue(inflightDir)
    },

    async readInflight(digest) {
      await migrateLegacyOutbox(dataDir, digest)
      return readQueue(queueDirectory(dataDir, digest, "inflight"))
    },

    async readOutbox(digest) {
      await migrateLegacyOutbox(dataDir, digest)
      return [
        ...await readQueue(queueDirectory(dataDir, digest, "inflight")),
        ...await readQueue(queueDirectory(dataDir, digest, "incoming")),
      ]
    },

    async writeInflight(digest, record) {
      const directory = queueDirectory(dataDir, digest, "inflight")
      const name = await findQueueName(directory, record.id)
      if (!name) throw new Error(`inflight entry is missing: ${record.id}`)
      await writeAtomic(path.join(directory, name), JSON.stringify(record))
    },

    async removeInflight(digest, ids) {
      const drop = new Set(ids)
      const directory = queueDirectory(dataDir, digest, "inflight")
      for (const name of await listQueueNames(directory)) {
        const record = JSON.parse(await readFile(path.join(directory, name), "utf8"))
        if (drop.has(record.id)) await rm(path.join(directory, name), { force: true })
      }
    },

    async replaceOutbox(digest, records) {
      await rm(legacyOutboxPath(dataDir, digest), { force: true })
      await clearQueue(queueDirectory(dataDir, digest, "inflight"))
      await clearQueue(queueDirectory(dataDir, digest, "incoming"))
      for (let index = 0; index < records.length; index += 1) {
        await writeQueueFile(queueDirectory(dataDir, digest, "incoming"), index + 1, records[index])
      }
    },

    async putBlob(digest, expectedHash, bytes) {
      const actual = sha256(bytes)
      if (actual !== expectedHash) throw new Error("attachment hash mismatch")
      const destination = blobPath(dataDir, digest, expectedHash)
      try {
        const existing = await readFile(destination)
        if (sha256(existing) !== expectedHash || existing.length !== bytes.length) {
          throw new Error("attachment cache mismatch")
        }
        return
      } catch (error) {
        if (error.code !== "ENOENT") throw error
      }
      const temporary = path.join(path.dirname(destination), `.${expectedHash}.${randomUUID()}.tmp`)
      await mkdir(path.dirname(destination), { recursive: true })
      await writeFile(temporary, bytes)
      await rename(temporary, destination)
    },

    blobFile(digest, expectedHash) {
      return blobPath(dataDir, digest, expectedHash)
    },

    async hasBlob(digest, expectedHash) {
      try {
        await stat(blobPath(dataDir, digest, expectedHash))
        return true
      } catch (error) {
        if (error.code === "ENOENT") return false
        throw error
      }
    },

    beginDownload(digest) {
      return path.join(subjectDirectory(dataDir, digest), "attachments", `.${randomUUID()}.download`)
    },

    async commitDownload(digest, expectedHash, tempPath) {
      const temp = await stat(tempPath)
      const actual = await hashFile(tempPath)
      if (actual !== expectedHash) {
        await rm(tempPath, { force: true })
        throw new Error("attachment hash mismatch")
      }
      const destination = blobPath(dataDir, digest, expectedHash)
      try {
        const existing = await stat(destination)
        if (existing.size !== temp.size) throw new Error("attachment cache mismatch")
        await rm(tempPath, { force: true })
        return
      } catch (error) {
        if (error.code !== "ENOENT") throw error
      }
      await rename(tempPath, destination)
    },

    async discardDownload(file) {
      await rm(file, { force: true })
    },

    async readBlob(digest, expectedHash) {
      return readFile(blobPath(dataDir, digest, expectedHash))
    },

    async dropBlob(digest, expectedHash) {
      await rm(blobPath(dataDir, digest, expectedHash), { force: true })
    },

    async ensureClientId() {
      const file = path.join(dataDir, "client-id")
      try {
        const text = (await readFile(file, "utf8")).trim()
        if (text) return text
      } catch (error) {
        if (error.code !== "ENOENT") throw error
      }
      const id = randomUUID()
      await mkdir(dataDir, { recursive: true })
      await writeAtomic(file, id)
      return id
    },

    async readUnprocessed(digest) {
      return readJsonArray(unprocessedPath(dataDir, digest))
    },

    async writeUnprocessed(digest, rows) {
      await writeAtomic(unprocessedPath(dataDir, digest), JSON.stringify(rows))
    },

    async readFailures(digest) {
      return readJsonArray(failuresPath(dataDir, digest))
    },

    async writeFailures(digest, rows) {
      await writeAtomic(failuresPath(dataDir, digest), JSON.stringify(rows))
    },
  }
}

function currentPath(dataDir) {
  return path.join(dataDir, "current")
}

function subjectDirectory(dataDir, digest) {
  return path.join(dataDir, "subjects", digest)
}

function pointerPath(dataDir, digest) {
  return path.join(subjectDirectory(dataDir, digest), "pointer")
}

function queueDirectory(dataDir, digest, name) {
  return path.join(subjectDirectory(dataDir, digest), name)
}

function legacyOutboxPath(dataDir, digest) {
  return path.join(subjectDirectory(dataDir, digest), "outbox.jsonl")
}

function queueFileName(seq, id) {
  return `${String(seq).padStart(10, "0")}-${id}.json`
}

function seqOf(name) {
  const seq = Number(name.slice(0, name.indexOf("-")))
  return Number.isInteger(seq) ? seq : 0
}

async function listQueueNames(directory) {
  let names
  try {
    names = await readdir(directory)
  } catch (error) {
    if (error.code === "ENOENT") return []
    throw error
  }
  return names.filter((name) => name.endsWith(".json") && !name.startsWith(".")).sort()
}

async function readQueue(directory) {
  const records = []
  for (const name of await listQueueNames(directory)) {
    records.push(JSON.parse(await readFile(path.join(directory, name), "utf8")))
  }
  return records
}

async function findQueueName(directory, id) {
  for (const name of await listQueueNames(directory)) {
    const record = JSON.parse(await readFile(path.join(directory, name), "utf8"))
    if (record.id === id) return name
  }
  return null
}

async function writeQueueFile(directory, seq, record) {
  await mkdir(directory, { recursive: true })
  await writeAtomic(path.join(directory, queueFileName(seq, record.id)), JSON.stringify(record))
}

async function clearQueue(directory) {
  await mkdir(directory, { recursive: true })
  for (const name of await listQueueNames(directory)) {
    await rm(path.join(directory, name), { force: true })
  }
}

async function migrateLegacyOutbox(dataDir, digest) {
  const file = legacyOutboxPath(dataDir, digest)
  let text
  try {
    text = await readFile(file, "utf8")
  } catch (error) {
    if (error.code === "ENOENT") return
    throw error
  }
  const lines = text.trim() ? text.trim().split("\n") : []
  for (let index = 0; index < lines.length; index += 1) {
    await writeQueueFile(queueDirectory(dataDir, digest, "inflight"), index + 1, JSON.parse(lines[index]))
  }
  await rm(file, { force: true })
}

function unprocessedPath(dataDir, digest) {
  return path.join(subjectDirectory(dataDir, digest), "unprocessed.json")
}

function failuresPath(dataDir, digest) {
  return path.join(subjectDirectory(dataDir, digest), "failures.json")
}

async function readJsonArray(file) {
  try {
    const text = await readFile(file, "utf8")
    const parsed = text ? JSON.parse(text) : []
    return Array.isArray(parsed) ? parsed : []
  } catch (error) {
    if (error.code === "ENOENT") return []
    throw error
  }
}

function blobPath(dataDir, digest, expectedHash) {
  return path.join(subjectDirectory(dataDir, digest), "attachments", expectedHash)
}

async function writeIfMissing(file, contents) {
  try {
    await readFile(file)
  } catch (error) {
    if (error.code !== "ENOENT") throw error
    await writeAtomic(file, contents)
  }
}

async function writeAtomic(file, contents) {
  const temporary = `${file}.${randomUUID()}.tmp`
  await writeFile(temporary, contents)
  await rename(temporary, file)
}
