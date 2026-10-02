import { randomUUID } from "node:crypto"
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises"
import path from "node:path"
import { sha256, subjectDigest } from "./contracts.js"
import { hashFile } from "./hash-stream.js"

export function createNodeClientStorage(dataDir) {
  return {
    async initSubject(subject) {
      const digest = subjectDigest(subject)
      const directory = subjectDirectory(dataDir, digest)
      await mkdir(path.join(directory, "attachments"), { recursive: true })
      await writeIfMissing(path.join(directory, "pointer"), "0")
      await writeIfMissing(path.join(directory, "outbox.jsonl"), "")
      return digest
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

    async readOutbox(digest) {
      const text = await readFile(outboxPath(dataDir, digest), "utf8")
      if (text.length === 0) return []
      return text.trim().split("\n").map((line) => JSON.parse(line))
    },

    async appendOutbox(digest, record) {
      const file = outboxPath(dataDir, digest)
      const current = await readFile(file, "utf8")
      const next = current.length === 0 || current.endsWith("\n") ? current : `${current}\n`
      await writeAtomic(file, `${next}${JSON.stringify(record)}\n`)
    },

    async removeOutbox(digest, ids) {
      const drop = new Set(ids)
      const records = await this.readOutbox(digest)
      const kept = records.filter((record) => !drop.has(record.id))
      const text = kept.length === 0 ? "" : `${kept.map((record) => JSON.stringify(record)).join("\n")}\n`
      await writeAtomic(outboxPath(dataDir, digest), text)
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
  }
}

function subjectDirectory(dataDir, digest) {
  return path.join(dataDir, "subjects", digest)
}

function pointerPath(dataDir, digest) {
  return path.join(subjectDirectory(dataDir, digest), "pointer")
}

function outboxPath(dataDir, digest) {
  return path.join(subjectDirectory(dataDir, digest), "outbox.jsonl")
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
