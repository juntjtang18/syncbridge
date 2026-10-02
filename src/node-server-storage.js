import { randomUUID } from "node:crypto"
import { copyFileSync, existsSync, mkdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs"
import path from "node:path"
import Database from "better-sqlite3"
import { sha256, subjectDigest } from "./contracts.js"
import { hashFile } from "./hash-stream.js"

export function createNodeServerStorage(dataDir) {
  mkdirSync(dataDir, { recursive: true })
  mkdirSync(path.join(dataDir, "attachments"), { recursive: true })
  const db = new Database(path.join(dataDir, "syncbridge.sqlite"))
  db.pragma("journal_mode = WAL")
  db.exec(`
    CREATE TABLE IF NOT EXISTS log_entries (
      subject TEXT NOT NULL,
      position INTEGER NOT NULL,
      id TEXT NOT NULL,
      package_json TEXT NOT NULL,
      UNIQUE (subject, position),
      UNIQUE (subject, id)
    );
    CREATE TABLE IF NOT EXISTS subject_counters (
      subject TEXT PRIMARY KEY,
      next_position INTEGER NOT NULL
    );
  `)

  const selectById = db.prepare("SELECT position FROM log_entries WHERE subject = ? AND id = ?")
  const selectCounter = db.prepare("SELECT next_position FROM subject_counters WHERE subject = ?")
  const insertCounter = db.prepare("INSERT INTO subject_counters (subject, next_position) VALUES (?, ?)")
  const updateCounter = db.prepare("UPDATE subject_counters SET next_position = ? WHERE subject = ?")
  const insertEntry = db.prepare(
    "INSERT INTO log_entries (subject, position, id, package_json) VALUES (?, ?, ?, ?)",
  )
  const selectAfter = db.prepare(`
    SELECT id, position, package_json
    FROM log_entries
    WHERE subject = ? AND position > ?
    ORDER BY position ASC
  `)
  const selectTip = db.prepare("SELECT MAX(position) AS position FROM log_entries WHERE subject = ?")
  const deleteEntry = db.prepare("DELETE FROM log_entries WHERE subject = ? AND id = ? AND position = ?")

  const append = db.transaction((subject, id, packageValue) => {
    const existing = selectById.get(subject, id)
    if (existing) return { duplicate: true, position: existing.position }
    const counter = selectCounter.get(subject)
    const position = counter ? counter.next_position : 1
    if (counter) updateCounter.run(position + 1, subject)
    else insertCounter.run(subject, position + 1)
    insertEntry.run(subject, position, id, JSON.stringify(packageValue))
    return { duplicate: false, position }
  })

  const removeTip = db.transaction((subject, id, position) => {
    const tip = selectTip.get(subject)
    if (!tip || tip.position !== position) throw new Error("cannot roll back a log entry that is not the tip")
    deleteEntry.run(subject, id, position)
    updateCounter.run(position, subject)
  })

  function blobFile(expectedHash) {
    return path.join(dataDir, "blobs", requireHash(expectedHash))
  }

  function subjectFile(subject, expectedHash) {
    return path.join(dataDir, "attachments", subjectDigest(subject), requireHash(expectedHash))
  }

  function writeBlob(expectedHash, bytes) {
    const destination = blobFile(expectedHash)
    mkdirSync(path.dirname(destination), { recursive: true })
    if (existsSync(destination)) {
      if (statSync(destination).size !== bytes.length) throw new Error("attachment cache mismatch")
      return
    }
    const temporary = path.join(dataDir, "tmp", `${randomUUID()}.tmp`)
    mkdirSync(path.dirname(temporary), { recursive: true })
    writeFileSync(temporary, bytes)
    renameSync(temporary, destination)
  }

  function copyBlobToSubject(subject, expectedHash, size) {
    const destination = subjectFile(subject, expectedHash)
    mkdirSync(path.dirname(destination), { recursive: true })
    if (existsSync(destination)) {
      if (statSync(destination).size !== size) throw new Error("attachment cache mismatch")
      return
    }
    copyFileSync(blobFile(expectedHash), destination)
  }

  return {
    appendEntry(subject, id, packageValue) {
      return append(subject, id, packageValue)
    },

    entriesAfter(subject, pointer) {
      return selectAfter.all(subject, pointer).map((row) => ({
        id: row.id,
        position: row.position,
        package: JSON.parse(row.package_json),
      }))
    },

    removeTip(subject, id, position) {
      removeTip(subject, id, position)
    },

    putAttachment(subject, expectedHash, bytes) {
      const actual = sha256(bytes)
      if (actual !== expectedHash) throw new Error("attachment hash mismatch")
      writeBlob(expectedHash, bytes)
      copyBlobToSubject(subject, expectedHash, bytes.length)
    },

    beginBlob() {
      const directory = path.join(dataDir, "tmp")
      mkdirSync(directory, { recursive: true })
      return path.join(directory, `${randomUUID()}.tmp`)
    },

    discardBlob(file) {
      rmSync(file, { force: true })
    },

    commitBlob(expectedHash, tempPath, size) {
      const temp = statSync(tempPath)
      if (temp.size !== size) throw new Error("attachment hash mismatch")
      const destination = blobFile(expectedHash)
      mkdirSync(path.dirname(destination), { recursive: true })
      if (existsSync(destination)) {
        const existing = statSync(destination)
        rmSync(tempPath, { force: true })
        if (existing.size !== size) throw new Error("attachment cache mismatch")
        return
      }
      renameSync(tempPath, destination)
    },

    async installAttachment(subject, expectedHash, size) {
      const source = blobFile(expectedHash)
      let info
      try {
        info = statSync(source)
      } catch (error) {
        if (error.code === "ENOENT") throw new Error("attachment verification failed")
        throw error
      }
      if (info.size !== size || await hashFile(source) !== expectedHash) {
        throw new Error("attachment verification failed")
      }
      copyBlobToSubject(subject, expectedHash, size)
    },

    openBlob(expectedHash) {
      const file = blobFile(expectedHash)
      try {
        return { file, size: statSync(file).size }
      } catch (error) {
        if (error.code === "ENOENT") return null
        throw error
      }
    },

    readAttachment(subject, expectedHash) {
      return readFileSync(subjectFile(subject, expectedHash))
    },

    close() {
      db.close()
    },
  }
}

function requireHash(hash) {
  if (!/^[0-9a-f]{64}$/.test(hash)) throw new TypeError("sha256 is invalid")
  return hash
}
