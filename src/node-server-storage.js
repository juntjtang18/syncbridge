import { randomUUID } from "node:crypto"
import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs"
import path from "node:path"
import Database from "better-sqlite3"

export function createNodeServerStorage(dataDir) {
  mkdirSync(dataDir, { recursive: true })
  const db = new Database(path.join(dataDir, "syncbridge.sqlite"))
  db.pragma("journal_mode = WAL")
  db.exec(`
    CREATE TABLE IF NOT EXISTS log_entries (
      subject TEXT NOT NULL,
      position INTEGER NOT NULL,
      id TEXT NOT NULL,
      package_json TEXT NOT NULL,
      client_id TEXT,
      UNIQUE (subject, position),
      UNIQUE (subject, id)
    );
    CREATE TABLE IF NOT EXISTS subject_counters (
      subject TEXT PRIMARY KEY,
      next_position INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS unprocessed (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      subject TEXT NOT NULL,
      position INTEGER NOT NULL,
      id TEXT NOT NULL,
      package_json TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      UNIQUE (subject, id)
    );
    CREATE TABLE IF NOT EXISTS failed_entries (
      subject TEXT NOT NULL,
      position INTEGER,
      id TEXT NOT NULL,
      client_id TEXT NOT NULL DEFAULT '',
      kind TEXT NOT NULL,
      error TEXT NOT NULL,
      failed_at TEXT NOT NULL,
      PRIMARY KEY (subject, id, client_id, kind)
    );
    CREATE TABLE IF NOT EXISTS send_attempts (
      subject TEXT NOT NULL,
      client_id TEXT NOT NULL,
      id TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      PRIMARY KEY (subject, client_id, id)
    );
  `)
  const logColumns = db.prepare("PRAGMA table_info(log_entries)").all()
  if (!logColumns.some((column) => column.name === "client_id")) {
    db.exec("ALTER TABLE log_entries ADD COLUMN client_id TEXT")
  }
  if (!logColumns.some((column) => column.name === "base_position")) {
    db.exec("ALTER TABLE log_entries ADD COLUMN base_position INTEGER")
  }

  const selectById = db.prepare("SELECT position FROM log_entries WHERE subject = ? AND id = ?")
  const countUnprocessed = db.prepare("SELECT count(*) AS n FROM unprocessed WHERE subject = ?")
  const selectApplyFailuresFor = db.prepare(`
    SELECT f.id, f.position, f.error, f.failed_at FROM failed_entries f
    JOIN log_entries l ON l.subject = f.subject AND l.id = f.id
    WHERE f.subject = ? AND f.kind = 'apply' AND l.client_id = ?
    ORDER BY f.position
  `)
  const selectUnprocessedUsingBlob = db.prepare("SELECT 1 FROM unprocessed WHERE package_json LIKE ? LIMIT 1")
  const selectEntry = db.prepare("SELECT package_json FROM log_entries WHERE subject = ? AND id = ?")
  const selectCounter = db.prepare("SELECT next_position FROM subject_counters WHERE subject = ?")
  const insertCounter = db.prepare("INSERT INTO subject_counters (subject, next_position) VALUES (?, ?)")
  const updateCounter = db.prepare("UPDATE subject_counters SET next_position = ? WHERE subject = ?")
  const insertEntry = db.prepare(
    "INSERT INTO log_entries (subject, position, id, package_json, client_id, base_position) VALUES (?, ?, ?, ?, ?, ?)",
  )
  const updatePackage = db.prepare("UPDATE log_entries SET package_json = ? WHERE subject = ? AND id = ?")
  const selectAfter = db.prepare(`
    SELECT id, position, package_json, client_id, base_position
    FROM log_entries
    WHERE subject = ? AND position > ?
    ORDER BY position ASC
  `)
  const selectBetween = db.prepare(`
    SELECT id, position, package_json, client_id, base_position
    FROM log_entries
    WHERE subject = ? AND position > ? AND position < ?
    ORDER BY position ASC
  `)
  const selectOrigin = db.prepare("SELECT client_id, base_position FROM log_entries WHERE subject = ? AND id = ?")
  const selectTip = db.prepare("SELECT MAX(position) AS position FROM log_entries WHERE subject = ?")
  const deleteEntry = db.prepare("DELETE FROM log_entries WHERE subject = ? AND id = ? AND position = ?")
  const selectPackages = db.prepare(`
    SELECT package_json
    FROM log_entries
    WHERE subject = ?
    ORDER BY position DESC
  `)
  const selectEntryById = db.prepare(`
    SELECT id, position, package_json
    FROM log_entries
    WHERE subject = ? AND id = ?
  `)
  const insertUnprocessed = db.prepare(`
    INSERT OR IGNORE INTO unprocessed (subject, position, id, package_json, attempts)
    VALUES (?, ?, ?, ?, 0)
  `)
  const selectUnprocessedBySubject = db.prepare(`
    SELECT position, id, package_json, attempts
    FROM unprocessed
    WHERE subject = ?
    ORDER BY seq ASC
  `)
  const deleteUnprocessedSubject = db.prepare("DELETE FROM unprocessed WHERE subject = ?")
  const insertUnprocessedRow = db.prepare(`
    INSERT INTO unprocessed (subject, position, id, package_json, attempts)
    VALUES (?, ?, ?, ?, ?)
  `)
  const selectHead = db.prepare(`
    SELECT subject, position, id, package_json, attempts
    FROM unprocessed
    ORDER BY seq ASC
    LIMIT 1
  `)
  const selectUnprocessedCount = db.prepare("SELECT COUNT(*) AS count FROM unprocessed")
  const updateAttempts = db.prepare("UPDATE unprocessed SET attempts = ? WHERE subject = ? AND id = ?")
  const deleteUnprocessed = db.prepare("DELETE FROM unprocessed WHERE subject = ? AND id = ?")
  const insertFailure = db.prepare(`
    INSERT OR REPLACE INTO failed_entries (subject, position, id, client_id, kind, error, failed_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `)
  const selectFailures = db.prepare(`
    SELECT position, id, client_id, kind, error, failed_at
    FROM failed_entries
    WHERE subject = ?
    ORDER BY failed_at ASC, id ASC
  `)
  const selectSendFailure = db.prepare(`
    SELECT position, id
    FROM failed_entries
    WHERE subject = ? AND id = ? AND client_id = ? AND kind = 'send'
  `)
  const selectSendAttempt = db.prepare(`
    SELECT attempts FROM send_attempts WHERE subject = ? AND client_id = ? AND id = ?
  `)
  const insertSendAttempt = db.prepare(`
    INSERT INTO send_attempts (subject, client_id, id, attempts) VALUES (?, ?, ?, ?)
  `)
  const updateSendAttempt = db.prepare(`
    UPDATE send_attempts SET attempts = ? WHERE subject = ? AND client_id = ? AND id = ?
  `)

  const append = db.transaction((subject, id, packageValue, clientId, base) => {
    const existing = selectById.get(subject, id)
    if (existing) return { duplicate: true, position: existing.position }
    const counter = selectCounter.get(subject)
    const position = counter ? counter.next_position : 1
    if (counter) updateCounter.run(position + 1, subject)
    else insertCounter.run(subject, position + 1)
    insertEntry.run(subject, position, id, JSON.stringify(packageValue), clientId || null, Number.isInteger(base) ? base : null)
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

  return {
    // `base` is the sender's pointer when it sent the change: the last position it had seen.
    appendEntry(subject, id, packageValue, clientId, base) {
      return append(subject, id, packageValue, clientId, base)
    },

    entryOrigin(subject, id) {
      const row = selectOrigin.get(subject, id)
      if (!row) return { clientId: null, base: null }
      return { clientId: row.client_id || null, base: Number.isInteger(row.base_position) ? row.base_position : null }
    },

    entriesBetween(subject, after, before) {
      return selectBetween.all(subject, after, before).map(logRow)
    },

    hasEntry(subject, id) {
      return Boolean(selectById.get(subject, id))
    },

    entriesAfter(subject, pointer) {
      return selectAfter.all(subject, pointer).map(logRow)
    },

    findAttachment(subject, hash) {
      requireHash(hash)
      for (const row of selectPackages.all(subject)) {
        const packageValue = JSON.parse(row.package_json)
        const manifest = (packageValue.attachments || []).find((item) => item && item.sha256 === hash)
        if (manifest) return manifest
      }
      return null
    },

    updateAttachmentPaths(subject, id, updates) {
      if (!Array.isArray(updates) || updates.length === 0) return
      const row = selectEntry.get(subject, id)
      if (!row) return
      const packageValue = JSON.parse(row.package_json)
      const byId = new Map(updates.map((item) => [item.id, item]))
      for (const manifest of packageValue.attachments) {
        const next = byId.get(manifest.id)
        if (next && typeof next.path === "string" && next.path.length > 0) {
          manifest.path = next.path
        }
      }
      updatePackage.run(JSON.stringify(packageValue), subject, id)
    },

    removeTip(subject, id, position) {
      removeTip(subject, id, position)
    },

    entryById(subject, id) {
      const row = selectEntryById.get(subject, id)
      if (!row) return null
      return { id: row.id, position: row.position, package: JSON.parse(row.package_json) }
    },

    enqueueUnprocessed(subject, position, id, packageValue) {
      insertUnprocessed.run(subject, position, id, JSON.stringify(packageValue))
    },

    listUnprocessed(subject) {
      return selectUnprocessedBySubject.all(subject).map((row) => ({
        id: row.id,
        position: row.position,
        package: JSON.parse(row.package_json),
        attempts: row.attempts,
      }))
    },

    replaceUnprocessed(subject, rows) {
      const replace = db.transaction((items) => {
        deleteUnprocessedSubject.run(subject)
        for (const row of items) {
          insertUnprocessedRow.run(subject, row.position, row.id, JSON.stringify(row.package), row.attempts)
        }
      })
      replace(rows)
    },

    headUnprocessed() {
      const row = selectHead.get()
      if (!row) return null
      return {
        subject: row.subject,
        position: row.position,
        id: row.id,
        package: JSON.parse(row.package_json),
        attempts: row.attempts,
      }
    },

    // Changes this client sent that the server could not apply, for the client's failure list.
    applyFailuresFor(subject, clientId) {
      return selectApplyFailuresFor.all(subject, clientId).map((row) => ({ id: row.id, position: row.position, error: row.error, failedAt: row.failed_at }))
    },

    unprocessedCount(subject) {
      return countUnprocessed.get(subject).n
    },

    hasUnprocessed() {
      return selectUnprocessedCount.get().count > 0
    },

    setUnprocessedAttempts(subject, id, attempts) {
      updateAttempts.run(attempts, subject, id)
    },

    removeUnprocessed(subject, id) {
      deleteUnprocessed.run(subject, id)
    },

    recordFailure({ subject, position, id, clientId, kind, error, failedAt }) {
      insertFailure.run(subject, position ?? null, id, clientId || "", kind, error, failedAt)
    },

    failedEntries(subject) {
      return selectFailures.all(subject).map((row) => ({
        id: row.id,
        position: row.position,
        clientId: row.client_id || null,
        kind: row.kind,
        error: row.error,
        failedAt: row.failed_at,
      }))
    },

    sendFailure(subject, clientId, id) {
      return selectSendFailure.get(subject, id, clientId) || null
    },

    sendAttempts(subject, clientId, id) {
      return selectSendAttempt.get(subject, clientId, id)?.attempts || 0
    },

    incrementSendAttempts(subject, clientId, id) {
      const current = selectSendAttempt.get(subject, clientId, id)
      const next = current ? current.attempts + 1 : 1
      if (current) updateSendAttempt.run(next, subject, clientId, id)
      else insertSendAttempt.run(subject, clientId, id, next)
      return next
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

    requireBlob(expectedHash, size) {
      const hash = requireHash(expectedHash)
      const file = blobFile(hash)
      let info
      try {
        info = statSync(file)
      } catch (error) {
        if (error.code === "ENOENT") throw attachmentError("ATTACHMENT_NOT_FOUND", `ATTACHMENT_NOT_FOUND sha256 ${hash}`)
        throw error
      }
      if (info.size !== size) {
        throw attachmentError(
          "ATTACHMENT_SIZE_MISMATCH",
          `ATTACHMENT_SIZE_MISMATCH sha256 ${hash} expected ${size} actual ${info.size}`,
        )
      }
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

    readBlob(expectedHash) {
      return readFileSync(blobFile(expectedHash))
    },

    dropBlob(expectedHash) {
      rmSync(blobFile(expectedHash), { force: true })
    },

    unprocessedUsesBlob(expectedHash) {
      return Boolean(selectUnprocessedUsingBlob.get(`%"sha256":"${requireHash(expectedHash)}"%`))
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

function attachmentError(code, message) {
  const error = new Error(message)
  error.code = code
  return error
}

function logRow(row) {
  return {
    id: row.id,
    position: row.position,
    package: JSON.parse(row.package_json),
    clientId: row.client_id || null,
    base: Number.isInteger(row.base_position) ? row.base_position : null,
  }
}
