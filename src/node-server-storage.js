import { randomUUID } from "node:crypto"
import { mkdirSync, readFileSync, renameSync, rmSync, statSync } from "node:fs"
import path from "node:path"
import Database from "better-sqlite3"

// The server's state under `dataDir`:
//   syncbridge.sqlite  each subject's log, the changes waiting to be applied, and given-up applies
//   uploads/<upload>   one file per PUT, until its change is applied
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
  `)
  const uploads = path.join(dataDir, "uploads")
  mkdirSync(uploads, { recursive: true })

  const q = {
    entry: db.prepare("SELECT id, position, package_json, client_id FROM log_entries WHERE subject = ? AND id = ?"),
    counter: db.prepare("SELECT next_position FROM subject_counters WHERE subject = ?"),
    setCounter: db.prepare("INSERT INTO subject_counters (subject, next_position) VALUES (?, ?) ON CONFLICT (subject) DO UPDATE SET next_position = excluded.next_position"),
    insert: db.prepare("INSERT INTO log_entries (subject, position, id, package_json, client_id) VALUES (?, ?, ?, ?, ?)"),
    page: db.prepare("SELECT id, position, package_json, client_id FROM log_entries WHERE subject = ? AND position > ? ORDER BY position LIMIT ?"),
    after: db.prepare("SELECT count(*) AS n FROM log_entries WHERE subject = ? AND position > ?"),
    between: db.prepare("SELECT id, position, package_json, client_id FROM log_entries WHERE subject = ? AND position > ? AND position < ? ORDER BY position"),
    setPackage: db.prepare("UPDATE log_entries SET package_json = ? WHERE subject = ? AND id = ?"),
    enqueue: db.prepare("INSERT OR IGNORE INTO unprocessed (subject, position, id, package_json) VALUES (?, ?, ?, ?)"),
    head: db.prepare("SELECT subject, position, id, package_json FROM unprocessed ORDER BY seq LIMIT ?"),
    dequeue: db.prepare("DELETE FROM unprocessed WHERE subject = ? AND id = ?"),
    waiting: db.prepare("SELECT count(*) AS n FROM unprocessed"),
    pending: db.prepare("SELECT DISTINCT subject FROM unprocessed ORDER BY subject"),
    fail: db.prepare("INSERT OR REPLACE INTO failed_entries (subject, position, id, client_id, kind, error, failed_at) VALUES (?, ?, ?, '', 'apply', ?, ?)"),
    failures: db.prepare("SELECT id, position, error, failed_at FROM failed_entries WHERE subject = ? ORDER BY position"),
  }
  const row = (found) => found && { id: found.id, position: found.position, package: JSON.parse(found.package_json), clientId: found.client_id || null }

  const append = db.transaction((subject, id, packageValue, clientId) => {
    const existing = q.entry.get(subject, id)
    if (existing) return { duplicate: true, position: existing.position }
    const position = q.counter.get(subject)?.next_position ?? 1
    q.setCounter.run(subject, position + 1)
    q.insert.run(subject, position, id, JSON.stringify(packageValue), clientId || null)
    return { duplicate: false, position }
  })

  function uploadFile(upload) {
    if (typeof upload !== "string" || !/^[0-9a-f-]{16,64}$/.test(upload)) throw new TypeError("upload is invalid")
    return path.join(uploads, upload)
  }

  return {
    appendEntry: (subject, id, packageValue, clientId = null) => append(subject, id, packageValue, clientId),
    entryById: (subject, id) => row(q.entry.get(subject, id)),
    page: (subject, after, limit) => q.page.all(subject, after, limit).map(row),
    countAfter: (subject, after) => q.after.get(subject, after).n,
    entriesBetween: (subject, after, before) => q.between.all(subject, after, before).map(row),

    // The host's apply may move an attachment; later downloads read it from its new path.
    setAttachmentPaths(subject, id, moved) {
      const entry = row(q.entry.get(subject, id))
      const byId = new Map(moved.map((item) => [item.id, item.path]))
      const attachments = entry.package.attachments.map((manifest) => ({ ...manifest, path: byId.get(manifest.id) || manifest.path }))
      q.setPackage.run(JSON.stringify({ ...entry.package, attachments }), subject, id)
    },

    enqueue: (subject, position, id, packageValue) => q.enqueue.run(subject, position, id, JSON.stringify(packageValue)),
    unprocessedHead: (limit) => q.head.all(limit).map((found) => ({ ...row(found), subject: found.subject })),
    dequeue: (subject, id) => q.dequeue.run(subject, id),
    hasUnprocessed: () => q.waiting.get().n > 0,
    pendingSubjects: () => q.pending.all().map((found) => found.subject),

    recordFailure: (subject, position, id, error) => q.fail.run(subject, position, id, error, new Date().toISOString()),
    failedEntries: (subject) => q.failures.all(subject).map((found) => ({ id: found.id, position: found.position, error: found.error, failedAt: found.failed_at })),

    // A PUT streams into a temp file, which becomes uploads/<upload> once fully written.
    uploadTemp: () => path.join(uploads, `.${randomUUID()}.tmp`),
    commitUpload: (upload, temp) => renameSync(temp, uploadFile(upload)),
    discard: (temp) => rmSync(temp, { force: true }),
    uploadSize(upload) {
      try {
        return statSync(uploadFile(upload)).size
      } catch (error) {
        if (error.code === "ENOENT") return null
        throw error
      }
    },
    uploadPath: (upload) => uploadFile(upload),
    readUpload: (upload) => new Uint8Array(readFileSync(uploadFile(upload))),
    dropUpload: (upload) => rmSync(uploadFile(upload), { force: true }),

    close: () => db.close(),
  }
}
