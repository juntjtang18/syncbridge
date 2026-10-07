import { createHash } from "node:crypto"
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import Database from "better-sqlite3"
import assert from "node:assert/strict"
import test from "node:test"
import { subjectDigest } from "../src/index.js"
import { createSyncClient, createSyncServer } from "../src/node.js"
import { listen } from "../testing/listen.js"

const subject = "tenant.company-123"

test("init validates inputs and creates subject state without syncing", async (t) => {
  const dirs = await tempDirs(t)
  let headersCalled = 0
  const client = createSyncClient({
    dataDir: dirs.client,
    url: "http://127.0.0.1:9/sync",
    headers() {
      headersCalled += 1
      return {}
    },
  })

  await assert.rejects(() => client.sync(), /not initialized/)
  await assert.rejects(() => client.appendChanges({ data: "x" }), /not initialized/)
  await assert.rejects(() => client.init("", async () => {}), /subject/)
  await assert.rejects(() => client.init(subject, null), /applyChange/)

  await client.init(subject, async () => {})
  await client.use(subject, async () => {})
  assert.equal(await client.current(), subject)
  assert.deepEqual(await client.subjects(), [subject])
  assert.equal(headersCalled, 0)

  const digest = subjectDigest(subject)
  const names = await readdir(path.join(dirs.client, "subjects"))
  assert.deepEqual(names, [digest])
  assert.equal(digest.includes(subject), false)
  assert.equal(await readFile(path.join(dirs.client, "subjects", digest, "pointer"), "utf8"), "0")
  assert.equal(await readFile(path.join(dirs.client, "subjects", digest, "outbox.jsonl"), "utf8"), "")
  assert.equal(await readFile(path.join(dirs.client, "subjects", digest, "subject"), "utf8"), subject)
  assert.equal(await readFile(path.join(dirs.client, "current"), "utf8"), subject)
  await client.appendChanges({ data: { ok: true } })
  await assert.rejects(() => client.appendChanges({ data: undefined }), /package.data/)
  await assert.rejects(
    () => client.appendChanges({ data: "x", attachments: [{ id: "photo" }] }),
    /attachment.bytes is required/,
  )
  await assert.rejects(
    () => client.appendChanges({
      data: "x",
      attachments: [
        { id: "a", bytes: Buffer.from("a") },
        { id: "a", bytes: Buffer.from("b") },
      ],
    }),
    /duplicate attachment id/,
  )
})

test("two clients sync one subject through the server log", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.from("photo-bytes")
  const photoHash = sha256(photo)
  let serverApplies = 0
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async (entry, attachments) => {
    serverApplies += 1
    const file = attachments.get("photo-1")
    assert.equal(file.sha256, photoHash)
    assert.deepEqual(Buffer.from(await file.read()), photo)
    assert.equal(entry.package.data.poi, "north-wall")
  })

  const clientA = createSyncClient({
    dataDir: dirs.clientA,
    url: endpoints.url,
  })
  await clientA.init(subject, async () => {})
  const appended = await clientA.appendChanges({
    data: {
      poi: "north-wall",
      poiEntries: [{ description: "Close-up", photo: { attachmentId: "photo-1" } }],
    },
    attachments: [{ id: "photo-1", bytes: photo, contentType: "image/jpeg", name: "poi-1.jpg" }],
  })

  const digest = subjectDigest(subject)
  const cached = await readFile(path.join(dirs.clientA, "subjects", digest, "attachments", photoHash))
  assert.deepEqual(cached, photo)
  const outbox = await readFile(path.join(dirs.clientA, "subjects", digest, "outbox.jsonl"), "utf8")
  assert.match(outbox, new RegExp(appended.id))

  await clientA.sync()
  assert.equal(serverApplies, 1)
  assert.equal(await readFile(path.join(dirs.clientA, "subjects", digest, "pointer"), "utf8"), "1")
  assert.equal(await readFile(path.join(dirs.clientA, "subjects", digest, "outbox.jsonl"), "utf8"), "")
  const serverPhoto = await readFile(path.join(dirs.server, "attachments", digest, photoHash))
  assert.deepEqual(serverPhoto, photo)

  const seen = []
  const clientB = createSyncClient({
    dataDir: dirs.clientB,
    url: endpoints.url,
  })
  let attempts = 0
  await clientB.init(subject, async (entry, attachments) => {
    attempts += 1
    if (attempts === 1) throw new Error("apply failed")
    seen.push({ data: entry.package.data, bytes: await attachments.get("photo-1").read() })
  })
  await assert.rejects(() => clientB.sync(), /apply failed/)
  assert.equal(await readFile(path.join(dirs.clientB, "subjects", digest, "pointer"), "utf8"), "0")
  await clientB.sync()
  assert.equal(attempts, 2)
  assert.equal(seen.length, 1)
  assert.equal(seen[0].data.poi, "north-wall")
  assert.deepEqual(Buffer.from(seen[0].bytes), photo)
  assert.equal(await readFile(path.join(dirs.clientB, "subjects", digest, "pointer"), "utf8"), "1")

  const db = new Database(path.join(dirs.server, "syncbridge.sqlite"), { readonly: true })
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").all()
  assert.deepEqual(tables.map((row) => row.name).sort(), ["log_entries", "subject_counters"])
  db.close()
})

test("a failed server apply rolls the log entry back for retry", async (t) => {
  const dirs = await tempDirs(t)
  let calls = 0
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {
    calls += 1
    if (calls === 1) throw new Error("server failed")
  })
  const client = createSyncClient({
    dataDir: dirs.client,
    url: endpoints.url,
  })
  const positions = []
  await client.init(subject, async (entry) => {
    positions.push(entry.position)
  })
  await client.appendChanges({ data: "retry" })
  await assert.rejects(() => client.sync(), /server failed/)
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "0")
  await client.sync()
  assert.equal(calls, 2)
  assert.deepEqual(positions, [1])
})

test("opening an existing server keeps the log and a server append is not applied again", async (t) => {
  const dirs = await tempDirs(t)
  let applies = 0
  const first = createSyncServer({ dataDir: dirs.server })
  first.init(subject, async () => {
    applies += 1
  })
  await first.appendChanges(subject, { data: "kept" })
  first.close()
  assert.equal(applies, 0)

  const second = createSyncServer({ dataDir: dirs.server })
  t.after(() => second.close())
  const endpoints = await listen(second, t)
  second.init(subject, async () => {
    applies += 1
  })
  assert.throws(() => second.init(subject, async () => {}), /already initialized/)
  const client = createSyncClient({
    dataDir: dirs.client,
    url: endpoints.url,
  })
  const received = []
  await client.init(subject, async (entry) => {
    received.push(entry)
  })
  await client.sync()
  assert.equal(applies, 0)
  assert.equal(received.length, 1)
  assert.equal(received[0].position, 1)
  assert.equal(received[0].package.data, "kept")
})

test("use switches current and keeps the previous subject's pointer and outbox", async (t) => {
  const dirs = await tempDirs(t)
  const other = "user.4.google.folder-b"
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {})
  server.init(other, async () => {})

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subject, async () => {})
  await client.appendChanges({ data: "queued-on-a" })
  await client.sync()
  await client.appendChanges({ data: "still-on-a" })
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")

  await client.use(other, async () => {})
  assert.equal(await client.current(), other)
  assert.deepEqual(new Set(await client.subjects()), new Set([subject, other]))
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(other), "pointer"), "utf8"), "0")
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")
  const outboxA = await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "outbox.jsonl"), "utf8")
  assert.match(outboxA, /still-on-a/)
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(other), "outbox.jsonl"), "utf8"), "")

  await client.use(subject, async () => {})
  assert.equal(await client.current(), subject)
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")
})

test("a live subject change does not write the old log and reuses a known pointer", async (t) => {
  const dirs = await tempDirs(t)
  const liveB = "user.4.google.folder-b"
  let live = subject
  const appliedA = []
  const appliedB = []
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() {
      return live
    },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {})
  server.init(liveB, async () => {})

  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.use(subject, async (entry) => {
    appliedA.push(entry.package.data)
  })
  await client.appendChanges({ data: "on-a" })
  const first = await client.sync()
  assert.deepEqual(first, { subject })
  assert.deepEqual(appliedA, ["on-a"])
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")

  live = liveB
  const switched = await client.sync()
  assert.deepEqual(switched, { switched: true, subject: liveB })
  assert.equal(await client.current(), liveB)
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(liveB), "pointer"), "utf8"), "0")
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")
  assert.deepEqual(appliedA, ["on-a"])
  const db = new Database(path.join(dirs.server, "syncbridge.sqlite"), { readonly: true })
  const rows = db.prepare("SELECT subject, package_json FROM log_entries").all()
  db.close()
  assert.equal(rows.length, 1)
  assert.equal(rows[0].subject, subject)

  await client.use(liveB, async (entry) => {
    appliedB.push(entry.package.data)
  })
  await client.appendChanges({ data: "on-b" })
  const syncedB = await client.sync()
  assert.deepEqual(syncedB, { subject: liveB })
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(liveB), "pointer"), "utf8"), "1")
  assert.deepEqual(appliedB, ["on-b"])

  live = subject
  const back = await client.sync()
  assert.deepEqual(back, { switched: true, subject })
  await client.use(subject, async (entry) => {
    appliedA.push(entry.package.data)
  })
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")
})

async function tempDirs(t) {
  const root = await mkdtemp(path.join(tmpdir(), "syncbridge-"))
  t.after(() => rm(root, { recursive: true, force: true }))
  return {
    root,
    server: path.join(root, "server"),
    client: path.join(root, "client"),
    clientA: path.join(root, "client-a"),
    clientB: path.join(root, "client-b"),
  }
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex")
}
