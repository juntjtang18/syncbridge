import { createHash } from "node:crypto"
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import Database from "better-sqlite3"
import assert from "node:assert/strict"
import test from "node:test"
import { randomId, sha256 as digestBytes } from "../src/contracts.js"
import { createHttpTransport } from "../src/http-transport.js"
import { subjectDigest } from "../src/index.js"
import { createNodeClientStorage } from "../src/node-client-storage.js"
import { createSyncClient, createSyncServer } from "../src/node.js"
import { createClient } from "../src/sync-client.js"
import { createServer } from "../src/sync-server.js"
import { createNodeServerStorage } from "../src/node-server-storage.js"
import { assertNoClientStaging, assertNoServerStaging, createHostStore } from "../testing/host.js"
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
    /attachment.path/,
  )
  await assert.rejects(
    () => client.appendChanges({
      data: "x",
      attachments: [
        { id: "a", path: "a.jpg" },
        { id: "a", path: "b.jpg" },
      ],
    }),
    /duplicate attachment id/,
  )
})

test("two clients sync one subject through the server log", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.from("photo-bytes")
  const photoHash = sha256(photo)
  const store = createHostStore([["poi-1.jpg", photo]])
  let serverApplies = 0
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() { return subject },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async (entry, attachments) => {
    serverApplies += 1
    const file = attachments.get("photo-1")
    assert.equal(file.sha256, photoHash)
    assert.deepEqual(Buffer.from(await file.read()), photo)
    assert.equal(entry.package.data.poi, "north-wall")
    store.put("server/poi-1.jpg", photo)
    return { attachments: [{ id: "photo-1", path: "server/poi-1.jpg" }] }
  }, { readAttachment: store.readAttachment })

  const clientA = createSyncClient({
    dataDir: dirs.clientA,
    url: endpoints.url,
  })
  await clientA.init(subject, async () => {}, { readAttachment: store.readAttachment })
  const appended = await clientA.appendChanges({
    data: {
      poi: "north-wall",
      poiEntries: [{ description: "Close-up", photo: { attachmentId: "photo-1" } }],
    },
    attachments: [{ id: "photo-1", path: "poi-1.jpg", contentType: "image/jpeg" }],
  })

  const digest = subjectDigest(subject)
  await assertNoClientStaging(dirs.clientA)
  const outbox = await readFile(path.join(dirs.clientA, "subjects", digest, "outbox.jsonl"), "utf8")
  assert.match(outbox, new RegExp(appended.id))

  await clientA.sync()
  await server.whenIdle()
  await clientA.whenIdle()
  assert.equal(serverApplies, 1)
  assert.equal(await readFile(path.join(dirs.clientA, "subjects", digest, "pointer"), "utf8"), "1")
  assert.equal(await readFile(path.join(dirs.clientA, "subjects", digest, "outbox.jsonl"), "utf8"), "")
  await assertNoServerStaging(dirs.server)
  await assertNoClientStaging(dirs.clientA)

  const seen = []
  const clientB = createSyncClient({
    dataDir: dirs.clientB,
    url: endpoints.url,
  })
  let attempts = 0
  await clientB.init(subject, async (entry, attachments) => {
    attempts += 1
    if (entry.package.data.poi === "north-wall") throw new Error("apply failed")
    seen.push({ data: entry.package.data, bytes: await attachments.get("photo-1").read() })
  }, { readAttachment: store.readAttachment })
  const synced = await clientB.sync()
  await clientB.whenIdle()
  assert.deepEqual(synced, { status: "succeed", subject })
  assert.equal(await readFile(path.join(dirs.clientB, "subjects", digest, "pointer"), "utf8"), "1")
  assert.equal(attempts, 3)
  assert.equal(seen.length, 0)
  const failed = await clientB.failedEntries()
  assert.equal(failed.length, 1)
  assert.match(failed[0].error, /apply failed after 3 attempts: apply failed/)

  await clientA.appendChanges({
    data: { poi: "later" },
    attachments: [{ id: "photo-1", path: "poi-1.jpg", contentType: "image/jpeg" }],
  })
  await clientA.sync()
  await server.whenIdle()
  await clientB.sync()
  await clientB.whenIdle()
  assert.equal(attempts, 4)
  assert.equal(seen.length, 1)
  assert.equal(seen[0].data.poi, "later")
  assert.deepEqual(Buffer.from(seen[0].bytes), photo)

  const db = new Database(path.join(dirs.server, "syncbridge.sqlite"), { readonly: true })
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").all()
  assert.deepEqual(tables.map((row) => row.name).sort(), ["failed_entries", "log_entries", "send_attempts", "subject_counters", "unprocessed"])
  db.close()
})

test("a failed server apply keeps the log and records the apply error", async (t) => {
  const dirs = await tempDirs(t)
  let calls = 0
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {
    calls += 1
    throw new Error("server failed")
  })
  const client = createSyncClient({
    dataDir: dirs.client,
    url: endpoints.url,
  })
  await client.init(subject, async () => {})
  await client.appendChanges({ data: "retry" })
  const synced = await client.sync()
  await server.whenIdle()
  assert.deepEqual(synced, { status: "succeed", subject })
  assert.equal(calls, 3)
  assert.match(server.failedEntries(subject)[0].error, /apply failed after 3 attempts: server failed/)
  const db = new Database(path.join(dirs.server, "syncbridge.sqlite"), { readonly: true })
  const rows = db.prepare("SELECT package_json FROM log_entries").all()
  db.close()
  assert.equal(rows.length, 1)
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
  await client.whenIdle()
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
  await client.whenIdle()
  assert.deepEqual(first, { status: "succeed", subject })
  assert.deepEqual(appliedA, ["on-a"])
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")

  live = liveB
  const switched = await client.sync()
  assert.deepEqual(switched, { status: "switched", switched: true, subject: liveB })
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
  await client.whenIdle()
  assert.deepEqual(syncedB, { status: "succeed", subject: liveB })
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(liveB), "pointer"), "utf8"), "1")
  assert.deepEqual(appliedB, ["on-b"])

  live = subject
  const back = await client.sync()
  assert.deepEqual(back, { status: "switched", switched: true, subject })
  await client.use(subject, async (entry) => {
    appliedA.push(entry.package.data)
  })
  assert.equal(await readFile(path.join(dirs.client, "subjects", subjectDigest(subject), "pointer"), "utf8"), "1")
})

test("appendChanges returns while a client apply is still running", async (t) => {
  const dirs = await tempDirs(t)
  let release
  const gate = new Promise((resolve) => { release = resolve })
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, () => gate)
  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.init(subject, async () => {})
  await client.appendChanges({ data: "uploaded" })
  await client.sync()
  const raced = await Promise.race([
    server.appendChanges(subject, { data: "local" }).then(() => "appended"),
    new Promise((resolve) => setTimeout(() => resolve("blocked"), 200)),
  ])
  assert.equal(raced, "appended")
  release()
  await server.whenIdle()
  const failed = server.failedEntries(subject)
  assert.equal(failed.length, 0)
})

test("a bad change is refused twice and the third send moves it to the client failure list", async (t) => {
  const dirs = await tempDirs(t)
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {})
  const storage = createNodeClientStorage(dirs.client)
  const client = createClient({
    storage,
    transport: createHttpTransport({ url: endpoints.url }),
    sha256: digestBytes,
    randomId,
  })
  await client.init(subject, async () => {})
  const digest = subjectDigest(subject)
  const missing = "ab".repeat(32)
  await storage.appendOutbox(digest, {
    id: "bad",
    attempts: 0,
    package: {
      data: "bad",
      attachments: [{ id: "a", path: "a.jpg", sha256: missing, size: 1 }],
    },
  })
  await storage.appendOutbox(digest, {
    id: "good",
    attempts: 0,
    package: { data: "good", attachments: [] },
  })
  const first = await client.sync()
  assert.equal(first.status, "failed")
  assert.equal(server.failedEntries(subject).length, 0)
  const second = await client.sync()
  assert.equal(second.status, "failed")
  const third = await client.sync()
  assert.equal(third.status, "succeed")
  const failures = await client.failedEntries()
  assert.equal(failures.length, 1)
  assert.match(failures[0].error, /send failed after 3 attempts/)
  const db = new Database(path.join(dirs.server, "syncbridge.sqlite"), { readonly: true })
  const rows = db.prepare("SELECT id, package_json FROM log_entries").all()
  db.close()
  assert.deepEqual(rows.map((row) => row.id), ["good"])
})

test("a download failure is retried by the server and then skipped for that client only", async (t) => {
  const dirs = await tempDirs(t)
  const photo = Buffer.from("photo-bytes")
  const store = createHostStore([["poi-1.jpg", photo]])
  const server = createSyncServer({
    dataDir: dirs.server,
    currentSubject() { return subject },
  })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async (entry, attachments) => {
    if (entry.package.attachments.length === 0) return
    const bytes = Buffer.from(await attachments.get("photo-1").read())
    store.put("server/poi-1.jpg", bytes)
    return { attachments: [{ id: "photo-1", path: "server/poi-1.jpg" }] }
  }, { readAttachment: store.readAttachment })
  const sender = createSyncClient({ dataDir: dirs.clientA, url: endpoints.url })
  await sender.init(subject, async () => {}, { readAttachment: store.readAttachment })
  await sender.appendChanges({
    data: { poi: "north-wall" },
    attachments: [{ id: "photo-1", path: "poi-1.jpg", contentType: "image/jpeg" }],
  })
  await sender.appendChanges({ data: { poi: "later" } })
  await sender.sync()
  await server.whenIdle()

  const real = createHttpTransport({ url: endpoints.url })
  let downloads = 0
  const receiver = createClient({
    storage: createNodeClientStorage(dirs.clientB),
    transport: {
      post: (body) => real.post(body),
      put: (body) => real.put(body),
      get(hash) {
        downloads += 1
        throw new Error("download failed")
      },
    },
    sha256: digestBytes,
    randomId,
  })
  const seen = []
  await receiver.init(subject, async (entry) => { seen.push(entry.package.data.poi) })
  await receiver.sync()
  await receiver.whenIdle()
  assert.equal(downloads, 3)
  assert.deepEqual(seen, ["later"])
  const skipped = server.failedEntries(subject).filter((row) => row.kind === "send" && /download failed/.test(row.error))
  assert.equal(skipped.length, 1)

  const otherSeen = []
  const other = createSyncClient({ dataDir: path.join(dirs.root, "client-c"), url: endpoints.url })
  await other.init(subject, async (entry) => { otherSeen.push(entry.package.data.poi) }, { readAttachment: store.readAttachment })
  await other.sync()
  await other.whenIdle()
  assert.deepEqual(otherSeen, ["north-wall", "later"])
})

test("a bad JSON body is a failed ack and the next sync still succeeds", async (t) => {
  const dirs = await tempDirs(t)
  const server = createSyncServer({ dataDir: dirs.server })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {})
  const bad = await fetch(endpoints.url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "not-json",
  })
  assert.equal(bad.status, 200)
  const body = await bad.json()
  assert.equal(body.status, "failed")
  assert.match(body.error, /JSON/)
  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.init(subject, async () => {})
  await client.appendChanges({ data: "after" })
  const synced = await client.sync()
  assert.equal(synced.status, "succeed")
})

test("an unexpected storage throw is an error ack and the next sync still succeeds", async (t) => {
  const dirs = await tempDirs(t)
  const storage = createNodeServerStorage(dirs.server)
  const original = storage.appendEntry.bind(storage)
  let armed = true
  storage.appendEntry = (...args) => {
    if (armed) {
      armed = false
      throw new Error("disk failed")
    }
    return original(...args)
  }
  const server = createServer({ storage })
  t.after(() => server.close())
  const endpoints = await listen(server, t)
  server.init(subject, async () => {})
  const client = createSyncClient({ dataDir: dirs.client, url: endpoints.url })
  await client.init(subject, async () => {})
  await client.appendChanges({ data: "once" })
  const failed = await client.sync()
  assert.equal(failed.status, "error")
  assert.match(failed.error, /disk failed/)
  const synced = await client.sync()
  assert.equal(synced.status, "succeed")
  await server.whenIdle()
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
